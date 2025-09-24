#!/usr/bin/env python3
"""Scrape Primal Hunter chapters and persist them via the ``Version`` document."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, NamedTuple, Optional, TypedDict

import requests
from beanie import init_beanie
from bs4 import BeautifulSoup
from bs4.element import Tag
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient as MotorClient
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich_gradient import Text

from primal_hunter.logger import get_console, get_logger, get_progress
from primal_hunter.v2.models.version import Version

console: Console = get_console()
log = get_logger()
progress = get_progress()

TOC_PATH: Path = Path("static/json/toc.json")
HTTP_HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (compatible; scraping-script/1.0)"
}
REQUEST_TIMEOUT: float = 30.0
CONTENT_SELECTORS: tuple[str, ...] = (
    "div.chapter-inner",
    "div.chapter-content",
    "div#chapter-content",
    "div#chapterContent",
    "article#chapter-content",
)


class ChapterPayload(TypedDict, total=False):
    """Shape of a chapter entry produced by ``get_toc.py``."""

    chapter: int
    title: str
    url: str
    published: Optional[str]


class ChapterContent(NamedTuple):
    """Return type for ``extract_content`` containing text and HTML variants."""

    text: str
    html: str


async def init_db() -> None:
    """Initialise the MongoDB connection and Beanie document models."""

    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DBNAME", "primal_hunter")

    log.debug(f"Using MongoDB URI: {mongo_uri}")
    log.debug(f"Using MongoDB database: {db_name}")

    client: MotorClient = MotorClient(mongo_uri)
    database = client[db_name]
    await init_beanie(database=database, document_models=[Version])  # type: ignore
    log.success("Initialized Beanie with Version document model")


def load_toc() -> list[ChapterPayload]:
    """Return the table of contents entries sorted by chapter number.

    The TOC is persisted as a mapping keyed by the chapter number string.  This
    helper normalises the structure into a list of ``ChapterPayload`` items for
    downstream consumers that expect sequence semantics.
    """

    if not TOC_PATH.exists():
        msg = f"TOC file not found: {TOC_PATH}"
        raise FileNotFoundError(msg)

    with TOC_PATH.open("r", encoding="utf-8") as handle:
        raw_data = json.load(handle)

    toc_entries: list[ChapterPayload] = []
    if isinstance(raw_data, dict):
        for chapter_key, payload in raw_data.items():
            if not isinstance(payload, dict):
                log.warning(
                    f"Skipping malformed TOC entry for chapter {chapter_key}"
                )
                continue

            title = payload.get("title")
            url = payload.get("url")
            if not title or not url:
                log.warning(
                    f"Skipping incomplete TOC entry for chapter {chapter_key}"
                )
                continue

            entry: ChapterPayload = {
                "chapter": int(chapter_key),
                "title": str(title),
                "url": str(url),
            }
            if published := payload.get("published"):
                entry["published"] = str(published)
            toc_entries.append(entry)
    elif isinstance(raw_data, list):
        # Backwards compatibility for legacy list-based TOCs.
        for item in raw_data:
            if not isinstance(item, dict):
                log.warning("Skipping malformed legacy TOC entry")
                continue
            try:
                chapter = int(item["chapter"])
            except Exception as exc:  # pragma: no cover - defensive
                log.warning(f"Skipping legacy TOC entry with invalid chapter: {exc}")
                continue

            entry: ChapterPayload = {
                "chapter": chapter,
                "title": str(item.get("title", "")),
                "url": str(item.get("url", "")),
            }
            if published := item.get("published"):
                entry["published"] = str(published)
            toc_entries.append(entry)
    else:  # pragma: no cover - defensive
        raise TypeError(
            "Unsupported TOC structure; expected mapping or sequence"
        )

    toc_entries.sort(key=lambda entry: entry.get("chapter", 0))
    log.success(f"Loaded TOC with {len(toc_entries)} chapters from {TOC_PATH}")
    return toc_entries


async def fetch_chapter(url: str) -> Optional[str]:
    """Fetch the HTML for ``url`` in a worker thread to avoid blocking the loop."""

    def _request() -> Optional[str]:
        try:
            response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:  # pragma: no cover - network guard
            log.warning(f"Failed to fetch {url}: {exc}")
            return None

    return await asyncio.to_thread(_request)


def _ensure_justified_style(style: Optional[str]) -> str:
    """Append ``text-align: justify`` to ``style`` when absent."""

    normalized = (style or "").strip()
    if "text-align" in normalized:
        return normalized
    if normalized and not normalized.endswith(";"):
        normalized = f"{normalized};"
    return f"{normalized} text-align: justify;".strip()


def _formatted_html(container: Tag) -> str:
    """Return sanitised HTML for ``container`` with paragraph justification."""

    snapshot = BeautifulSoup(str(container), "html.parser")
    root = snapshot.find()
    if not isinstance(root, Tag):
        return (
            container.decode_contents()
            if hasattr(container, "decode_contents")
            else str(container)
        )

    for tag in root.find_all(["script", "style"]):
        tag.decompose()

    for paragraph in root.find_all("p"):
        paragraph["style"] = _ensure_justified_style(paragraph.get("style"))

    return str(root)


def _display_chapter_preview(
    *, title: str, url: str, published: Optional[str], content: str
) -> None:
    """Render a chapter snippet to the Rich console."""

    text = "\n\n".join(line.strip() for line in content.splitlines() if line.strip())
    panel_subtitle = f"{url} | Published: {published}" if published else url

    try:
        console.print(
            Panel(
                Markdown(text or "_(no content extracted)_"),
                title=Text(title, colors=["#95FF00", "#37FF00"], style="bold"),
                subtitle=panel_subtitle,
                border_style="#006400",
            )
        )
    except Exception:  # pragma: no cover - presentation fallback
        log.info(f"Chapter preview for {title} available at {url}")


def extract_content(html: str, *, chapter: ChapterPayload) -> ChapterContent:
    """Extract plain text and formatted HTML content from the chapter HTML."""

    chapter_num = chapter["chapter"]
    title = chapter["title"]

    soup = BeautifulSoup(html, "html.parser")
    container: Optional[Tag] = None
    for selector in CONTENT_SELECTORS:
        candidate = soup.select_one(selector)
        if isinstance(candidate, Tag):
            container = candidate
            break

    if container is None:
        log.warning(f"Chapter {chapter_num}: unable to locate content container")
        return ChapterContent(text="", html="")

    content_text: str = container.get_text(separator="\n\n", strip=True)
    formatted_html = _formatted_html(container)
    _display_chapter_preview(
        title=title,
        url=chapter["url"],
        published=chapter.get("published"),
        content=content_text,
    )
    return ChapterContent(text=content_text, html=formatted_html)


async def process_chapter(chapter_data: ChapterPayload) -> None:
    """Fetch, parse, and store a single chapter entry."""

    chapter_num = chapter_data["chapter"]
    log.debug(f"Processing chapter {chapter_num}: {chapter_data['title']}")

    html = await fetch_chapter(chapter_data["url"])
    if html is None:
        return

    chapter_content = extract_content(html, chapter=chapter_data)
    if not chapter_content.text and not chapter_content.html:
        log.warning(f"Chapter {chapter_num}: extracted content is empty")

    document = Version.from_payload(
        chapter_data,
        content=chapter_content.text,
        content_html=chapter_content.html,
    )

    existing = await Version.find_one(Version.chapter == chapter_num)
    if existing:
        log.info(f"Chapter {chapter_num} already exists; updating")
        existing.title = document.title
        existing.url = document.url
        existing.content = document.content
        existing.content_html = document.content_html
        existing.published = document.published
        await existing.save()
        log.success(f"Updated chapter {chapter_num}: {existing.title}")
        return

    await document.insert()
    log.success(f"Inserted chapter {chapter_num}: {document.title}")


async def scrape_chapters() -> None:
    """Iterate through the TOC and persist each chapter."""

    toc_entries = load_toc()
    with progress as progress_bar:
        task_id = progress_bar.add_task("Scraping chapters...", total=len(toc_entries))
        for chapter in toc_entries:
            await process_chapter(chapter)
            progress_bar.advance(task_id)


async def main() -> None:
    await init_db()
    await scrape_chapters()


if __name__ == "__main__":
    asyncio.run(main())
