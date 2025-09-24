#!/usr/bin/env python3
"""Scrape Primal Hunter chapters and persist them via the `Version` document."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, NamedTuple, Optional, TypedDict

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
import subprocess

console: Console = get_console()
log = get_logger()
progress = get_progress(console)

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


class ChapterPayloadRequired(TypedDict):
    """Required attributes for a TOC entry."""

    chapter: int
    title: str
    url: str


class ChapterPayload(ChapterPayloadRequired, total=False):
    """Shape of a chapter entry produced by `get_toc.py`."""
    content_markdown: str
    published: Optional[datetime]


class ChapterContent(NamedTuple):
    """Return type for `extract_content` containing text and HTML variants."""
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


def _parse_published(value: Any) -> Optional[datetime]:
    """Return an aware ``datetime`` for ``value`` when possible."""

    if value in (None, ""):
        return None

    if isinstance(value, datetime):
        tz_aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return tz_aware.astimezone(timezone.utc)

    if isinstance(value, Mapping):
        iso_value = value.get("iso") or value.get("ISO")
        if iso_value:
            return _parse_published(str(iso_value))

        timestamp_value = value.get("timestamp") or value.get("epoch")
        if timestamp_value is not None:
            try:
                return datetime.fromtimestamp(float(timestamp_value), tz=timezone.utc)
            except (TypeError, ValueError):
                log.warning(
                    f"Unable to parse published timestamp epoch: {timestamp_value!r}"
                )

        utc_block = value.get("utc")
        if isinstance(utc_block, Mapping):
            try:
                base = datetime(
                    year=int(utc_block.get("year")),
                    month=int(utc_block.get("month")),
                    day=int(utc_block.get("day")),
                    hour=int(utc_block.get("hour", 0)),
                    minute=int(utc_block.get("minute", 0)),
                    second=int(utc_block.get("second", 0)),
                    microsecond=int(utc_block.get("microsecond", 0)),
                    tzinfo=timezone.utc,
                )
            except (TypeError, ValueError):
                log.warning(
                    f"Unable to construct datetime from utc block: {utc_block!r}"
                )
            else:
                return base

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            log.warning(f"Unable to parse published timestamp: {value!r}")
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    log.warning(f"Unsupported published type {type(value)!r}; ignoring")
    return None


def _normalize_toc_entry(
    *, chapter_key: str, payload: Mapping[str, Any]
) -> Optional[ChapterPayload]:
    """Return a well-typed TOC entry or ``None`` when the payload is invalid."""

    try:
        chapter_from_key = int(chapter_key)
    except (TypeError, ValueError):
        log.warning(f"Skipping TOC entry with non-numeric key: {chapter_key!r}")
        return None

    candidate = payload.get("chapter", chapter_from_key)
    try:
        chapter = int(candidate)
    except (TypeError, ValueError) as exc:
        msg = f"Skipping TOC entry with invalid chapter value: {candidate!r} (key {chapter_key})"
        log.warning(msg)
        log.debug(f"Exception details: {exc}")
        return None

    if chapter != chapter_from_key:
        log.debug(
            f"Chapter mismatch between key {chapter_key} and \
payload {candidate}; using payload value"
        )

    title = payload.get("title")
    url = payload.get("url")
    if not title or not url:
        log.warning(f"Skipping incomplete TOC entry for chapter {chapter}")
        return None

    entry: ChapterPayload = {
        "chapter": chapter,
        "title": str(title),
        "url": str(url),
    }

    if (published := _parse_published(payload.get("published"))) is not None:
        entry["published"] = published

    return entry


def _iter_toc_entries(raw_data: Any) -> Iterator[ChapterPayload]:
    """Yield normalized TOC entries while deduplicating chapter numbers."""

    seen_chapters: set[int] = set()

    if isinstance(raw_data, dict):
        ordered_keys = sorted(raw_data.keys(), key=lambda value: int(str(value)))
        iterator: Iterable[tuple[str, Any]] = (
            (str(key), raw_data[key]) for key in ordered_keys
        )
    elif isinstance(raw_data, list):
        iterator = ((str(item.get("chapter")), item) for item in raw_data)
    else:  # pragma: no cover - defensive
        raise TypeError("Unsupported TOC structure; expected mapping or sequence")

    for chapter_key, payload in iterator:
        if not isinstance(payload, dict):
            log.warning(f"Skipping malformed TOC entry for chapter {chapter_key}")
            continue

        entry = _normalize_toc_entry(chapter_key=chapter_key, payload=payload)
        if not entry:
            continue

        chapter_num = entry["chapter"]
        if chapter_num in seen_chapters:
            log.warning(
                f"Duplicate TOC entry for chapter {chapter_num} detected; skipping"
            )
            continue

        seen_chapters.add(chapter_num)
        yield entry


def load_toc() -> tuple[Iterator[ChapterPayload], int]:
    """Return a streaming iterator for TOC entries and the expected total count."""

    if not TOC_PATH.exists():
        msg = f"TOC file not found: {TOC_PATH}"
        raise FileNotFoundError(msg)

    with TOC_PATH.open("r", encoding="utf-8") as handle:
        raw_data = json.load(handle)

    if isinstance(raw_data, dict):
        expected = len(raw_data)
    elif isinstance(raw_data, list):
        expected = len(raw_data)
    else:
        raise TypeError("Unsupported TOC structure; expected mapping or sequence")

    def iterator() -> Iterator[ChapterPayload]:
        produced = 0
        try:
            for entry in _iter_toc_entries(raw_data):
                produced += 1
                yield entry
        finally:
            log.success(
                f"Loaded TOC with {produced} chapters from {TOC_PATH}"
            )

    return iterator(), expected


async def fetch_chapter(url: str) -> Optional[str]:
    """Fetch the HTML for `url` in a worker thread to avoid blocking the loop."""

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
    """Append `text-align: justify` to `style` when absent."""

    normalized = (style or "").strip()
    if "text-align" in normalized:
        return normalized
    if normalized and not normalized.endswith(";"):
        normalized = f"{normalized};"
    return f"{normalized} text-align: justify;".strip()


def _formatted_html(container: Tag) -> str:
    """Return sanitized HTML for `container` with paragraph justification."""

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
        if isinstance(paragraph, Tag):
            style_attr = paragraph.get("style")
            # Coerce bs4 AttributeValueList (or other non-str types) to str while preserving None
            style_str = None if style_attr is None else str(style_attr)
            paragraph["style"] = _ensure_justified_style(style_str)

    return str(root)


def _display_chapter_preview(
    *,
    chapter: int,
    title: str,
    url: str,
    published: Optional[datetime],
    content: str,
    max_preview_chars: int = 2000,
) -> None:
    """Render a chapter snippet to the Rich console."""

    preview_lines: list[str] = []
    consumed = 0
    truncated = False
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        remaining = max_preview_chars - consumed
        if remaining <= 0:
            truncated = True
            break

        if len(line) > remaining:
            preview_lines.append(line[:remaining].rstrip() + "…")
            consumed = max_preview_chars
            truncated = True
            break

        preview_lines.append(line)
        consumed += len(line) + 2  # account for the artificial paragraph spacing

    text = "\n\n".join(preview_lines)
    if truncated and text:
        text += "\n\n_(preview truncated)_"

    idx = url.find("/chapter/")
    subtitle_url = url[idx:] if idx != -1 else url
    if isinstance(published, datetime):
        published_text = published.astimezone(timezone.utc).isoformat()
    elif published:
        published_text = str(published)
    else:
        published_text = None

    heading = f"Chapter {chapter}: {title}" if title else f"Chapter {chapter}"

    panel_subtitle = (
        f"{subtitle_url} | Published: {published_text}"
        if published_text
        else subtitle_url
    )

    try:
        console.print(
            Panel(
                Markdown(text or "_(no content extracted)_"),
                title=Text(heading, colors=["#95FF00", "#37FF00"], style="bold"),
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
        chapter=chapter["chapter"],
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

    # Convert the formatted HTML into pandoc-flavoured Markdown while stripping
    # class attributes but preserving inline styles (so decoration and
    # justification survive). We try pypandoc first, then fall back to calling
    # the pandoc binary if available.
    try:
        import pypandoc  # optional, may not be installed
    except Exception:
        pypandoc = None

    # Use the globally imported BeautifulSoup as the parser; avoid reassigning
    # a name that could be interpreted as a type in different control flow paths.
    _BS = BeautifulSoup

    # Remove class attributes but keep style attributes
    soup_for_pandoc = _BS(chapter_content.html or "", "html.parser")
    for tag in soup_for_pandoc.find_all(True):
        # find_all can return non-Tag PageElement / NavigableString values; guard with isinstance
        if isinstance(tag, Tag) and "class" in tag.attrs:
            del tag.attrs["class"]
    cleaned_html = str(soup_for_pandoc)

    markdown: str = ""
    if pypandoc is not None:
        try:
            # ask pypandoc to convert; keep wrapping disabled for readability
            markdown = pypandoc.convert_text(
                cleaned_html, to="markdown", format="html", extra_args=["--wrap=none"]
            )
            log.debug(f"pypandoc conversion succeeded for chapter {chapter_num}")
        except Exception as exc:
            log.warning(f"pypandoc conversion failed for chapter {chapter_num}: {exc}")
            markdown = ""

    if not markdown:
        # Fallback to pandoc CLI. Use the markdown+raw_html target so any remaining
        # style-bearing HTML is preserved verbatim in the Markdown output.
        log.debug(f"Falling back to pandoc CLI for chapter {chapter_num} markdown conversion")
        try:
            proc = subprocess.run(
                ["pandoc", "--from=html", "--to=markdown+raw_html", "--wrap=none"],
                input=cleaned_html,
                text=True,
                capture_output=True,
                check=True,
            )
            markdown = proc.stdout
            log.debug(f"Pandoc CLI conversion succeeded for chapter {chapter_num}")
            console.print()
        except FileNotFoundError:
            log.warning("Pandoc executable not found; skipping markdown conversion")
        except subprocess.CalledProcessError as exc:
            err = exc.stderr.strip() if exc.stderr else str(exc)
            log.warning(f"Pandoc conversion failed for chapter {chapter_num}: {err}")

    if markdown:
        # Keep the computed markdown available for downstream use / inspection.
        # We attach it to the chapter payload (non-invasive) and log the result.
        chapter_data["content_markdown"] = markdown
        log.debug(f"Converted chapter {chapter_num} HTML to pandoc markdown ({len(markdown)} chars)")



    document = Version.from_payload(
        chapter_data,
        content=chapter_content.text,
        content_html=chapter_content.html,
        content_markdown=markdown,
    )

    existing = await Version.find_one(Version.chapter == chapter_num)
    if existing:
        log.info(f"Chapter {chapter_num} already exists; updating")
        existing.title = document.title
        existing.url = document.url
        existing.content = document.content
        existing.content_html = document.content_html
        existing.content_markdown = document.content_markdown
        existing.published = document.published
        await existing.save()
        log.success(f"Updated chapter {chapter_num}: {existing.title}")
        return

    await document.insert()
    log.success(f"Inserted chapter {chapter_num}: {document.title}")


async def scrape_chapters() -> None:
    """Iterate through the TOC and persist each chapter."""

    toc_entries, expected_total = load_toc()
    with progress as progress_bar:
        task_id = progress_bar.add_task(
            "Scraping chapters...", total=expected_total or None
        )
        for chapter in toc_entries:
            await process_chapter(chapter)
            progress_bar.advance(task_id)


async def main() -> None:
    await init_db()
    await scrape_chapters()


if __name__ == "__main__":
    asyncio.run(main())
