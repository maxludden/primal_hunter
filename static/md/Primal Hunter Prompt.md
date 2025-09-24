# Primal Hunter Prompt

## v0.1.0


```python
"""Scrape chapters from RoyalRoad and generate a styled EPUB volume."""

from __future__ import annotations

import os
import re
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Sequence, cast

import pendulum
import requests
from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString
from ebooklib import epub
from rich.console import Console
from rich.progress import Progress

from primal_hunter.logger import get_console, get_logger, get_progress

if TYPE_CHECKING:
    from loguru import Logger
    from pymongo.collection import Collection

from markdownify import markdownify as html_to_markdown
from pymongo import MongoClient

BASE_URL = "https://www.royalroad.com"
FICTION_URL = f"{BASE_URL}/fiction/36049/the-primal-hunter"
START_CHAPTER = 986
OUTPUT_FILE = "The_Primal_Hunter_from986.epub"


HEADERS = {"User-Agent": "Mozilla/5.0"}
DEFAULT_MAX_WORKERS = 4
BOOK_TITLE = "Primal Hunter: Book 14"
BOOK_SUBTITLE = "Chapters 986+"
PARAGRAPH_STYLE = "text-indent:2em;margin:0;"
FIRST_PARAGRAPH_STYLE = "text-indent:0;margin:0;"
DROP_CAP_STYLE = (
    "float:left;font-size:3em;line-height:0.85;margin-right:0.25em;font-weight:bold;"
)

ChapterLink = tuple[int, str, str]
"""Tuple of chapter number, absolute URL, and title string."""


@dataclass(slots=True)
class ChapterData:
    """Normalized payload for a downloaded chapter."""

    number: int
    title: str
    url: str
    html: str
    posted: Optional[datetime]
    downloaded: datetime
    markdown: str
    text: str


STATIC_DIR = Path(__file__).resolve().parents[2] / "static"
STATIC_HTML_DIR = STATIC_DIR / "html"
STATIC_CSS_FILE = STATIC_DIR / "css" / "styles.css"
COVER_IMAGE_FILE = STATIC_DIR / "img" / "book14.png"
MONGO_URI_ENV = "PRIMAL_HUNTER_MONGODB_URI"
MONGO_DB_NAME = "primal-hunter"
MONGO_COLLECTION_NAME = "chapter"


def _clean_chapter_title(raw_title: str) -> str:
    """Return a normalized chapter title without book marker suffixes."""

    cleaned = raw_title.strip().strip('"“”')
    suffixes = [
        " - START OF BOOK 14",
        " – START OF BOOK 14",
        "— START OF BOOK 14",
    ]
    for suffix in suffixes:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]
    return cleaned.strip()


def _apply_dropcap(paragraph: Tag) -> None:
    """Wrap the first alphabetical character in the paragraph in a styled span."""

    for descendant in paragraph.descendants:
        if isinstance(descendant, NavigableString):
            text = str(descendant)
            for idx, char in enumerate(text):
                if char.isalpha():
                    before = text[:idx]
                    after = text[idx + 1 :]

                    # Create a new <span> for the drop cap using a known callable.
                    # Prefer the Tag.new_tag method when available and callable; otherwise
                    # fall back to BeautifulSoup().new_tag to ensure we create a Tag.
                    if hasattr(paragraph, "new_tag") and callable(getattr(paragraph, "new_tag")):
                        # paragraph.new_tag is a bound method on Tag instances
                        drop_tag = cast(Tag, paragraph.new_tag("span")) # type: ignore
                    else:
                        # Use a fresh BeautifulSoup instance to create an unbound tag
                        drop_tag = cast(Tag, BeautifulSoup("", "html.parser").new_tag("span"))

                    drop_tag["style"] = DROP_CAP_STYLE
                    drop_tag.string = char

                    # Build surrounding text nodes if needed.
                    before_node = NavigableString(before) if before else None
                    after_node = NavigableString(after) if after else None

                    # Replace the original text node with the new sequence:
                    # insert 'before' (if any), then the drop tag, then 'after' (if any).
                    if before_node is not None:
                        descendant.insert_before(before_node)
                    descendant.replace_with(drop_tag)
                    if after_node is not None:
                        drop_tag.insert_after(after_node)

                    return


def _parse_datetime_string(raw_value: str) -> Optional[datetime]:
    """Parse a datetime string into a timezone-aware ``datetime`` if possible.

    Uses pendulum for robust parsing of ISO and human-readable date strings.
    Returns a timezone-aware datetime in UTC or ``None`` if parsing fails.
    """

    candidate = (raw_value or "").strip()
    if not candidate:
        return None

    try:
        # pendulum.parse is permissive when strict=False and handles many formats
        parsed = pendulum.parse(candidate, strict=False)
    except Exception:
        return None

    # Ensure the returned value is timezone-aware and normalized to UTC
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    # Return a datetime (Pendulum is a datetime subclass) in UTC
    try:
        return parsed.astimezone(timezone.utc)
    except Exception:
        # Fallback: convert using Pendulum's in_timezone
        return parsed.in_timezone("UTC")


def _parse_posted_datetime(soup: BeautifulSoup) -> Optional[datetime]:
    """Extract a posted timestamp from common RoyalRoad chapter markup."""

    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if meta and isinstance(meta, Tag):
        content = meta.get("content")
        if content:
            parsed = _parse_datetime_string(str(content))
            if parsed:
                return parsed

    time_tag = soup.find("time")
    if time_tag and isinstance(time_tag, Tag):
        datetime_attr = time_tag.get("datetime")
        if datetime_attr:
            parsed = _parse_datetime_string(str(datetime_attr))
            if parsed:
                return parsed
        if time_tag.string:
            parsed = _parse_datetime_string(str(time_tag.string))
            if parsed:
                return parsed

    return None


_MONGO_CLIENT: Optional[MongoClient] = None
_MONGO_COLLECTION: Optional["Collection[dict[str, Any]]"] = None


def _ensure_static_dir() -> None:
    """Guarantee the on-disk HTML directory exists."""

    STATIC_HTML_DIR.mkdir(parents=True, exist_ok=True)


def _write_chapter_html(chapter: ChapterData, log: "Logger") -> Path:
    """Persist chapter HTML to ``static/html`` using a zero-padded filename."""

    _ensure_static_dir()
    file_path = STATIC_HTML_DIR / f"{chapter.number:04d}.html"
    file_path.write_text(chapter.html, encoding="utf-8")
    log.debug("Saved chapter {number} to {path}", number=chapter.number, path=file_path)
    return file_path


def _ensure_mongo_collection(log: "Logger") -> Optional["Collection[dict[str, Any]]"]:
    """Return the MongoDB collection, establishing a connection on first use."""

    global _MONGO_CLIENT, _MONGO_COLLECTION
    if _MONGO_COLLECTION is not None:
        return _MONGO_COLLECTION

    uri = os.getenv(MONGO_URI_ENV, "mongodb://localhost:27017")
    try:
        client = MongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as exc:  # pragma: no cover - runtime dependency
        log.error("MongoDB connection failed for {uri}: {error}", uri=uri, error=exc)
        return None

    try:
        collection = client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
        collection.create_index("chapter", unique=True)
    except Exception as exc:  # pragma: no cover - runtime dependency
        log.error("Unable to prepare MongoDB collection: {error}", error=exc)
        return None

    _MONGO_CLIENT = client
    _MONGO_COLLECTION = collection
    return _MONGO_COLLECTION


def _store_chapter_document(chapter: ChapterData, log: "Logger") -> None:
    """Upsert the chapter document into MongoDB if available."""

    collection = _ensure_mongo_collection(log)
    if collection is None:
        return

    document = {
        "chapter": chapter.number,
        "title": chapter.title,
        "posted": chapter.posted or chapter.downloaded,
        "downloaded": chapter.downloaded,
        "url": chapter.url,
        "html": chapter.html,
        "markdown": chapter.markdown,
        "text": chapter.text,
    }

    try:
        collection.update_one(
            {"chapter": chapter.number}, {"$set": document}, upsert=True
        )
    except Exception as exc:  # pragma: no cover - runtime dependency
        log.error(
            "Failed to upsert chapter {number} into MongoDB: {error}",
            number=chapter.number,
            error=exc,
        )


def _persist_chapter(
    chapter: ChapterData,
    log: "Logger",
    progress: Optional[Progress] = None,
    task_id: Optional[Any] = None,
) -> None:
    """Persist chapter assets to disk and MongoDB, updating progress if provided."""

    _write_chapter_html(chapter, log)
    _store_chapter_document(chapter, log)

    if progress is not None and task_id is not None:
        progress.update(
            cast(Any, task_id),
            advance=1,
            description=(
                f"Persisting chapters • last saved {chapter.number:04d}"
                if chapter.number
                else "Persisting chapters"
            ),
        )


def get_chapter_links(
    console: Console,
    log: "Logger",
    progress: Optional[Progress] = None,
    task_id: Optional[Any] = None,
) -> list[ChapterLink]:
    """Fetch the fiction page and return filtered chapter links.

    Args:
        console: Console used for transient progress display.
        log: Loguru logger for status messages.
        progress: Optional shared progress instance for reporting.
        task_id: Optional task identifier to reuse on the provided progress bar.

    Returns:
        A list of ordered tuples ``(chapter_number, absolute_url, title)``.
    """

    log.info("Fetching table of contents …")
    resp: requests.Response = requests.get(FICTION_URL, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    chapter_pattern = re.compile(r"Chapter\s+(\d+)", re.IGNORECASE)

    anchors: list[Tag] = [
        a for a in soup.find_all("a", href=True) if isinstance(a, Tag)
    ]
    links: list[ChapterLink] = []

    shared_progress = progress
    own_progress = False
    if shared_progress is None:
        shared_progress = get_progress(console=console)
        own_progress = True

    manager = shared_progress if own_progress else nullcontext()
    with manager:
        if task_id is None:
            toc_task = shared_progress.add_task(
                "Collecting chapter links", total=len(anchors) or 1
            )
            toc_task = cast(Any, toc_task)
        else:
            toc_task = task_id
            toc_task = cast(Any, toc_task)
            shared_progress.reset(
                toc_task,
                total=len(anchors) or 1,
                description="Collecting chapter links",
            )

        for anchor in anchors:
            href = anchor.get("href", "") if isinstance(anchor, Tag) else ""
            text = anchor.get_text(strip=True) if isinstance(anchor, Tag) else ""
            if href and "/chapter/" in href:
                match = chapter_pattern.search(text)
                if match:
                    number = int(match.group(1))
                    if number >= START_CHAPTER:
                        links.append((
                            number,
                            f"{BASE_URL}{href}",
                            _clean_chapter_title(text),
                        ))
                        shared_progress.update(
                            toc_task,
                            description=(
                                f"Collecting chapter links • latest {number:04d}"
                            ),
                        )
            shared_progress.advance(toc_task)

        shared_progress.update(
            toc_task,
            completed=shared_progress.tasks[toc_task].total,
            description=f"Collected {len(links)} chapter links",
        )

    links.sort(key=lambda item: item[0])
    log.success("Collected {count} chapter links.", count=len(links))
    return links


def scrape_chapter(number: int, url: str, title_text: str) -> ChapterData:
    """Return normalized chapter data for a single chapter page.

    Args:
        number: Chapter number used for ordering and filenames.
        url: Fully-qualified chapter URL.
        title_text: Title text to display above the chapter content.

    Returns:
        ``ChapterData`` containing HTML, Markdown, and plain text variants.
    """

    resp: requests.Response = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    posted = _parse_posted_datetime(soup)

    # Grab chapter content div
    candidate = soup.find("div", class_="chapter-inner chapter-content")
    div: Optional[Tag]
    if candidate and isinstance(candidate, Tag):
        div = candidate
    else:
        div = None

    if not div:
        downloaded = datetime.now(timezone.utc)
        return ChapterData(
            number=number,
            title=title_text,
            url=url,
            html="",
            posted=posted,
            downloaded=downloaded,
            markdown="",
            text="",
        )

    # Remove unwanted tags (ads, images, etc.)
    for tag in list(div.find_all(["img", "script", "style"])):
        if isinstance(tag, Tag):
            tag.decompose()

    # Collect paragraphs with styling
    paragraphs: list[Tag] = []
    for paragraph in div.find_all("p"):
        if not isinstance(paragraph, Tag):
            continue
        if not paragraph.get_text(strip=True):
            continue
        paragraph["style"] = PARAGRAPH_STYLE
        paragraphs.append(paragraph)

    if paragraphs:
        paragraphs[0]["style"] = FIRST_PARAGRAPH_STYLE
        _apply_dropcap(paragraphs[0])

    content_html = "\n".join(str(paragraph) for paragraph in paragraphs)

    # Add bold chapter title at top
    chapter_section = f"<section class=\"chapter\">\n{content_html}\n</section>"
    html = f"<h2><strong>{title_text}</strong></h2>\n{chapter_section}"

    markdown = html_to_markdown(html).strip()
    text = BeautifulSoup(html, "html.parser").get_text("\n").strip()
    downloaded = datetime.now(timezone.utc)

    return ChapterData(
        number=number,
        title=title_text,
        url=url,
        html=html,
        posted=posted,
        downloaded=downloaded,
        markdown=markdown,
        text=text,
    )


def scrape_chapters_concurrently(
    chapter_links: Sequence[ChapterLink],
    console: Console,
    log: "Logger",
    max_workers: Optional[int] = None,
    polite_delay: float = 0.2,
    progress: Optional[Progress] = None,
    download_task_id: Optional[Any] = None,
    persist_task_id: Optional[Any] = None,
) -> list[ChapterData]:
    """Download chapters concurrently while preserving the original order.

    Args:
        chapter_links: Sequence of chapter metadata to download.
        console: Console used to render progress updates.
        log: Loguru logger for download status.
        max_workers: Optional override for the thread pool size.
        polite_delay: Delay (seconds) between completed downloads for rate limiting.
        progress: Optional shared progress instance for reporting.
        download_task_id: Task identifier to reuse for download progress updates.
        persist_task_id: Task identifier to reuse for persistence updates.

    Returns:
        A list of ``ChapterData`` instances sorted by chapter number.
    """

    resolved_workers: int
    if max_workers is None:
        env_value: Optional[str] = os.getenv("PRIMAL_HUNTER_WORKERS")
        if env_value is not None:
            try:
                max_workers = int(env_value)
            except ValueError:
                log.warning(
                    "Invalid PRIMAL_HUNTER_WORKERS value {value!r}; falling back to {default}.",
                    value=env_value,
                    default=DEFAULT_MAX_WORKERS,
                )
                max_workers = DEFAULT_MAX_WORKERS
    if max_workers is None:
        resolved_workers = DEFAULT_MAX_WORKERS
    else:
        resolved_workers = max_workers

    resolved_workers = max(1, resolved_workers)
    log.info(
        "Scraping {count} chapters with up to {workers} workers …",
        count=len(chapter_links),
        workers=resolved_workers,
    )

    results: list[ChapterData] = []
    with ThreadPoolExecutor(max_workers=resolved_workers) as executor:
        futures: dict[Future[ChapterData], ChapterLink] = {
            executor.submit(scrape_chapter, number, url, title): (number, url, title)
            for number, url, title in chapter_links
        }

        shared_progress = progress
        own_progress = False
        if shared_progress is None:
            shared_progress = get_progress(console=console)
            own_progress = True

        manager = shared_progress if own_progress else nullcontext()
        with manager:
            total = len(futures) or 1
            if download_task_id is None:
                download_task = shared_progress.add_task(
                    "Downloading chapters", total=total
                )
                download_task = cast(Any, download_task)
            else:
                download_task = download_task_id
                download_task = cast(Any, download_task)
                shared_progress.reset(
                    download_task,
                    total=total,
                    description="Downloading chapters",
                )

            if persist_task_id is None:
                persist_task = shared_progress.add_task(
                    "Persisting chapters", total=total
                )
                persist_task = cast(Any, persist_task)
            else:
                persist_task = persist_task_id
                persist_task = cast(Any, persist_task)
                shared_progress.reset(
                    persist_task,
                    total=total,
                    description="Persisting chapters",
                )

            for future in as_completed(futures):
                number, url, title = futures[future]
                try:
                    chapter_data = future.result()
                except Exception as exc:  # pragma: no cover - defensive path
                    log.error(
                        "Failed to scrape {title}: {error}", title=title, error=exc
                    )
                    chapter_data = ChapterData(
                        number=number,
                        title=title,
                        url=url,
                        html="",
                        posted=None,
                        downloaded=datetime.now(timezone.utc),
                        markdown="",
                        text="",
                    )
                else:
                    log.debug("Finished {title}", title=title)

                shared_progress.update(
                    download_task,
                    advance=1,
                    description=f"Downloading chapters • last {number:04d}",
                )
                _persist_chapter(chapter_data, log, shared_progress, persist_task)
                results.append(chapter_data)
                if polite_delay:
                    time.sleep(polite_delay)

            shared_progress.update(
                download_task,
                completed=total,
                description="Downloading chapters • complete",
            )
            shared_progress.update(
                persist_task,
                completed=total,
                description="Persisting chapters • complete",
            )

    results.sort(key=lambda item: item.number)
    return results


def build_epub(chapters: Sequence[ChapterData]) -> None:
    """Assemble an EPUB volume from chapter HTML snippets.

    Args:
        chapters: Sequence of ``ChapterData`` objects containing chapter metadata and HTML.
    """

    book = epub.EpubBook()
    book.set_title(BOOK_TITLE)
    book.set_language("en")
    book.add_author("Zogarth")
    book.add_metadata("DC", "title", BOOK_SUBTITLE, {"id": "subtitle"})
    book.add_metadata(
        "OPF",
        "meta",
        "",
        {"refines": "#subtitle", "property": "title-type", "content": "subtitle"},
    )
    book.add_metadata("DC", "description", BOOK_SUBTITLE)

    style_item: Optional[epub.EpubItem] = None
    if STATIC_CSS_FILE.exists():
        css_content = STATIC_CSS_FILE.read_text(encoding="utf-8")
        style_item = epub.EpubItem(
            uid="style_default",
            file_name="styles/styles.css",
            media_type="text/css",
            content=css_content,
        )
        book.add_item(style_item)
    else:  # pragma: no cover - guard for misconfigured deployments
        print(f"⚠️ Missing stylesheet: {STATIC_CSS_FILE}")

    if COVER_IMAGE_FILE.exists():
        cover_bytes = COVER_IMAGE_FILE.read_bytes()
        book.set_cover("cover.png", cover_bytes)
    else:  # pragma: no cover - guard for misconfigured deployments
        print(f"⚠️ Missing cover image: {COVER_IMAGE_FILE}")

    title_page = epub.EpubHtml(
        title=BOOK_TITLE,
        file_name="title_page.xhtml",
        lang="en",
    )
    title_page.set_content(
        f"<h1 style='text-align:center;'>{BOOK_TITLE}</h1>\n"
        f"<h2 style='text-align:center;font-weight:normal;'>{BOOK_SUBTITLE}</h2>"
    )
    if style_item is not None:
        title_page.add_item(style_item)
    book.add_item(title_page)

    chapter_items: list[epub.EpubHtml] = [title_page]
    for chapter in chapters:
        file_name = f"chapter_{chapter.number}.xhtml"
        chapter_file = epub.EpubHtml(
            title=chapter.title, file_name=file_name, lang="en"
        )
        chapter_file.set_content(chapter.html)
        if style_item is not None:
            chapter_file.add_item(style_item)
        book.add_item(chapter_file)
        chapter_items.append(chapter_file)

    book.toc = list(chapter_items)
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())
    book.spine = ["nav"] + chapter_items

    epub.write_epub(OUTPUT_FILE, book)
    print(f"✅ EPUB created: {OUTPUT_FILE}")


def main() -> None:
    """Entry point for scraping chapters and writing the EPUB file."""

    console = get_console()
    log = get_logger(console=console)

    pipeline_progress = get_progress(console=console)
    with pipeline_progress:
        toc_task = pipeline_progress.add_task("Collecting chapter links", total=1)
        chapter_links: list[ChapterLink] = get_chapter_links(
            console, log, progress=pipeline_progress, task_id=toc_task
        )

        if not chapter_links:
            pipeline_progress.update(
                toc_task,
                completed=1,
                description="No chapters available",
            )
            console.print("No chapters found to scrape.")
            return

        download_task = pipeline_progress.add_task(
            "Downloading chapters", total=len(chapter_links)
        )
        persist_task = pipeline_progress.add_task(
            "Persisting chapters", total=len(chapter_links)
        )

        chapters: list[ChapterData] = scrape_chapters_concurrently(
            chapter_links,
            console,
            log,
            progress=pipeline_progress,
            download_task_id=download_task,
            persist_task_id=persist_task,
        )

        build_task = pipeline_progress.add_task("Building EPUB", total=1)
        build_epub(chapters)
        pipeline_progress.update(
            build_task, advance=1, description="Building EPUB • complete"
        )


if __name__ == "__main__":
    main()

```

## v0.2.0

I would like for you to rewrite this using playright. Script should be saved as, `primal_hunter/src/v2.py`. Every chapter should have:

```
class Chapter:
  chapter: int
  title: str
  content
  url: str
  published: pendulum.datetime
  downloaded: dendulum.datetime
  format: Literal["html","markdown", "txt"]
  _major: int = 0
  _minor: int = 0
  _patch: int = 1
```

tasks:

1. Scrape every chapter from:

- BASE_URL: `https://www.royalroad.com`
- CHAPTER_URL: f"{BASE_URL}/fiction/36049/the-primal-hunter"
- Only scrape chapters ≥ 986

