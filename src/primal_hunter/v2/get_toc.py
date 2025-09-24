#!/usr/bin/env python3
from __future__ import annotations

"""
Fetch and parse The Primal Hunter TOC from RoyalRoad.
- Cache HTML to static/html/toc.html
- Refresh cache if older than 6 hours
- Extract chapter info (number, title, url, published)
- Save chapters > 985 to static/json/toc.json keyed by chapter number
"""

import json
import os
import re
from collections import OrderedDict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TypedDict

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from rich.console import Console

from primal_hunter.logger import get_console, get_logger, get_progress

BASE_URL: str = "https://www.royalroad.com"
TOC_URL: str = f"{BASE_URL}/fiction/36049/the-primal-hunter"
CACHE_HTML: Path = Path("static/html/toc.html")
OUTPUT_JSON: Path = Path("static/json/toc.json")

HEADERS: Dict[str, str] = {"User-Agent": "Mozilla/5.0"}
CACHE_TTL: timedelta = timedelta(hours=6)

# Ensure dirs exist
CACHE_HTML.parent.mkdir(parents=True, exist_ok=True)
OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

console: Console = get_console()
log = get_logger()


class ChapterRecord(TypedDict):
    """Structured metadata extracted for a single chapter."""

    chapter: int
    title: str
    url: str
    published: Optional[str]


def fetch_toc() -> str:
    """Return the Table of Contents HTML, refreshing the cache when stale.

    Returns:
        str: The HTML contents for the Primal Hunter table of contents page.
    """
    if CACHE_HTML.exists():
        mtime = datetime.fromtimestamp(CACHE_HTML.stat().st_mtime)
        if datetime.now() - mtime < CACHE_TTL:
            log.success("Using cached TOC HTML")
            return CACHE_HTML.read_text(encoding="utf-8")

    log.warning("Cache expired or missing. Fetching TOC from the web...")
    # Refetch if no cache or expired
    resp = requests.get(TOC_URL, headers=HEADERS)
    resp.raise_for_status()
    CACHE_HTML.write_text(resp.text, encoding="utf-8")
    log.success("Fetched fresh TOC HTML")
    return resp.text


def _iter_chapter_rows(soup: BeautifulSoup) -> Iterable[Tag]:
    """Yield each chapter row from the RoyalRoad TOC table."""

    return soup.select("table#chapters tbody tr.chapter-row")


def parse_chapters(html: str) -> List[ChapterRecord]:
    """Extract structured chapter metadata from the provided TOC HTML.

    Args:
        html: Raw HTML fetched from the RoyalRoad table of contents page.

    Returns:
        list[dict[str, Any]]: Ordered list of chapter dictionaries containing
            chapter number, title, URL, and published timestamp (ISO-8601).
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = list(_iter_chapter_rows(soup))
    chapters: List[ChapterRecord] = []

    log.trace(f"Parsing {len(rows)} chapter rows...")
    with get_progress(console) as progress_bar:
        task = progress_bar.add_task("Parsing chapters...", total=len(rows))

        for row_index, row in enumerate(rows, start=1):
            cells = row.find_all("td")
            if len(cells) != 2:
                log.warning(f"Skipping invalid row {row_index}")
                progress_bar.advance(task)
                continue

            progress_bar.update(
                task,
                description=f"Row {row_index}: Parsing chapter and title...",
            )
            # Parse chapter text (e.g., "Chapter 986 - A False God ...")
            text = cells[0].get_text(strip=True)
            log.debug(f"Row {row_index}:1 text: `{text}`")
            match = re.match(r"Chapter\s+(?P<chapter>\d+) - (?P<title>.+)", text)
            if not match:
                log.warning(f"Could not parse row {row_index}: {text}")
                progress_bar.advance(task)
                continue
            chapter_num = int(match.group("chapter"))
            title = match.group("title").strip()
            progress_bar.console.log(f"Parsed chapter {chapter_num}: {title}")

            progress_bar.update(
                task, description=f"Row {row_index}: Parsing URL..."
            )
            # Ensure the first cell is a Tag and extract the href safely.
            first_cell = cells[0]
            if not isinstance(first_cell, Tag):
                log.warning(f"Skipping invalid row {row_index}")
                progress_bar.advance(task)
                continue
            anchor = first_cell.find("a")
            href = anchor.get("href") if isinstance(anchor, Tag) else None
            if not href:
                log.warning(f"Skipping invalid row {row_index}")
                progress_bar.advance(task)
                continue
            url = f"{BASE_URL}{href}"

            # Published datetime from <time datetime="">
            progress_bar.update(
                task, description=f"Row {row_index}: Parsing published..."
            )
            second_cell = cells[1]
            time_tag = (
                second_cell.find("time") if isinstance(second_cell, Tag) else None
            )
            published_str = (
                time_tag.get("datetime")
                if (time_tag and isinstance(time_tag, Tag))
                else None
            )

            # Ensure published_str is a plain string (BeautifulSoup may return an AttributeValueList)
            if published_str:
                published_text = (
                    published_str
                    if isinstance(published_str, str)
                    else " ".join(published_str)
                )
                published = datetime.fromisoformat(
                    published_text.replace("Z", "+00:00")
                )
            else:
                published = None

            chapters.append(
                ChapterRecord(
                    chapter=chapter_num,
                    title=title,
                    url=url,
                    published=published.isoformat() if published else None,
                )
            )
            log.trace(f"Parsed chapter {chapter_num}: {title}")
            progress_bar.advance(task)

    return chapters


def _serialise_chapters(chapters: Iterable[ChapterRecord]) -> OrderedDict[str, Dict[str, Any]]:
    """Convert chapter records to a JSON-friendly mapping keyed by chapter number."""

    serialised: list[tuple[int, Dict[str, Any]]] = []
    for record in chapters:
        chapter_num = record["chapter"]
        payload = {
            key: value
            for key, value in record.items()
            if key != "chapter"
        }
        serialised.append((chapter_num, payload))

    ordered = OrderedDict(
        (str(number), payload) for number, payload in sorted(serialised, key=lambda item: item[0])
    )
    return ordered


def main() -> None:
    """Fetch, parse, filter, and persist the Primal Hunter chapter listing."""
    log.trace("Fetching TOC...")
    html = fetch_toc()
    chapters = parse_chapters(html)
    log.trace(f"Parsed {len(chapters)} chapters total")
    filtered = [c for c in chapters if c["chapter"] > 985]
    log.trace(f"Filtered {len(filtered)} chapters > 985")

    serialised = _serialise_chapters(filtered)
    OUTPUT_JSON.write_text(json.dumps(serialised, indent=2), encoding="utf-8")
    log.success(f"Saved {len(serialised)} chapters to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
