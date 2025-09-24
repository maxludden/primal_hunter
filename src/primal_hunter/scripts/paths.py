"""
primal_hunter.scripts.paths

Module purpose
---------------
This module locates and processes EPUB-style book directories under a local
"static/epub" folder. It discovers HTML files within each book directory,
parses them with BeautifulSoup, extracts textual content from a set of block
and inline elements, applies normalization patterns and formatting detection,
and prepares structured data that can be serialized to JSON (paths for HTML
and CSS JSON outputs are defined).

High-level behavior
-------------------
- Ensures the EPUB_DIR ("static/epub") exists on import (created if missing).
- Walks each subdirectory of EPUB_DIR and collects .html files (case-insensitive).
- For directories with more than one HTML file:
    - Parses the first HTML file to build stylesheet/class-based style mappings
      via an external parse_stylesheets(...) helper.
    - Iterates all HTML files, parsing with BeautifulSoup using the "lxml"
      parser and reading files using UTF-8 encoding.
    - Extracts text from a curated list of elements (paragraphs, headings,
      tables, table rows/cells, horizontal rules, and images).
    - Applies a set of regular-expression substitutions defined by
      EXTRA_PATTERNS to normalize line text.
    - Detects inline formatting tags using a FORMAT_MAP mapping and determines
      text justification from inline style attributes or class-based styles.
- Provides progress feedback and logging via get_progress(...) and
  get_logger(...), creating Rich-style progress tasks for directories and
  individual HTML file processing.

Primary module-level objects
----------------------------
- EPUB_DIR: pathlib.Path to the base EPUB directory ("static/epub"). Created on import.
- html_json_path: pathlib.Path pointing to "static/json/html.json" (intended
  output path for collected HTML metadata).
- css_json_path: pathlib.Path pointing to "static/json/css.json" (intended
  output path for collected CSS/class-style metadata).
- console, progress, log, _console: logging and progress helpers obtained from
  the project's logging utilities.

Important details & assumptions
------------------------------
- Uses BeautifulSoup with the "lxml" parser; lxml and bs4 must be installed.
- The code expects helper objects/constants to exist in the module scope or
  imported elsewhere, notably:
    - parse_stylesheets(epub_dir, soup): builds class/style mappings for the book.
    - EXTRA_PATTERNS: iterable of dicts with "pattern" (regex) and "sub" (replacement).
    - FORMAT_MAP: mapping of inline tag -> (open_tag, close_tag, description).
- Only processes directories that contain more than one HTML file (heuristic
  chosen by the application).
- Text is extracted using elem.get_text(strip=True); non-text content (e.g.
  images) is included only insofar as they produce a non-empty string.
- Inline style alignment is detected using a regex on an element's "style"
  attribute; class-based alignment can be inferred from parse_stylesheets output.
- Progress/task updates and logging are side effects; the module performs work
  at import time by iterating EPUB_DIR (this may be undesirable in some
  contexts—consider refactoring to an explicit function call if import-time
  execution is not wanted).

Side effects
------------
- Creates the directory "static/epub" if it does not already exist.
- Reads many HTML files from the filesystem and logs progress to the configured
  console. Intended to write or update "static/json/html.json" and
  "static/json/css.json" (paths are declared; actual write behavior is handled
  elsewhere in the codebase).

Error handling
--------------
- File I/O and parsing errors may be raised at runtime (e.g., FileNotFoundError,
  UnicodeDecodeError, bs4-related exceptions). Upstream code should catch or
  guard against these as needed.
- The module presumes that the auxiliary variables and helpers referenced
  (EXTRA_PATTERNS, FORMAT_MAP, parse_stylesheets, logger/progress providers)
  are present and correctly implemented.

Example (conceptual)
--------------------
The module is intended to be used as part of a command-line or build step that
prepares HTML text and CSS metadata for downstream consumption (search indexing,
JSON APIs, or static site generation). It is not designed as a reusable import
API without refactoring the import-time processing into callable functions.
"""
import re
from itertools import chain
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from bs4 import BeautifulSoup, SoupStrainer

from primal_hunter.logger import get_console, get_logger, get_progress
from primal_hunter.scripts.loop import EXTRA_PATTERNS, FORMAT_MAP, parse_stylesheets

TARGET_TAGS: Sequence[str] = (
    "p",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "table",
    "tr",
    "th",
    "td",
    "img",
)

TEXT_ALIGN_RE = re.compile(r"text-align\s*:\s*(\w+)")
TARGET_STRAINER = SoupStrainer(TARGET_TAGS)
NORMALIZATION_PATTERNS = [
    (re.compile(pattern_dict["pattern"]), pattern_dict["sub"])
    for pattern_dict in EXTRA_PATTERNS
]


def iter_book_dirs(root: Path) -> Iterator[Path]:
    """Yield directory entries under ``root``."""
    for entry in root.iterdir():
        if entry.is_dir():
            yield entry


def iter_html_files(book_dir: Path) -> Iterator[Path]:
    """Yield HTML files for ``book_dir`` using a streaming iterator."""
    for path in book_dir.rglob("*.html"):
        if path.is_file() and path.suffix.lower() == ".html":
            yield path

_console = get_console()
progress = get_progress(_console)
log = get_logger(console=progress.console)
console = progress.console
EPUB_DIR = Path("static") / "epub"


EPUB_DIR.mkdir(parents=True, exist_ok=True)
html_json_path = Path("static") / "json" / "html.json"
css_json_path = Path("static") / "json" / "css.json"

with progress:
    dir_count = sum(1 for _ in iter_book_dirs(EPUB_DIR))
    book_task = progress.add_task(
        "[cyan]Processing book directories...",
        total=dir_count or None,
    )
    for book_dir in iter_book_dirs(EPUB_DIR):
        log.info(f"Processing EPUB directory: {book_dir}")

        html_iter = iter_html_files(book_dir)
        first_html = next(html_iter, None)
        second_html = next(html_iter, None)

        if not first_html or not second_html:
            progress.update(book_task, advance=1)
            continue

        with first_html.open("r", encoding="utf-8") as first_file:
            first_soup = BeautifulSoup(first_file, "lxml")

        class_styles = parse_stylesheets(str(book_dir), first_soup)
        first_soup.decompose()

        html_files_iter = chain([first_html, second_html], html_iter)
        html_file_task = progress.add_task(
            f"[green]Processing HTML files in {book_dir.name}...",
            total=None,
        )

        for html_file in html_files_iter:
            with html_file.open("r", encoding="utf-8") as handle:
                soup = BeautifulSoup(handle, "lxml", parse_only=TARGET_STRAINER)

            for elem in soup.find_all(TARGET_TAGS):
                text = elem.get_text(strip=True)
                if not text:
                    continue

                normalized_text = text
                for pattern, replacement in NORMALIZATION_PATTERNS:
                    normalized_text = pattern.sub(replacement, normalized_text)

                justify = "left"
                style_attr = elem.get("style")
                if style_attr and (match := TEXT_ALIGN_RE.search(str(style_attr))):
                    justify = match.group(1)

                classes_attr = elem.get("class")
                if isinstance(classes_attr, str):
                    class_names: Iterable[str] = [classes_attr]
                elif isinstance(classes_attr, (list, tuple)):
                    class_names = [str(c) for c in classes_attr]
                else:
                    class_names = []

                for class_name in class_names:
                    props = class_styles.get(class_name)
                    if props and props.get("text-align"):
                        justify = props["text-align"]
                        break

                # Further processing/writing occurs elsewhere in the pipeline.

            soup.decompose()
            progress.update(html_file_task, advance=1)
            del soup

        progress.remove_task(html_file_task)
        progress.update(book_task, advance=1)
