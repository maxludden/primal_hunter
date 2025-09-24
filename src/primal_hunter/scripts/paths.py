"""Locate and record paths to HTML and CSS files in EPUB directories."""
import re
from pathlib import Path
from typing import List, Optional, Any
from bs4 import BeautifulSoup

from primal_hunter.logger import get_console, get_logger, get_progress

_console = get_console()
progress = get_progress(_console)
log = get_logger(console=progress.console)
console = progress.console
EPUB_DIR = Path("static") / "epub"


EPUB_DIR.mkdir(parents=True, exist_ok=True)
html_json_path = Path("static") / "json" / "html.json"
css_json_path = Path("static") / "json" / "css.json"

with progress:
    dir_count = len(list(EPUB_DIR.iterdir()))
    book_task = progress.add_task(
        "[cyan]Processing book directories...",
        total=dir_count
    )
    for book_index, book_dir in enumerate(EPUB_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        log.info(f"Processing EPUB directory: {book_dir}")
        html_file_count = len(list(book_dir.glob("**/*.html")))
        html_file_task = progress.add_task("Searching for HTML files...", total=html_file_count)
        html_files: List[Path] = []
        html_files = [p for p in book_dir.rglob(
            "*.html") if p.is_file() and p.suffix.lower() == ".html"
        ]
        if len(html_files) > 1:
            # build class_styles by parsing first file's soup (stylesheet
            #   links should be in all)
            with open(html_files[0], "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f, "lxml")
            class_styles = parse_stylesheets(epub_dir, soup)

            # Process each HTML file in the directory
            all_lines = []
            file_task = progress.add_task(
                f"[green]Processing HTML files in {book_dir.name}...",
                total=len(html_files),
            )
            for index, html_file in enumerate(html_files, start=1):
                with open(html_file, "r", encoding="utf-8") as f:
                    soup = BeautifulSoup(f, "lxml")

                # Extract lines from paragraphs and headings
                elements_with_text = []
                for elem in soup.find_all([
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
                ]):
                    text = elem.get_text(strip=True)
                    if text:
                        elements_with_text.append((elem, text))
                for elem, text in elements_with_text:
                    # Apply regex substitutions
                    for pattern_dict in EXTRA_PATTERNS:
                        pattern = pattern_dict["pattern"]
                        sub = pattern_dict["sub"]
                        text = re.sub(pattern, sub, text)

                    # Determine formatting and justification
                    fmt_tags = []
                    justify = "left"  # default justification

                    # Inline tag formatting
                    for tag, (open_tag, close_tag, desc) in FORMAT_MAP.items():
                        if elem.find(tag):
                            fmt_tags.append(desc)

                    # Inline style alignment
                    if soup.has_attr("style"):
                        if m := re.search(
                            r"text-align\s*:\s*(\w+)", str(soup["style"])
                        ):
                            justify = m[1]

                    # Class-based styles
                    classes_attr = soup.get("class")
                    if isinstance(classes_attr, (list, tuple)):
                        classes: List[str] = [str(c) for c in classes_attr]
