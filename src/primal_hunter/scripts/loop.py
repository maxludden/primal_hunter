#!/usr/bin/env python3
"""This script processes EPUB directories to extract and format lines from XHTML files.
It parses linked CSS stylesheets to determine text formatting and justification,
applies regex substitutions for specific patterns, and outputs the results to a JSON file.
"""
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, TextIO, Tuple
from xml.parsers import expat

import cssutils
from bs4 import BeautifulSoup, Tag

from primal_hunter.logger import get_console, get_logger, get_progress



_console = get_console()
progress = get_progress(_console)
log = get_logger(console=progress.console)
console = progress.console

# Regex substitutions provided
EXTRA_PATTERNS: List[Dict[str, str]] = [
    {"pattern": r"^(.*amazon.*)$", "sub": ""},
    {"pattern": r"^(.*\[(.+?)(?:\s*[-–]\s*(?:lvl\s*)?\d+)?\].*)$", "sub": r"**\1**"},
]

# Map HTML tags to replacements
FORMAT_MAP: Dict[str, Tuple[str, str, str]] = {
    "i": ("[i]", "[/i]", "italic"),
    "em": ("*", "*", "italic"),
    "b": ("**", "**", "bold"),
    "strong": ("**", "**", "bold"),
    "u": ("__", "__", "underline"),
}


class _JsonArrayWriter:
    """Streaming JSON array writer to avoid buffering large result sets."""

    def __init__(self, path: str, indent: int = 4) -> None:
        self._path = Path(path)
        self._file: Optional[TextIO] = None
        self._count = 0
        self._indent = indent

    def __enter__(self) -> "_JsonArrayWriter":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._path.open("w", encoding="utf-8")
        self._file.write("[")
        return self

    def write(self, item: Dict[str, Any]) -> None:
        if self._file is None:
            raise RuntimeError("JSON writer not initialized")
        if self._count:
            self._file.write(",\n")
        else:
            self._file.write("\n")
        dumped = json.dumps(item, ensure_ascii=False, indent=self._indent)
        if self._indent > 0:
            pad = " " * self._indent
            dumped = "\n".join(f"{pad}{line}" for line in dumped.splitlines())
        self._file.write(dumped)
        self._count += 1

    def write_many(self, items: Iterable[Dict[str, Any]]) -> None:
        for item in items:
            self.write(item)

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[Any],
    ) -> None:
        if self._file is None:
            return
        if self._count:
            self._file.write("\n")
        self._file.write("]\n")
        self._file.close()
        self._file = None


def parse_stylesheets(base_dir: str, soup: BeautifulSoup) -> Dict[str, Dict[str, str]]:
    """Parse linked CSS files for an EPUB folder and build a class->properties dict."""
    log.trace(f"Entered parse_stylesheets(\n\tbase_dir={base_dir},\n\tsoup={soup.prettify()[:50]}...\n)")
    styles: Dict[str, Dict[str, str]] = {}

    for link in soup.find_all("link", rel="stylesheet"):
        # Narrow the type to Tag before accessing .get to satisfy type checkers
        if not isinstance(link, Tag):
            continue
        href = link.get("href")
        if not href:
            continue
        # coerce href to a string (BeautifulSoup can return lists for multi-valued attributes)
        if isinstance(href, (list, tuple)):
            href_str = href[0] if href else ""
        else:
            href_str = str(href)
        css_path = Path(base_dir) / href_str
        if not css_path.exists():
            continue
        try:
            sheet = cssutils.parseFile(css_path)
        except (OSError, expat.ExpatError) as e:  # pragma: no cover - defensive
            log.warning(f"Failed to parse CSS file {css_path}: {e}")
            # skip files that fail to parse
            continue

        for rule in sheet:
            if rule.type != rule.STYLE_RULE:
                continue
            for selector in rule.selectorList:
                sel = selector.selectorText.strip()
                if not sel.startswith("."):  # only class selectors
                    continue
                cls = sel[1:]
                # use a dict comprehension to build property mapping
                props: Dict[str, str] = {
                    p.name.lower(): p.value.lower() for p in rule.style
                }
                styles[cls] = props
    return styles


def detect_format_and_justify(
    soup: Tag, class_styles: Dict[str, Dict[str, str]]
) -> Tuple[str, str, Optional[str], str]:
    """Return (text, new_line, format_str, justify) given a BeautifulSoup fragment."""
    text: str = soup.get_text()
    new_line: str = text
    formats: Set[str] = set()
    justify: str = "default"

    # Tag-based formatting
    for tag, (start, end, label) in FORMAT_MAP.items():
        for el in soup.find_all(tag):
            formats.add(label)
            new_line = new_line.replace(el.get_text(), f"{start}{el.get_text()}{end}")

    # Inline style alignment
    if soup.has_attr("style"):
        if (m := re.search(r"text-align\s*:\s*(\w+)", str(soup["style"]))):
            justify = m[1]

    # Class-based styles
    classes_attr = soup.get("class")
    if isinstance(classes_attr, (list, tuple)):
        classes: List[str] = [str(c) for c in classes_attr]
    elif isinstance(classes_attr, str):
        classes = [classes_attr]
    else:
        classes = []

    for cls in classes:
        if cls in class_styles:
            props = class_styles[cls]
            if props.get("font-weight") == "bold":
                formats.add("bold")
                new_line = f"**{new_line}**"
            if props.get("font-style") == "italic":
                formats.add("italic")
                new_line = f"*{new_line}*"
            if "text-decoration" in props and "underline" in props["text-decoration"]:
                formats.add("underline")
                new_line = f"__{new_line}__"
            if "text-align" in props:
                justify = props["text-align"]

    return (
        text.strip(),
        new_line.strip(),
        " ".join(sorted(formats)) if formats else None,
        justify,
    )


def process_xhtml_file(
    filepath: str, class_styles: Dict[str, Dict[str, str]]
) -> Iterator[Dict[str, Any]]:
    """Yield formatted/justified lines from an XHTML file without buffering all rows."""
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if any(
                tag in line
                for tag in [
                    "<i>",
                    "<em>",
                    "<b>",
                    "<strong>",
                    "<u>",
                    "class=",
                    "text-align",
                ]
            ):
                soup = BeautifulSoup(line, "lxml")
                text, new_line, fmt, justify = detect_format_and_justify(
                    soup, class_styles
                )
                if fmt:
                    yield {
                        "line": text,
                        "new_line": new_line,
                        "format": fmt,
                        "justify": justify,
                    }


def apply_extra_patterns(
    entries: Iterable[Dict[str, Any]]
) -> Iterator[Dict[str, Any]]:
    """Apply regex substitutions to extra cases like Amazon links and level brackets."""
    for entry in entries:
        text: str = entry["line"]
        for rule in EXTRA_PATTERNS:
            if re.match(rule["pattern"], text):
                entry["new_line"] = re.sub(rule["pattern"], rule["sub"], text)
        yield entry


def main(
    root_epub_dir: str = "/Users/maxludden/dev/py/primal_hunter/static/epub",
    output_file: str = "formatted_lines.json",
) -> None:
    """Process EPUB directories to extract and format lines from XHTML files.

    This function walks through the given EPUB root directory, parses stylesheets,
    extracts formatted and justified lines from XHTML files, applies extra regex patterns,
    and writes the results to a JSON file.

    Args:
        root_epub_dir: The root directory containing EPUB folders.
        output_file: The path to the output JSON file.

    Returns:
        None
    """
    with _JsonArrayWriter(output_file) as writer:
        for root, dirs, _ in os.walk(root_epub_dir):
            for d in dirs:
                epub_dir = os.path.join(root, d)
                xhtml_files = [
                    os.path.join(epub_dir, f)
                    for f in os.listdir(epub_dir)
                    if f.endswith(".xhtml")
                ]
                if len(xhtml_files) > 1:
                    with open(xhtml_files[0], "r", encoding="utf-8") as f:
                        soup = BeautifulSoup(f, "lxml")
                    class_styles = parse_stylesheets(epub_dir, soup)

                    for file in xhtml_files:
                        entries = process_xhtml_file(file, class_styles)
                        writer.write_many(apply_extra_patterns(entries))


if __name__ == "__main__":
    main()
