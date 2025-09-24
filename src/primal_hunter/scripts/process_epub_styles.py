"""Process EPUB HTML files by inlining CSS formatting and exporting outputs.

This script walks the ``static/epub`` directory and, for every book folder,
performs the following steps:

* Discover all CSS rules (external ``.css`` files and inline ``<style>`` blocks)
  and build a mapping of selectors to the formatting attributes that influence
  bold, italic, underline and text alignment.
* Collapse identical style declarations so they can be reused across selectors.
* Apply the discovered styles to every HTML document in the book by converting
  class/id driven formatting into inline ``style=`` declarations.
* Emit cleaned HTML copies to ``static/html/books/{book}/{chapter:04}.html``.
* Convert the chapter HTML to Markdown using Pandoc and write to
  ``static/markdowm/books/{book}/{chapter:04}.md`` (``markdowm`` matches the
  requested directory name).
* Collect every snippet of formatted text and record it in
  ``static/json/format.json``.

The script is designed to be re-runnable: output directories are created on
 demand and existing files are overwritten.
"""
from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, MutableMapping, Optional, Sequence, Tuple

import cssutils
from bs4 import BeautifulSoup, Tag

from primal_hunter.pandoc_sh import Pandoc, PandocError


LOGGER = logging.getLogger(__name__)
cssutils.log.setLevel(logging.CRITICAL)

# Properties we care about and their canonical representation
PropertyDict = Dict[str, str]
SelectorStyles = Dict[str, PropertyDict]
StyleGroup = Dict[str, Sequence[str] | PropertyDict]

INTERESTING_PROPERTIES = {"font-weight", "font-style", "text-decoration", "text-align"}


def find_book_directories(root: Path) -> List[Path]:
    """Return a sorted list of book directories inside ``root``."""
    return sorted([p for p in root.iterdir() if p.is_dir()])


def extract_book_number(name: str, default: int) -> int:
    """Extract the trailing integer from a directory name.

    Args:
        name: Directory name to inspect.
        default: Value returned if no integer can be located.
    """
    match = re.search(r"(\d+)(?!.*\d)", name)
    if match:
        return int(match.group(1))
    return default


def parse_css_sources(css_files: Iterable[Path], inline_css: Iterable[str]) -> Tuple[SelectorStyles, List[StyleGroup]]:
    """Parse CSS from files and inline blocks, returning selector mappings and grouped styles."""
    selector_map: SelectorStyles = {}
    grouped_styles: Dict[Tuple[Tuple[str, str], ...], StyleGroup] = {}

    for css_path in css_files:
        try:
            sheet = cssutils.parseFile(css_path)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("Failed to parse CSS file %s: %s", css_path, exc)
            continue
        _merge_sheet(sheet, selector_map, grouped_styles)

    for css_text in inline_css:
        if not css_text.strip():
            continue
        try:
            sheet = cssutils.parseString(css_text)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("Failed to parse inline CSS block: %s", exc)
            continue
        _merge_sheet(sheet, selector_map, grouped_styles)

    # Convert grouped_styles to a list with stable ordering for potential serialization
    ordered_groups: List[StyleGroup] = []
    for _, group in sorted(grouped_styles.items(), key=lambda item: item[0]):
        ordered_groups.append(
            {
                "properties": dict(group["properties"]),
                "selectors": tuple(sorted(set(group["selectors"]))),
            }
        )
    return selector_map, ordered_groups


def _merge_sheet(
    sheet: cssutils.css.CSSStyleSheet,
    selector_map: SelectorStyles,
    grouped_styles: MutableMapping[Tuple[Tuple[str, str], ...], StyleGroup],
) -> None:
    """Read a cssutils stylesheet and merge its style rules into the mappings."""
    for rule in sheet:
        if rule.type != rule.STYLE_RULE:
            continue

        normalized_props: PropertyDict = {}
        for declaration in rule.style:
            name = declaration.name.lower()
            if name not in INTERESTING_PROPERTIES:
                continue
            normalized_props.update(_normalize_property(name, declaration.value))
        if not normalized_props:
            continue

        key = tuple(sorted(normalized_props.items()))
        group = grouped_styles.setdefault(
            key,
            {
                "properties": dict(normalized_props),
                "selectors": [],
            },
        )

        for selector in rule.selectorList:
            selector_text = selector.selectorText.strip()
            if not selector_text:
                continue
            group_selectors = list(group["selectors"])
            group_selectors.append(selector_text)
            group["selectors"] = group_selectors
            existing = selector_map.get(selector_text, {})
            selector_map[selector_text] = merge_styles(existing, normalized_props)


def collapse_style_groups(groups: Iterable[StyleGroup]) -> List[StyleGroup]:
    """Merge groups that share identical properties by combining their selectors."""
    collapsed: Dict[Tuple[Tuple[str, str], ...], Dict[str, object]] = {}
    for group in groups:
        props = dict(group["properties"])
        selectors = group["selectors"]
        props_key = tuple(sorted(props.items()))
        entry = collapsed.setdefault(props_key, {"properties": props, "selectors": set()})
        entry_selectors = entry["selectors"]
        if isinstance(entry_selectors, set):
            entry_selectors.update(selectors)
        else:  # pragma: no cover - defensive
            entry_selectors = set(entry_selectors)
            entry_selectors.update(selectors)
            entry["selectors"] = entry_selectors
    collapsed_groups: List[StyleGroup] = []
    for entry in collapsed.values():
        selectors_set = entry["selectors"]
        if isinstance(selectors_set, set):
            selectors_tuple = tuple(sorted(selectors_set))
        else:  # pragma: no cover - defensive
            selectors_tuple = tuple(sorted(selectors_set))
        collapsed_groups.append({"properties": entry["properties"], "selectors": selectors_tuple})
    return collapsed_groups


def _normalize_property(name: str, value: str) -> PropertyDict:
    """Normalize a CSS property value for the properties we track."""
    cleaned_value = value.strip().lower().split("!important")[0].strip()
    if name == "font-weight":
        if cleaned_value.isdigit():
            try:
                weight = int(cleaned_value)
            except ValueError:  # pragma: no cover - defensive
                return {}
            if weight > 400:
                return {"font-weight": "bold"}
        elif cleaned_value in {"bold", "bolder"}:
            return {"font-weight": "bold"}
        return {}
    if name == "font-style":
        if "italic" in cleaned_value or "oblique" in cleaned_value:
            return {"font-style": "italic"}
        return {}
    if name == "text-decoration":
        if "underline" in cleaned_value:
            return {"text-decoration": "underline"}
        return {}
    if name == "text-align":
        # Keep first token; it represents the alignment keyword.
        align = cleaned_value.split()[0]
        return {"text-align": align}
    return {}


def collect_inline_css(soup: BeautifulSoup) -> List[str]:
    """Return the contents of all <style> blocks."""
    inline_css: List[str] = []
    for style_tag in soup.find_all("style"):
        if isinstance(style_tag.string, str):
            inline_css.append(style_tag.string)
    return inline_css


def parse_document(html_text: str) -> BeautifulSoup:
    """Parse XHTML content with an XML parser, falling back to HTML when needed."""
    try:
        return BeautifulSoup(html_text, "xml")
    except Exception as exc:  # pragma: no cover - defensive fallback
        LOGGER.debug("Falling back to HTML parser for document: %s", exc)
        return BeautifulSoup(html_text, "lxml")


def parse_inline_style(style_value: Optional[str]) -> PropertyDict:
    """Parse an inline ``style`` attribute into a dictionary."""
    if not style_value:
        return {}
    styles: PropertyDict = {}
    for part in style_value.split(";"):
        if not part.strip() or ":" not in part:
            continue
        key, val = part.split(":", 1)
        key = key.strip().lower()
        val = val.strip().lower()
        if not key or not val:
            continue
        styles[key] = val
    return styles


def features_from_tag(tag: Tag) -> PropertyDict:
    """Return formatting implied by the HTML tag itself."""
    name = tag.name.lower()
    if name in {"b", "strong"}:
        return {"font-weight": "bold"}
    if name in {"i", "em"}:
        return {"font-style": "italic"}
    if name == "u":
        return {"text-decoration": "underline"}
    return {}


def merge_styles(base: PropertyDict, extra: PropertyDict) -> PropertyDict:
    """Return a new dict that merges ``extra`` into ``base`` (``extra`` wins)."""
    result: PropertyDict = dict(base)
    result.update(extra)
    return result


def apply_styles_to_soup(
    soup: BeautifulSoup,
    selector_styles: SelectorStyles,
    book_number: int,
    chapter_index: int,
) -> Tuple[str, List[Dict[str, object]]]:
    """Apply CSS-derived styles to a BeautifulSoup tree and return HTML + format records."""
    element_styles: defaultdict[int, PropertyDict] = defaultdict(dict)

    for selector, style in selector_styles.items():
        try:
            matched_elements = soup.select(selector)
        except Exception:  # pragma: no cover - defensive
            LOGGER.debug("Skipping unsupported selector: %s", selector)
            continue
        for element in matched_elements:
            eid = id(element)
            element_styles[eid] = merge_styles(element_styles[eid], style)

    formatted_entries: List[Dict[str, object]] = []

    for element in soup.find_all(True):
        eid = id(element)
        computed_style = dict(element_styles.get(eid, {}))

        inline_style = parse_inline_style(element.attrs.get("style"))
        computed_style = merge_styles(computed_style, inline_style)
        computed_style = merge_styles(computed_style, features_from_tag(element))

        if not computed_style:
            if "class" in element.attrs:
                del element.attrs["class"]
            continue

        labels: List[str] = []
        style_to_apply: PropertyDict = {}

        if computed_style.get("font-weight") in {"bold", "700", "600", "800", "900"}:
            style_to_apply["font-weight"] = "bold"
            labels.append("bold")
        if computed_style.get("font-style") in {"italic", "oblique"}:
            style_to_apply["font-style"] = "italic"
            labels.append("italic")
        text_decoration = computed_style.get("text-decoration")
        if text_decoration and "underline" in text_decoration:
            style_to_apply["text-decoration"] = "underline"
            labels.append("underline")
        if computed_style.get("text-align"):
            style_to_apply["text-align"] = computed_style["text-align"]
            labels.append(f"text-align:{computed_style['text-align']}")

        # Merge the filtered style with any other inline attributes that should persist
        remaining_inline = {
            key: value
            for key, value in computed_style.items()
            if key not in style_to_apply
        }
        final_style = merge_styles(remaining_inline, style_to_apply)

        if final_style:
            style_parts = [f"{prop}:{val}" for prop, val in sorted(final_style.items())]
            element["style"] = ";".join(style_parts) + ";"
        else:
            element.attrs.pop("style", None)

        if "class" in element.attrs:
            del element.attrs["class"]

        text_content = element.get_text(strip=True)
        if text_content and labels:
            formatted_entries.append(
                {
                    "book": book_number,
                    "chapter": chapter_index,
                    "element": element.name,
                    "text": text_content,
                    "format": ", ".join(sorted(labels)),
                }
            )

    html_output = soup.decode()
    return html_output, formatted_entries


def convert_html_to_markdown(pandoc: Pandoc, html: str) -> str:
    """Convert HTML string to Markdown using pandoc."""
    return pandoc.convert_string(html, from_format="html", to_format="gfm")


def process_book(
    book_dir: Path,
    html_output_dir: Path,
    markdown_output_dir: Path,
    pandoc: Pandoc,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    """Process every HTML file in a book directory."""
    html_files = sorted(book_dir.rglob("*.html"))
    if not html_files:
        LOGGER.info("No HTML files found in %s", book_dir)
        return [], {"selectors": {}, "groups": []}

    # Use the first HTML file to collect inline style blocks; they often share the same CSS.
    with open(html_files[0], "r", encoding="utf-8") as fp:
        soup_sample = parse_document(fp.read())

    inline_css_blocks = collect_inline_css(soup_sample)
    css_files = sorted(book_dir.rglob("*.css"))
    base_selector_styles, base_style_groups = parse_css_sources(css_files, inline_css_blocks)

    book_number = extract_book_number(book_dir.name, default=0)
    formatted_entries: List[Dict[str, object]] = []
    summary_selector_styles: SelectorStyles = dict(base_selector_styles)
    summary_style_groups: List[StyleGroup] = list(base_style_groups)

    for chapter_index, html_path in enumerate(html_files, start=1):
        with open(html_path, "r", encoding="utf-8") as fp:
            soup = parse_document(fp.read())

        # Include inline CSS from each chapter as well.
        inline_blocks = collect_inline_css(soup)
        if inline_blocks:
            inline_selector_styles, inline_groups = parse_css_sources([], inline_blocks)
            combined_styles = dict(base_selector_styles)
            combined_styles.update(inline_selector_styles)
            summary_selector_styles.update(inline_selector_styles)
            summary_style_groups.extend(inline_groups)
        else:
            combined_styles = base_selector_styles

        html_string, entries = apply_styles_to_soup(
            soup,
            combined_styles,
            book_number,
            chapter_index,
        )
        formatted_entries.extend(entries)

        chapter_html_path = html_output_dir / f"{chapter_index:04}.html"
        chapter_html_path.parent.mkdir(parents=True, exist_ok=True)
        chapter_html_path.write_text(html_string, encoding="utf-8")

        try:
            markdown = convert_html_to_markdown(pandoc, html_string)
        except PandocError as exc:  # pragma: no cover - defensive
            LOGGER.error("Pandoc conversion failed for %s: %s", html_path, exc)
            raise

        chapter_markdown_path = markdown_output_dir / f"{chapter_index:04}.md"
        chapter_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        chapter_markdown_path.write_text(markdown, encoding="utf-8")

    css_summary = {
        "selectors": summary_selector_styles,
        "groups": collapse_style_groups(summary_style_groups),
    }
    return formatted_entries, css_summary


def write_format_records(path: Path, entries: List[Dict[str, object]]) -> None:
    """Write formatted text records to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8")


def write_css_summary(path: Path, summary: Dict[int, Dict[str, object]]) -> None:
    """Persist CSS summary data for inspection."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    root = Path("static") / "epub"
    html_root = Path("static") / "html" / "books"
    markdown_root = Path("static") / "markdowm" / "books"
    format_json_path = Path("static") / "json" / "format.json"
    css_summary_path = Path("static") / "json" / "css_styles.json"

    book_dirs = find_book_directories(root)
    if not book_dirs:
        LOGGER.warning("No book directories found under %s", root)
        return

    all_format_records: List[Dict[str, object]] = []
    css_summary: Dict[int, Dict[str, object]] = {}

    with Pandoc() as pandoc:
        for index, book_dir in enumerate(book_dirs, start=1):
            book_number = extract_book_number(book_dir.name, default=index)
            LOGGER.info("Processing book %s (parsed number %s)", book_dir.name, book_number)
            html_output_dir = html_root / str(book_number)
            markdown_output_dir = markdown_root / str(book_number)
            entries, summary = process_book(
                book_dir,
                html_output_dir,
                markdown_output_dir,
                pandoc,
            )
            all_format_records.extend(entries)
            css_summary[book_number] = summary

    write_format_records(format_json_path, all_format_records)
    write_css_summary(css_summary_path, css_summary)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
