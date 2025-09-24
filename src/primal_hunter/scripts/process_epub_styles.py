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
  ``static/markdown/books/{book}/{chapter:04}.md`` (``markdown`` matches the
  requested directory name).
* Collect every snippet of formatted text and record it in
  ``static/json/format.json``.

The script is designed to be re-runnable: output directories are created on
 demand and existing files are overwritten.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    TextIO,
    Tuple,
    TypedDict,
)

import cssutils
from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString
from rich.console import Console
from rich.progress import Progress
from rich.panel import Panel
from rich.markdown import Markdown
from rich import inspect
from rich_gradient import Gradient

from primal_hunter.logger import get_console, get_logger, get_progress
from primal_hunter.pandoc_sh import Pandoc, PandocError

_console: Console = get_console()
progress: Progress = get_progress(_console)
console: Console = progress.console
log: Any = get_logger(console=_console)

cssutils.log.setLevel("CRITICAL")

# Properties we care about and their canonical representation
PropertyDict = Dict[str, str]
SelectorStyles = Dict[str, PropertyDict]


class StyleGroup(TypedDict):
    """Mapping of selectors that share the same normalized CSS properties."""

    properties: PropertyDict
    selectors: Sequence[str]


class FormattedEntry(TypedDict):
    """Structured record describing formatted text discovered in a chapter."""

    book: int
    chapter: int
    element: str
    text: str
    format: str


class CssSummary(TypedDict):
    """Aggregated CSS selector data for a processed book."""

    selectors: SelectorStyles
    groups: List[StyleGroup]


INTERESTING_PROPERTIES = {"font-weight", "font-style", "text-decoration", "text-align"}


class _JsonArrayWriter:
    """Streaming writer that emits a JSON array without buffering all rows."""

    def __init__(self, path: Path, indent: int = 2) -> None:
        self._path = path
        self._file: Optional[TextIO] = None
        self._count = 0
        self._indent = indent

    def __enter__(self) -> "_JsonArrayWriter":
        """Open the output file and write the opening bracket for the JSON array."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._path.open("w", encoding="utf-8")
        self._file.write("[")
        return self

    def write(self, item: FormattedEntry) -> None:
        """Write a single JSON object to the stream, inserting separators as needed."""
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

    def write_many(self, items: Iterable[FormattedEntry]) -> None:
        """Write multiple JSON objects to the stream in sequence."""
        for item in items:
            self.write(item)

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[Any],
    ) -> None:
        """Close the JSON array, finalise the file, and release the file handle."""
        if self._file is None:
            return
        if self._count:
            self._file.write("\n")
        self._file.write("]\n")
        self._file.close()
        self._file = None


def find_book_directories(root: Path) -> List[Path]:
    """Return a sorted list of book directories located beneath ``root``.

    Args:
        root: Root directory containing one subdirectory per book.

    Returns:
        Sorted list of discovered book directory paths.
    """
    log.trace(f"Entered find_book_directories(root={root})")
    book_dirs: List[Path] = sorted([p for p in root.iterdir() if p.is_dir()])
    log.trace(
        f"Found book directories: {', '.join(str(book_dir) for book_dir in book_dirs)}"
    )
    return book_dirs


def extract_book_number(name: str, default: int) -> int:
    """Extract the trailing integer from a directory name, if present.

    Args:
        name: Directory name to inspect.
        default: Value returned when no integer suffix can be located.

    Returns:
        Trailing integer parsed from ``name`` or the supplied ``default``.
    """
    log.trace(f"Entered extract_book_number(name={name}, default={default})")
    match = re.search(r"(\d+)(?!.*\d)", name)
    if match:
        return int(match.group(1))
    return default


def parse_css_sources(
    css_files: Iterable[Path],
    inline_css: Iterable[str]
) -> Tuple[SelectorStyles, List[StyleGroup]]:
    """Parse CSS inputs and return selector mappings and grouped styles.

    Args:
        css_files: Paths to CSS files discovered for the current book.
        inline_css: Raw inline ``<style>`` blocks extracted from HTML.

    Returns:
        Tuple containing the selector-to-style mapping and grouped style summaries.
    """
    # Materialize the incoming iterables to lists so we can safely take len()
    # and iterate multiple times without risking consumption of generators.
    css_list: List[Path] = list(css_files)
    inline_list: List[str] = list(inline_css)

    log.trace(
        f"Entered parse_css_sources(css_files={css_list}, "
        f"inline_css=[{len(inline_list)} blocks])"
    )
    selector_map: SelectorStyles = {}
    grouped_styles: Dict[Tuple[Tuple[str, str], ...], StyleGroup] = {}

    for css_path in css_list:
        try:
            sheet = cssutils.parseFile(css_path)

            # Debug output of parsed CSS rules
            log.trace(
                f"Parsed CSS file {str(css_path.resolve())} \
with {len(sheet.cssRules)} rules"
            )
            inspect(sheet, console=console)
            console.print(
                Panel(
                    sheet,
                    title="CSS File",
                    subtitle=str(css_path),
                    subtitle_align="right"
                )
            )
            inspect(sheet, console=console)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning(f"Failed to parse CSS file {css_path}:\n\n {exc}")
            continue
        _merge_sheet(sheet, selector_map, grouped_styles)

    for css_text in inline_list:
        if not css_text.strip():
            continue
        try:
            sheet = cssutils.parseString(css_text)

            log.trace(
                f"Parsed CSS file {css_text} with {len(sheet.cssRules)} rules"
            )
            console.print(
                Gradient(
                    Panel(
                        Markdown(sheet),
                        title="CSS File",
                        subtitle=str(css_text),
                        subtitle_align="right"
                    ),
                    colors=['#f00', '#f50', '#f90', '#fc0', '#ff0']
                )
            )
            inspect(sheet, console=console)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("Failed to parse inline CSS block: %s", exc)
            continue
        _merge_sheet(sheet, selector_map, grouped_styles)

    # Convert grouped_styles to a list with stable ordering for potential serialization
    ordered_groups: List[StyleGroup] = []
    for _, group in sorted(grouped_styles.items(), key=lambda item: item[0]):
        # Ensure property keys and values are strings (cssutils may yield bytes)
        raw_props = dict(group["properties"])
        props: PropertyDict = {str(k): str(v) for k, v in raw_props.items()}
        # Ensure selectors are strings and unique
        raw_selectors = group.get("selectors", ())
        selectors = tuple(sorted({str(s) for s in raw_selectors}))
        ordered_groups.append({"properties": props, "selectors": selectors})
    return selector_map, ordered_groups


def _merge_sheet(
    sheet: cssutils.css.CSSStyleSheet,
    selector_map: SelectorStyles,
    grouped_styles: MutableMapping[Tuple[Tuple[str, str], ...], StyleGroup],
) -> None:
    """Merge rules from ``sheet`` into the global selector map and grouped styles.

    Args:
        sheet: Parsed stylesheet from either a file or inline block.
        selector_map: Aggregate mapping of selector text to normalized properties.
        grouped_styles: Cache of property tuples to their associated selectors.
    """
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
            {"properties": dict(normalized_props), "selectors": []},
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
    """Combine style groups that share identical properties into a single definition.

    Args:
        groups: Iterable of style groups to be deduplicated.

    Returns:
        List of groups with unique property sets and aggregated selectors.
    """
    collapsed: Dict[Tuple[Tuple[str, str], ...], Set[str]] = {}
    for group in groups:
        props_items = tuple(sorted(dict(group["properties"]).items()))
        selectors_bucket = collapsed.setdefault(props_items, set())
        selectors_bucket.update(map(str, group["selectors"]))

    collapsed_groups: List[StyleGroup] = []
    for props_items, selectors in collapsed.items():
        collapsed_groups.append({
            "properties": dict(props_items),
            "selectors": tuple(sorted(selectors)),
        })
    return collapsed_groups


def _normalize_property(name: str, value: str) -> PropertyDict:
    """Normalize a CSS property to the canonical values tracked by the pipeline.

    Args:
        name: CSS property name, expected to be lower-cased already.
        value: Raw property value as emitted by cssutils.

    Returns:
        Dictionary containing any tracked property/value pairs derived from ``value``.
    """
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
    """Extract the textual contents of every ``<style>`` tag in ``soup``.

    Args:
        soup: Parsed BeautifulSoup document.

    Returns:
        List of CSS strings gathered from inline blocks.
    """
    inline_css: List[str] = []
    for style_tag in soup.find_all("style"):
        # Ensure we have a Tag (not other PageElement types) before accessing .string
        if not isinstance(style_tag, Tag):
            continue
        if isinstance(style_tag.string, str):
            inline_css.append(style_tag.string)
    return inline_css


def parse_document(html_text: str) -> BeautifulSoup:
    """Parse XHTML content and gracefully fall back to an HTML parser if necessary.

    Args:
        html_text: Raw XHTML/HTML text to parse.

    Returns:
        BeautifulSoup tree parsed with the most suitable parser available.
    """
    try:
        return BeautifulSoup(html_text, "xml")
    except Exception as exc:  # pragma: no cover - defensive fallback
        log.debug("Falling back to HTML parser for document: %s", exc)
        return BeautifulSoup(html_text, "lxml")


def parse_inline_style(style_value: Optional[str]) -> PropertyDict:
    """Convert an inline ``style`` attribute string into a normalized dictionary.

    Args:
        style_value: Raw text stored in a ``style`` attribute, or ``None``.

    Returns:
        Dictionary mapping property names to lower-case values.
    """
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
    """Return formatting implied directly by ``tag`` (e.g., ``<b>`` implies bold).

    Args:
        tag: HTML tag to inspect for semantic formatting cues.

    Returns:
        Dictionary containing any tracked feature inferred from ``tag``.
    """
    name = tag.name.lower()
    if name in {"b", "strong"}:
        return {"font-weight": "bold"}
    if name in {"i", "em"}:
        return {"font-style": "italic"}
    if name == "u":
        return {"text-decoration": "underline"}
    return {}


def merge_styles(base: PropertyDict, extra: PropertyDict) -> PropertyDict:
    """Return a new dictionary where ``extra`` overrides keys from ``base``.

    Args:
        base: Existing style mapping to extend.
        extra: Additional properties that take precedence over ``base``.

    Returns:
        Combined dictionary containing the merged styles.
    """
    result: PropertyDict = dict(base)
    result.update(extra)
    return result


def apply_styles_to_soup(
    soup: BeautifulSoup,
    selector_styles: SelectorStyles,
    book_number: int,
    chapter_index: int,
) -> Tuple[str, List[FormattedEntry]]:
    """Apply CSS-derived styles to ``soup`` and capture formatted text occurrences.

    Args:
        soup: Parsed chapter document that will be modified in-place.
        selector_styles: Mapping of CSS selectors to normalized property dictionaries.
        book_number: Numeric identifier associated with the current book directory.
        chapter_index: 1-based index for the chapter within the book.

    Returns:
        Tuple of serialized HTML string and the collected formatted text entries.
    """
    element_styles: defaultdict[int, PropertyDict] = defaultdict(dict)

    for selector, style in selector_styles.items():
        try:
            matched_elements = soup.select(selector)
        except Exception:  # pragma: no cover - defensive
            log.debug("Skipping unsupported selector: %s", selector)
            continue
        for element in matched_elements:
            eid = id(element)
            element_styles[eid] = merge_styles(element_styles[eid], style)

    formatted_entries: List[FormattedEntry] = []
    tag_cascaded_styles: Dict[int, PropertyDict] = {}

    bold_values = {"bold", "700", "600", "800", "900"}
    italic_values = {"italic", "oblique"}

    def normalize_tracked_properties(style: PropertyDict) -> PropertyDict:
        normalized: PropertyDict = {}
        font_weight = style.get("font-weight")
        if font_weight:
            value = str(font_weight).strip().lower()
            if value in bold_values:
                normalized["font-weight"] = "bold"
        font_style = style.get("font-style")
        if font_style:
            value = str(font_style).strip().lower()
            if value in italic_values:
                normalized["font-style"] = "italic"
        text_decoration = style.get("text-decoration")
        if text_decoration:
            if "underline" in str(text_decoration).lower():
                normalized["text-decoration"] = "underline"
        text_align = style.get("text-align")
        if text_align:
            normalized["text-align"] = str(text_align).strip()
        return normalized

    def labels_from_style(style: PropertyDict) -> List[str]:
        labels: List[str] = []
        font_weight = style.get("font-weight")
        if font_weight and str(font_weight).strip().lower() in bold_values:
            labels.append("bold")
        font_style = style.get("font-style")
        if font_style and str(font_style).strip().lower() in italic_values:
            labels.append("italic")
        text_decoration = style.get("text-decoration")
        if text_decoration and "underline" in str(text_decoration).lower():
            labels.append("underline")
        text_align = style.get("text-align")
        if text_align:
            labels.append(f"text-align:{str(text_align).strip()}")
        return labels

    for raw_element in soup.find_all(True):
        # Ensure we are operating on a Tag (not a NavigableString or other PageElement)
        if not isinstance(raw_element, Tag):
            continue
        element = raw_element

        eid = id(element)
        computed_style = dict(element_styles.get(eid, {}))

        # Safely obtain inline style value (attrs can be lists or other types)
        style_attr = element.attrs.get("style")
        inline_style = parse_inline_style(
            style_attr if isinstance(style_attr, str) else None
        )
        computed_style = merge_styles(computed_style, inline_style)
        computed_style = merge_styles(computed_style, features_from_tag(element))

        parent_tag = element.parent if isinstance(element.parent, Tag) else None
        parent_cascade = (
            tag_cascaded_styles.get(id(parent_tag), {}) if parent_tag else {}
        )

        if not computed_style:
            if "class" in element.attrs:
                del element.attrs["class"]
            tag_cascaded_styles[eid] = parent_cascade
            continue

        tracked_properties = normalize_tracked_properties(computed_style)

        # Merge the filtered style with any other inline attributes that should persist
        remaining_inline = {
            key: value
            for key, value in computed_style.items()
            if key not in tracked_properties
        }
        final_style = merge_styles(remaining_inline, tracked_properties)

        if final_style:
            style_parts = [f"{prop}:{val}" for prop, val in sorted(final_style.items())]
            element["style"] = ";".join(style_parts) + ";"
        else:
            element.attrs.pop("style", None)

        if "class" in element.attrs:
            del element.attrs["class"]

        cascaded_style = (
            merge_styles(parent_cascade, final_style) if final_style else parent_cascade
        )
        tag_cascaded_styles[eid] = cascaded_style

    for text_node in soup.find_all(string=True):
        # Only handle NavigableString nodes
        if not isinstance(text_node, NavigableString):
            continue

        normalized_text = " ".join(str(text_node).split())
        if not normalized_text:
            continue

        parent = text_node.parent
        if not isinstance(parent, Tag):
            continue

        # Walk the ancestor chain from the document root down to the immediate parent
        # and merge styles so nearer ancestors override previous values.
        ancestors: List[Tag] = []
        cur: Optional[Tag] = parent
        while isinstance(cur, Tag):
            ancestors.append(cur)
            # Move to the parent, but keep cur typed as Optional[Tag]
            cur = cur.parent if isinstance(cur.parent, Tag) else None
        ancestors.reverse()

        cascaded: PropertyDict = {}
        for anc in ancestors:
            # Selector-derived styles collected earlier
            anc_selector_style = dict(element_styles.get(id(anc), {}))
            # Inline style attribute on the ancestor
            anc_style_attr = anc.attrs.get("style")
            anc_inline = parse_inline_style(
                anc_style_attr if isinstance(anc_style_attr, str) else None
            )
            # Tag-implied formatting like <b>/<i>/<u>
            anc_tag_features = features_from_tag(anc)

            # Combine selector -> inline -> tag features for this ancestor, then merge into cascaded
            anc_combined = merge_styles(
                merge_styles(anc_selector_style, anc_inline), anc_tag_features
            )
            cascaded = merge_styles(cascaded, anc_combined)

        # Reduce cascaded to the tracked properties and produce labels
        tracked_props = normalize_tracked_properties(cascaded)
        labels = labels_from_style(tracked_props)
        if not labels:
            continue

        entry: FormattedEntry = {
            "book": book_number,
            "chapter": chapter_index,
            "element": parent.name,
            "text": normalized_text,
            "format": ", ".join(sorted(labels)),
        }
        formatted_entries.append(entry)

    html_output = soup.decode()
    return html_output, formatted_entries


def convert_html_to_markdown(pandoc: Pandoc, html: str) -> str:
    """Convert an HTML fragment to GitHub-flavoured Markdown via Pandoc.

    Args:
        pandoc: Pandoc wrapper responsible for performing the conversion.
        html: HTML string that should be converted to Markdown.

    Returns:
        Markdown output produced by Pandoc.
    """
    return pandoc.convert_string(html, from_format="html", to_format="gfm")


def process_book(
    book_dir: Path,
    html_output_dir: Path,
    markdown_output_dir: Path,
    pandoc: Pandoc,
    entry_sink: Callable[[Iterable[FormattedEntry]], None],
) -> CssSummary:
    """Process every HTML file in ``book_dir`` and stream formatted entries.

    Args:
        book_dir: Directory containing the EPUB's extracted HTML assets.
        html_output_dir: Destination directory for generated inline-styled HTML.
        markdown_output_dir: Destination directory for Markdown conversions.
        pandoc: Pandoc wrapper configured for conversions.
        entry_sink: Callback that consumes formatted text entries for persistence.

    Returns:
        CSS summary describing selectors and grouped property declarations.
    """
    html_files = sorted(book_dir.rglob("*.html"))
    if not html_files:
        log.info("No HTML files found in %s", book_dir)
        empty_summary: CssSummary = {"selectors": {}, "groups": []}
        return empty_summary

    # Use the first HTML file to collect inline style blocks; they often share the same CSS.
    with open(html_files[0], "r", encoding="utf-8") as fp:
        soup_sample = parse_document(fp.read())

    inline_css_blocks = collect_inline_css(soup_sample)
    css_files = sorted(book_dir.rglob("*.css"))
    base_selector_styles, base_style_groups = parse_css_sources(
        css_files, inline_css_blocks
    )

    book_number = extract_book_number(book_dir.name, default=0)
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
        entry_sink(entries)

        chapter_html_path = html_output_dir / f"{chapter_index:04}.html"
        chapter_html_path.parent.mkdir(parents=True, exist_ok=True)
        chapter_html_path.write_text(html_string, encoding="utf-8")

        try:
            markdown = convert_html_to_markdown(pandoc, html_string)
        except PandocError as exc:  # pragma: no cover - defensive
            log.error("Pandoc conversion failed for %s: %s", html_path, exc)
            raise

        chapter_markdown_path = markdown_output_dir / f"{chapter_index:04}.md"
        chapter_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        chapter_markdown_path.write_text(markdown, encoding="utf-8")

    css_summary: CssSummary = {
        "selectors": summary_selector_styles,
        "groups": collapse_style_groups(summary_style_groups),
    }
    return css_summary


def write_css_summary(path: Path, summary: Dict[int, CssSummary]) -> None:
    """Persist CSS summary data for later inspection and debugging.

    Args:
        path: Destination path for the JSON payload.
        summary: Mapping from book number to collected CSS summary details.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    """Run the EPUB processing pipeline across every detected book directory."""
    root = Path("static") / "epub"
    html_root = Path("static") / "html" / "books"
    markdown_root = Path("static") / "markdown" / "books"
    format_json_path = Path("static") / "json" / "format.json"
    css_summary_path = Path("static") / "json" / "css_styles.json"

    book_dirs = find_book_directories(root)
    if not book_dirs:
        log.warning("No book directories found under %s", root)
        return

    css_summary: Dict[int, CssSummary] = {}

    with Pandoc() as pandoc, _JsonArrayWriter(format_json_path) as format_writer:
        for index, book_dir in enumerate(book_dirs, start=1):
            book_number = extract_book_number(book_dir.name, default=index)
            log.info(
                "Processing book %s (parsed number %s)", book_dir.name, book_number
            )
            html_output_dir = html_root / str(book_number)
            markdown_output_dir = markdown_root / str(book_number)
            summary = process_book(
                book_dir,
                html_output_dir,
                markdown_output_dir,
                pandoc,
                format_writer.write_many,
            )
            css_summary[book_number] = summary

    write_css_summary(css_summary_path, css_summary)


if __name__ == "__main__":
    log.info("Starting EPUB style processing")
    main()
