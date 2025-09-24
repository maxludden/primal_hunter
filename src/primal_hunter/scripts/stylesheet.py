"""Monkeypatch cssutils stylesheet classes to provide a Rich renderer.

This module attaches a __rich_console__ implementation to
cssutils.css.cssstylesheet.CSSStyleSheet (and a small helper for rules)
so that `rich.inspect(sheet)` and `console.print(sheet)` produce
readable CSS output when debugging.

Importing this module applies the patch automatically.  The implementation
is defensive: if the target types aren't available it silently no-ops.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

import cssutils.css.cssstylesheet as cssstylesheet
from rich.console import Console, ConsoleOptions, RenderResult
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text as RichText
from rich_color_ext import install as rc_install
from rich_gradient import Gradient

from primal_hunter import get_console, get_logger, get_progress

rc_install()
_console: Console = get_console()
progress = get_progress(_console)
log = get_logger(console=_console)

_PATCHED_ATTR = "__rich_console_patched__"
__all__ = ["install_css_rich_console"]


def _get_css_text_from_sheet(sheet: Any) -> str:
    """Return a CSS string representation for a cssutils stylesheet.

    cssutils rule and sheet objects sometimes expose bytes-like .cssText;
    normalize to str for rendering in Rich.
    """
    log.trace(f"Entered _get_css_text_from_sheet with sheet: {sheet}")

    # Try to iterate cssRules first (common API)
    parts: list[str] = []
    rules = getattr(sheet, "cssRules", None)
    if rules is not None:
        try:
            for r in rules:
                text = getattr(r, "cssRules", None)
                if isinstance(text, bytes):
                    try:
                        text = cast(bytes, text).decode("utf-8")
                    except Exception:
                        text = cast(bytes, text).decode("utf-8", errors="ignore")
                if text is None:
                    log.warning(
                        f"Rule has no cssText: falling back to stringification. Rule: {r}"
                    )
                    # Fallback to stringification
                    parts.append(str(r))
                else:
                    parts.append(str(text))
        except Exception:
            # Last-resort: stringify the sheet object
            return str(sheet)
        return "\n".join(parts)

    # Fallbacks
    text = getattr(sheet, "cssText", None)
    if isinstance(text, bytes):
        try:
            return text.decode("utf-8")
        except Exception:
            return text.decode(errors="ignore")
    if text is None:
        return str(sheet)
    return str(text)


def file_uri_to_path(uri: str) -> Path:
    """Convert a file:// URI to a pathlib.Path, handling percent-encoding.
    Args:
        uri (str): The file:// URI to convert.
    Returns:
        Path: The corresponding pathlib.Path object.
    Raises:
        ValueError: If the URI scheme is not 'file'.
    """
    log.trace(f"Entered file_uri_to_path with uri: {uri}")

    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError("not a file URI")
    log.debug(f"Converting file URI to path: {uri}")

    # Percent-decode
    path = unquote(parsed.path)
    log.debug(f"Percent-decoded path: {path}")

    # Convert percent/URL path to platform path (handles Windows drive/UNC)
    path = url2pathname(path)
    log.debug(f"Converted path: {path}")
    return Path(path)


def _rich_console_for_sheet(
    self: Any,
    console: Console,  # pylint: disable=unused-argument
    options: ConsoleOptions,  # pylint: disable=unused-argument
) -> RenderResult:
    """Rich console protocol implementation for a CSSStyleSheet instance.

    Yields a single Syntax panel containing the concatenated rules. Keep the
    output compact to avoid overwhelming the console during debugging.
    """
    css_text = _get_css_text_from_sheet(self)
    # Limit size so extremely large stylesheets don't blow up the console
    max_chars = 16_000
    if len(css_text) > max_chars:
        css_snippet = css_text[:max_chars] + "\n/*...truncated...*/"
    else:
        css_snippet = css_text

    # Use Syntax highlighting for CSS when available
    if Syntax is not None and Panel is not None:
        syntax = Syntax(
            css_snippet,
            "css",
            line_numbers=False,
            word_wrap=False,
            background_color="#111111",
        )
        filepath = file_uri_to_path(getattr(self, "href", ""))
        assert filepath.exists(), "Expected href to be a valid file URI"
        filename: str = filepath.name
        yield Panel(
            cast(
                Any, Gradient(syntax, colors=["#f00", "#f50", "#f90", "#fc0", "#ff0"])
            ),
            title=f"CSSStyleSheet {filename}",
            subtitle=RichText(str(filepath.resolve()), style="dim italic"),
            subtitle_align="right",
        )
    else:
        # Fall back to a plain string panel
        if Panel is not None:
            yield Panel(
                str(css_snippet), title=f"CSSStyleSheet ({getattr(self, 'href', '')})"
            )
        else:
            # If Rich isn't available, just print to the logger
            log.debug("cssutils stylesheet (truncated): %s", css_snippet[:1000])


def install_css_rich_console() -> None:
    """Attach __rich_console__ to cssutils cssstylesheet.CSSStyleSheet (if present).

    This function is idempotent; calling multiple times is harmless.
    """
    log.trace("Entered install_rich_console()")
    if cssstylesheet is None:
        log.debug("cssstylesheet module not found; skipping rich console install")
        return

    CSSStyleSheet = getattr(cssstylesheet, "CSSStyleSheet", None)
    if CSSStyleSheet is None:
        log.debug("CSSStyleSheet class not found in cssutils; skipping patch")
        return

    # Don't patch twice
    if getattr(CSSStyleSheet, _PATCHED_ATTR, False):
        return

    try:
        setattr(CSSStyleSheet, "__rich_console__", _rich_console_for_sheet)
        setattr(CSSStyleSheet, _PATCHED_ATTR, True)
        log.debug("Patched CSSStyleSheet with __rich_console__")
    except Exception as exc:  # pragma: no cover - defensive
        log.warning(f"Failed to patch CSSStyleSheet for rich rendering: {exc}")
