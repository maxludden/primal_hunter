import atexit
from datetime import datetime
from pathlib import Path
from re import Match, Pattern, compile, match
from typing import Any, Dict, Optional, Tuple

import loguru
from rich import inspect
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.prompt import Confirm
from rich.style import Style
from rich.text import Text as RichText
from rich.traceback import install as tr_install
from rich_color_ext import install as rc_install
from rich_gradient import Gradient, Text, logger

tr_install()
rc_install()

__all__ = [
    "get_console",
    "get_progress",
    "get_logger",
]

_console: Console = Console()


def get_progress(console: Optional[Console] = _console) -> Progress:
    """Get a Progress instance with the provided console or the global console."""
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        SpinnerColumn("simpleDots"),
        BarColumn(bar_width=None),
        TimeElapsedColumn(),
        MofNCompleteColumn(),
        console=console,
        transient=True,
    )


def get_console(
    console: Optional[Console] = None, progress: Optional[Progress] = None
) -> Console:
    """Get the provided console or the global console."""
    if console:
        return console
    if progress:
        return progress.console
    return _console


LOGS_DIR = Path("logs")
LOG_FILEPATH = LOGS_DIR / "primal_hunter.log"


class RichSink:
    """
    A custom Loguru sink that uses Rich to print styled log messages.
    Args:
        console (Console): The Rich console to print to. Defaults to the global console.
        padding (Tuple[int, int]): Padding for the panel (top/bottom, left/right). Defaults to (1, 2).
        expand (bool): Whether the panel should expand to the console width. Defaults to False.
    """

    LEVEL_STYLES: Dict[str, Style] = {
        "TRACE": Style(italic=True),
        "DEBUG": Style(color="#aaaaaa"),
        "INFO": Style(color="#00afff"),
        "SUCCESS": Style(bold=True, color="#00ff00"),
        "WARNING": Style(italic=True, color="#ffaf00"),
        "ERROR": Style(bold=True, color="#ff5000"),
        "CRITICAL": Style(bold=True, color="#ff0000"),
    }

    # Styles for each log level
    GRADIENTS: Dict[str, list[str]] = {
        "TRACE": ["#888888", "#aaaaaa", "#cccccc"],
        "DEBUG": ["#0F8C8C", "#19cfcf", "#00ffff"],
        "INFO": ["#1b83d3", "#00afff", "#54d1ff"],
        "SUCCESS": ["#00ff90", "#00ff00", "#afff00"],
        "WARNING": ["#ffaa00", "#ffcc00", "#ffff00"],
        "ERROR": ["#ff7700", "#ff5500", "#ff3300"],
        "CRITICAL": ["#ff0000", "#ff005f", "#ff009f"],
    }

    # Gradients for log level titles
    MSG_COLORS: Dict[str, list[str]] = {
        "TRACE": ["#eeeeee", "#dddddd", "#bbbbbb"],
        "INFO": ["#a4e7ff", "#72d3ff", "#52daff"],
        "SUCCESS": ["#d3ffd3", "#a9ffa9", "#64ff64"],
        "WARNING": ["#ffeb9b", "#ffe26e", "#ffc041"],
        "ERROR": ["#ffc59c", "#ffaa6e", "#FF4E3A"],
        "CRITICAL": ["#ffaaaa", "#FF6FA4", "#FF49C2"],
    }

    def __init__(
        self,
        console: Optional[Console] = None,
        padding: Tuple[int, int] = (1, 2),
        expand: bool = True,
    ) -> None:
        self.console = get_console(console)
        self.padding = padding
        self.expand = expand

    def __call__(self, message: Any) -> None:
        """
        Print a loguru.Message to the Rich console as a styled panel.
        Args:
            message (Message): The loguru message to print.
        """
        record = message.record
        panel = self._build_panel(record)
        self.console.print(panel)

    def _build_panel(self, record: Any) -> Panel:
        """Helper method to build a Rich Panel for a log record.
        Args:
            record (Record): The log record.
            run (int, optional): The current run number. Defaults to None.
        Returns:
            Panel: A Rich Panel containing the formatted log message.
        """
        level_name = record["level"].name
        colors = self.GRADIENTS.get(level_name, [])
        style = self.LEVEL_STYLES.get(level_name, Style())
        msg_style = self.MSG_COLORS.get(
            level_name,
            ["#eeeeee", "#aaaaaa", "#888888"],
        )
        # Title with gradient and highlighted separators.
        title: Text = Text(
            f" {level_name} | {record['file'].name} | Line {record['line']} ",
            colors=colors,
        )
        # title.stylize(Style(reverse=True))

        now_iso = datetime.now().isoformat(timespec="milliseconds").replace("T", " | ")
        subtitle_text: str = f"{now_iso}"
        subtitle: RichText = Text(
            subtitle_text,
            colors=list(reversed(msg_style)),
        ).as_rich()
        subtitle.highlight_words([":", ".", "-"], style="dim #aaaaaa")

        # Message text with gradient.
        msg: str = record["message"]
        message_text: Text = Text(msg, colors=msg_style)
        return Panel(
            message_text,
            title=title,
            title_align="left",
            subtitle=subtitle,
            subtitle_align="right",
            border_style=style + Style(bold=True),
            padding=self.padding,
            expand=self.expand,
        )


def _validate_level(level: str | int) -> int:
    """
    Validate the log level and convert it to an integer.
    Args:
        level (str|int): The logging level. Can be a string (e.g., "DEBUG", "INFO", etc.) or an integer (0-50).
    Returns:
        Optional[int]: The validated log level as an integer, or None if invalid.
    Raises:
        TypeError: If the log level is not a string or an integer.
        ValueError: If the log level is not valid.
    """

    _LEVEL_NAMES = [
        "TRACE",
        "DEBUG",
        "INFO",
        "SUCCESS",
        "WARNING",
        "ERROR",
        "CRITICAL",
    ]
    if isinstance(level, int):
        if not (0 <= level <= 50):
            raise ValueError(
                f"Log level integer must be between 0 and 50, got {level}."
            )
        return level
    if not isinstance(level, str):
        raise TypeError(f"Log level must be a string or an integer, got {type(level)}.")
    _level = level.upper()
    if _level not in _LEVEL_NAMES:
        raise ValueError(
            f"Invalid log level: {level!r}. Must be one of: {', '.join(_LEVEL_NAMES)}."
        )
    match _level:
        case "TRACE":
            return 5
        case "DEBUG":
            return 10
        case "INFO":
            return 20
        case "SUCCESS":
            return 25
        case "WARNING":
            return 30
        case "ERROR":
            return 40
        case "CRITICAL":
            return 50
        case _:
            raise ValueError(
                f"Unable to parse log level: {level!r}. Must be one of: {', '.join(_LEVEL_NAMES)}."
            )


def get_logger(
    level: int | str = "SUCCESS",
    console: Optional[Console] = None,
    padding: Tuple[int, int] = (1, 2),
    expand: bool = True,
):
    """Get a configured Loguru logger with RichSink."""
    _validate_level(level)
    if not LOGS_DIR.exists():
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
    resolved_console = get_console(console)
    rich_sink = RichSink(console=resolved_console, padding=padding, expand=expand)
    loguru.logger.remove()
    loguru.logger.configure(
        handlers=[
            {
                "sink": "logs/primal_hunter.log",
                "format": "{time:hh:mm:ss.SSS} | {file.name: ^12} | Line {line} | {level} ➤ {message}",
                "level": "TRACE",
                "backtrace": True,
                "diagnose": True,
                "catch": True,
                "mode": "w",  # Use write mode instead of append mode
                "retention": "30 minutes",
            },
            {
                "sink": rich_sink,
                "level": level,
                "format": "{message}",
                "backtrace": True,
                "diagnose": False,
                "catch": True,
                "colorize": False,
            },
        ]
    )

    return loguru.logger


if __name__ == "__main__":
    console = get_console()
    log = get_logger("TRACE", console=console, padding=(0, 2))
    log.trace("This is a trace message.")
    log.debug("This is a debug message.")
    log.info("This is an info message.")
    log.success("This is a success message.")
    log.warning("This is a warning message.")
    log.error("This is an error message.")
    log.critical("This is a critical message.")
    console.line(2)
    # inspect(log, all=True)  # type: ignore[call-arg]
