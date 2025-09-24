# primal_hunter/v2/models/chapter_link.py
from __future__ import annotations

import re
from typing import Any, Optional
from pydantic import Field, ConfigDict, AnyUrl, BaseModel, field_validator
from beanie import Document
from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString, PageElement
from pendulum import DateTime, now, parse as pendulum_parse

class ChapterRow(Document):
    """A class for a chapter row in the table of contents.
    Args:
        chapter (int): Chapter number (1 to 3462)
        title (str): Title of the chapter
        url (AnyUrl): Absolute URL of the chapter
        created (DateTime): Timestamp when the record was created
        modified (DateTime): Timestamp when the record was last modified
    """
    chapter: int = Field(
        ...,
        ge=1,
        le=3462,
        description="Chapter number"
    )
    title: str
    url: AnyUrl = Field(
        ...,
        description="Absolute URL of the chapter"
    )
    published: DateTime = Field(
        ...,
        description="Timestamp when the record was published"
    )

    @field_validator("published", mode="before")
    def parse_datetime(cls, value: Optional[Any]) -> DateTime:
        """Validator to parse datetime fields from strings or set to now if None."""
        if value is None:
            return now()
        if isinstance(value, str):
            pendulum_date: DateTime = pendulum_parse(value)  # type: ignore
            return pendulum_date
        raise TypeError(f"Cannot convert {value!r} to pendulum.DateTime")

    class Settings:
        """Beanie settings for the document."""
        name = "chapter_rows"
        indexes = [
            [("chapter", 1)],
            [("url", 1)],
        ]
        bson_encoders = {
            DateTime: lambda dt: dt.to_iso8601_string()
        }

    @classmethod
    def from_html_row(cls, row: Tag) -> ChapterRow:
        """Create a ChapterRow instance from an HTML table row.
        Args:
            row (Tag): A BeautifulSoup Tag representing the table row.
        Returns:
            ChapterRow: An instance of ChapterRow populated with data from the row.
        Raises:
            ValueError: If the row does not contain the expected number of columns or data.
        """
        cells = row.find_all("td")
        assert len(cells) == 2, f"Expected 2 columns in row, got {len(cells)}"
        chapter_cell, published_cell = cells
        chapter_data = {}

        chapter_cell_text = chapter_cell.text.strip()
        chapter_match = re.match(r"Chapter\s+(?<chapter>\d+) - (?<title>.+)", chapter_cell_text)
        if chapter_match:
            chapter: int = int(chapter_match.group("chapter"))
            title: str = chapter_match.group("title")
            url: str = cells[0].find("a")["href"]
        else:
            if chapter >= 986:

                return cls(
                    chapter=chapter,
                    title=title,
                    url=title_cell.find("a")["href"],
        else:
            raise ValueError(f"Unable to parse chapter number from cell: {chapter_cell_text!r}")




        title = title_cell.text.strip()
        return cls(chapter=chapter, title=title)
