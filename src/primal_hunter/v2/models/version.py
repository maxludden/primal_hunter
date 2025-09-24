"""Beanie document for scraped Primal Hunter chapters (v2).

This module defines the ``Version`` document that ``primal_hunter.v2.scrape_chapter``
relies on when persisting chapter data.  The previous revision of this file
contained a duplicated copy of the scraping script which not only created
circular imports but also failed to expose the expected document class.  The
implementation below restores a lean, well-validated Beanie model suitable for
inserting chapter records into MongoDB.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional, Mapping

from beanie import Document
from pendulum import DateTime as PendulumDateTime
from pendulum import parse as pendulum_parse
from pydantic import AnyHttpUrl, Field, ConfigDict, field_validator


def _utc_now() -> datetime:
    """Return the current UTC timestamp as a `datetime` instance."""
    return datetime.now(timezone.utc)


class Version(Document):
    """Persisted representation of a single scraped chapter.

    The document is intentionally lightweight: only the attributes required by
    the scraping pipeline are stored.  Additional housekeeping timestamps are
    included to make debugging data freshness easier when inspecting the
    MongoDB collection directly.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    chapter: int = Field(
        ...,
        ge=1,
        le=10_000,
        description="Sequential chapter number pulled from the TOC",
    )
    title: str = Field(..., min_length=1, description="Human friendly chapter title")
    url: AnyHttpUrl = Field(..., description="Canonical RoyalRoad URL for the chapter")
    content: str = Field("", description="Plain-text chapter body extracted from HTML")
    content_html: str = Field(
        "",
        alias="contentHtml",
        description="HTML chapter body preserving formatting and justification",
    )
    content_markdown: str = Field(
        "",
        description="Markdown chapter body (not yet implemented)",
    )
    published: Optional[datetime] = Field(
        default=None,
        description="Publication timestamp provided by RoyalRoad (UTC)",
    )
    created_at: datetime = Field(default_factory=_utc_now, description="Insertion timestamp (UTC)")
    updated_at: datetime = Field(
        default_factory=_utc_now,
        description="Last modification timestamp (UTC)"
    )

    class Settings:
        """Configuration for the Version document collection.

        Specifies the MongoDB collection name and indexes for the Version model.
        """
        name = "versions"
        indexes = ["chapter", "url"]

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("title")
    @classmethod
    def _normalize_title(cls, value: str) -> str:
        if not (title := value.strip()):
            raise ValueError("title may not be blank")
        return title

    @field_validator("content")
    @classmethod
    def _normalize_content(cls, value: str) -> str:
        return value.strip()

    @field_validator("content_html")
    @classmethod
    def _normalize_content_html(cls, value: str) -> str:
        return value.strip()

    @field_validator("published", mode="before")
    @classmethod
    def _parse_published(cls, value: Any) -> Optional[datetime]:
        """Accept ISO strings, Pendulum objects, or datetimes for `published`."""
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        if isinstance(value, PendulumDateTime):
            return value.in_timezone("UTC").naive().replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                parsed = pendulum_parse(value, strict=False)
            except Exception as exc:  # pragma: no cover - defensive
                raise ValueError(f"Unable to parse published timestamp: {value!r}") from exc
            # pendulum_parse can return DateTime, Date, Time, Duration, etc.
            if isinstance(parsed, PendulumDateTime):
                pendulum_dt = parsed.in_timezone("UTC")
                return pendulum_dt.naive().replace(tzinfo=timezone.utc)
            if isinstance(parsed, datetime):
                return parsed.astimezone(timezone.utc)
            # Fallback: try to interpret the stringified parsed value with stdlib
            try:
                dt = datetime.fromisoformat(str(parsed))
            except Exception as exc:  # pragma: no cover - defensive
                raise ValueError(f"Unable to interpret parsed published timestamp: {parsed!r}") from exc
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        msg = f"Unsupported type for published: {type(value)!r}"
        raise TypeError(msg)

    async def save(self, *args: Any, **kwargs: Any) -> Version:
        """Persist the document while refreshing `updated_at`."""

        self.updated_at = _utc_now()
        return await super().save(*args, **kwargs)

    async def replace(self, *args: Any, **kwargs: Any) -> Version:
        """Replace the document while refreshing ``updated_at``."""

        self.updated_at = _utc_now()
        return await super().replace(*args, **kwargs)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------
    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        content: str,
        content_html: Optional[str] = None,
        content_markdown: Optional[str] = None,
    ) -> "Version":
        """Construct a `Version` from a TOC payload and scraped `content`.

        The helper mirrors the shape produced by `get_toc.py` and keeps
        callers free from having to remember the extra bookkeeping fields.

        This implementation validates that required keys are present and normalizes
        the chapter value to an integer to avoid KeyError/Type issues when callers
        pass a TypedDict with optional keys.
        """

        chapter = payload.get("chapter")
        title = payload.get("title")
        url = payload.get("url")

        if chapter is None:
            raise ValueError("payload missing required key 'chapter'")
        if title is None:
            raise ValueError("payload missing required key 'title'")
        if url is None:
            raise ValueError("payload missing required key 'url'")

        try:
            chapter_int = int(chapter)
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"Invalid chapter value: {chapter!r}") from exc

        data = {
            "chapter": chapter_int,
            "title": str(title),
            "url": str(url),
            "content": content,
            "content_html": content_html or "",
            "content_markdown": content_markdown or "",
            "published": payload.get("published"),
        }
        return cls(**data)


__all__ = ["Version"]
