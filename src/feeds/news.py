from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class NewsProvider(BaseModel):
    """IBKR API news provider entitlement."""

    model_config = ConfigDict(extra="forbid")

    provider_code: str = Field(min_length=1)
    provider_name: str = Field(default="")

    @field_validator("provider_code", mode="before")
    @classmethod
    def normalize_provider_code(cls, value: Any) -> str:
        if value is None:
            raise ValueError("provider_code is required")
        return str(value).strip().upper()


class HistoricalNewsRequest(BaseModel):
    """IBKR historical news headline request."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    con_id: int = Field(gt=0)
    provider_codes: tuple[str, ...] = Field(min_length=1)
    start_datetime: datetime | None = None
    end_datetime: datetime | None = None
    total_results: int = Field(default=10, ge=1, le=300)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider_codes", mode="before")
    @classmethod
    def normalize_provider_codes(cls, value: Any) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("provider_codes must be a sequence of provider codes")
        normalized = tuple(str(item).strip().upper() for item in value if str(item).strip())
        if not normalized:
            raise ValueError("provider_codes must contain at least one provider")
        return normalized

    @field_validator("start_datetime", "end_datetime", mode="before")
    @classmethod
    def normalize_datetime(cls, value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        return normalize_news_timestamp(value)

    @model_validator(mode="after")
    def validate_time_window(self) -> Self:
        if self.start_datetime and self.end_datetime and self.start_datetime >= self.end_datetime:
            raise ValueError("start_datetime must be before end_datetime")
        return self

    @property
    def provider_codes_param(self) -> str:
        return ",".join(self.provider_codes)


class HistoricalNewsHeadline(BaseModel):
    """IBKR historical news headline response."""

    model_config = ConfigDict(extra="forbid")

    timestamp: datetime
    provider_code: str = Field(min_length=1)
    article_id: str = Field(min_length=1)
    headline: str
    source: str = Field(default="ibkr_news", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value: Any) -> datetime:
        return normalize_news_timestamp(value)

    @field_validator("provider_code", mode="before")
    @classmethod
    def normalize_provider_code(cls, value: Any) -> str:
        if value is None:
            raise ValueError("provider_code is required")
        return str(value).strip().upper()


class NewsArticleRequest(BaseModel):
    """IBKR news article body request."""

    model_config = ConfigDict(extra="forbid")

    provider_code: str = Field(min_length=1)
    article_id: str = Field(min_length=1)

    @field_validator("provider_code", mode="before")
    @classmethod
    def normalize_provider_code(cls, value: Any) -> str:
        if value is None:
            raise ValueError("provider_code is required")
        return str(value).strip().upper()


class NewsArticle(BaseModel):
    """IBKR news article response."""

    model_config = ConfigDict(extra="forbid")

    provider_code: str = Field(min_length=1)
    article_id: str = Field(min_length=1)
    article_type: int
    article_text: str
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = Field(default="ibkr_news", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider_code", mode="before")
    @classmethod
    def normalize_provider_code(cls, value: Any) -> str:
        if value is None:
            raise ValueError("provider_code is required")
        return str(value).strip().upper()

    @field_validator("received_at", mode="before")
    @classmethod
    def normalize_received_at(cls, value: Any) -> datetime:
        return normalize_news_timestamp(value)


class NewsTick(BaseModel):
    """Real-time news headline from EWrapper.tickNews / generic tick 292."""

    model_config = ConfigDict(extra="forbid")

    ticker_id: int
    timestamp: datetime
    provider_code: str = Field(min_length=1)
    article_id: str = Field(min_length=1)
    headline: str
    extra_data: str = ""
    source: str = Field(default="ibkr_news", min_length=1)

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value: Any) -> datetime:
        return normalize_news_timestamp(value)

    @field_validator("provider_code", mode="before")
    @classmethod
    def normalize_provider_code(cls, value: Any) -> str:
        if value is None:
            raise ValueError("provider_code is required")
        return str(value).strip().upper()


class NewsBulletin(BaseModel):
    """IBKR system/news bulletin response."""

    model_config = ConfigDict(extra="forbid")

    msg_id: int
    msg_type: int
    message: str
    origin_exchange: str = ""
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = Field(default="ibkr_bulletin", min_length=1)

    @field_validator("received_at", mode="before")
    @classmethod
    def normalize_received_at(cls, value: Any) -> datetime:
        return normalize_news_timestamp(value)


def normalize_news_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000
        parsed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        if not text:
            raise ValueError("timestamp cannot be empty")
        for fmt in ("%Y-%m-%d %H:%M:%S.0", "%Y-%m-%d %H:%M:%S", "%Y%m%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            parsed = datetime.fromisoformat(text)
    else:
        raise TypeError("unsupported news timestamp type")

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_historical_news_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    return normalize_news_timestamp(value).strftime("%Y-%m-%d %H:%M:%S.0")


def normalize_news_providers(providers: list[Any]) -> list[NewsProvider]:
    normalized: list[NewsProvider] = []
    for provider in providers:
        if isinstance(provider, NewsProvider):
            normalized.append(provider)
            continue
        provider_code = _news_provider_value(provider, "providerCode", "provider_code", "code")
        if provider_code is None:
            continue
        normalized.append(
            NewsProvider(
                provider_code=provider_code,
                provider_name=_news_provider_value(provider, "providerName", "provider_name", "name") or "",
            )
        )
    return normalized


def _news_provider_value(provider: Any, *names: str) -> Any:
    if isinstance(provider, dict):
        for name in names:
            if name in provider:
                return provider[name]
        return None
    for name in names:
        if hasattr(provider, name):
            return getattr(provider, name)
    if isinstance(provider, (tuple, list)) and provider:
        if any(name in {"providerCode", "provider_code", "code"} for name in names):
            return provider[0]
        if len(provider) > 1:
            return provider[1]
    return None


def normalize_historical_news(items: list[Any]) -> list[HistoricalNewsHeadline]:
    return [
        HistoricalNewsHeadline(
            timestamp=getattr(item, "time"),
            provider_code=getattr(item, "providerCode"),
            article_id=getattr(item, "articleId"),
            headline=getattr(item, "headline"),
        )
        for item in items
    ]


def normalize_news_article(raw_article: Any, request: NewsArticleRequest) -> NewsArticle:
    return NewsArticle(
        provider_code=request.provider_code,
        article_id=request.article_id,
        article_type=int(getattr(raw_article, "articleType")),
        article_text=str(getattr(raw_article, "articleText")),
    )
