from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from src.feeds.news import (
    HistoricalNewsRequest,
    NewsArticleRequest,
    NewsTick,
    format_historical_news_datetime,
    normalize_historical_news,
    normalize_news_article,
    normalize_news_providers,
)


def test_historical_news_request_formats_provider_codes() -> None:
    request = HistoricalNewsRequest(con_id=8314, provider_codes=["bz", "fly"], total_results=25)

    assert request.provider_codes == ("BZ", "FLY")
    assert request.provider_codes_param == "BZ+FLY"


def test_historical_news_request_rejects_invalid_window() -> None:
    with pytest.raises(ValidationError):
        HistoricalNewsRequest(
            con_id=8314,
            provider_codes=["BZ"],
            start_datetime="2026-01-02T00:00:00Z",
            end_datetime="2026-01-01T00:00:00Z",
        )


def test_format_historical_news_datetime_uses_utc_wire_format() -> None:
    value = datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc)

    assert format_historical_news_datetime(value) == "2026-01-01 12:30:00.0"


def test_normalize_news_providers() -> None:
    providers = normalize_news_providers(
        [SimpleNamespace(providerCode="bz", providerName="Benzinga Pro")]
    )

    assert providers[0].provider_code == "BZ"
    assert providers[0].provider_name == "Benzinga Pro"


def test_normalize_historical_news() -> None:
    headlines = normalize_historical_news(
        [
            SimpleNamespace(
                time="2026-01-01 12:30:00.0",
                providerCode="fly",
                articleId="FLY$1",
                headline="Test headline",
            )
        ]
    )

    assert headlines[0].timestamp == datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc)
    assert headlines[0].provider_code == "FLY"
    assert headlines[0].article_id == "FLY$1"


def test_normalize_news_article() -> None:
    request = NewsArticleRequest(provider_code="bz", article_id="BZ$1")
    article = normalize_news_article(
        SimpleNamespace(articleType=0, articleText="Body"),
        request,
    )

    assert article.provider_code == "BZ"
    assert article.article_id == "BZ$1"
    assert article.article_text == "Body"


def test_news_tick_converts_epoch_milliseconds() -> None:
    tick = NewsTick(
        ticker_id=1,
        timestamp=1_767_221_400_000,
        provider_code="brfg",
        article_id="BRFG$1",
        headline="Headline",
    )

    assert tick.timestamp == datetime(2025, 12, 31, 22, 50, tzinfo=timezone.utc)
    assert tick.provider_code == "BRFG"
