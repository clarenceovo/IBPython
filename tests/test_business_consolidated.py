"""Consolidated business overview endpoint behavior.

Covers the happy path, per-section failure isolation, and route registration
under the real app factory (/api/v1 prefix included).
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.account import AccountPnLDTO, AccountSummaryDTO, LivePositionDTO, PortfolioItemDTO
from src.feeds.orders import OpenOrder
from src.webapp.app import create_app

API_PATH = "/api/v1/business/consolidated/overview"

SECTION_KEYS = {"account_summary", "account_pnl", "positions", "portfolio", "open_orders"}


class _FakeFeed:
    """Feed stub returning canned DTOs; can be told to fail one section."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_positions = False

    async def load_account_summary(self, account: str = "") -> list[AccountSummaryDTO]:
        self.calls.append("account_summary")
        return [AccountSummaryDTO(account=account or "DU123", values={})]

    async def load_account_pnl_snapshot(self, account: str, model_code: str = "", *, wait_seconds: float = 1.2) -> AccountPnLDTO:
        self.calls.append("account_pnl")
        return AccountPnLDTO(account=account or "DU123", daily_pnl=12.5, unrealized_pnl=-3.25, realized_pnl=100.0)

    async def load_live_positions(self) -> list[LivePositionDTO]:
        self.calls.append("positions")
        if self.fail_positions:
            raise RuntimeError("positions unavailable")
        return [LivePositionDTO(account="DU123", symbol="AAPL", sec_type="STK", position=10.0, average_cost=150.0)]

    async def load_portfolio_items(self, account: str = "") -> list[PortfolioItemDTO]:
        self.calls.append("portfolio")
        return [PortfolioItemDTO(account=account or "DU123", symbol="AAPL", sec_type="STK", position=10.0)]

    async def load_open_orders(self) -> list[OpenOrder]:
        self.calls.append("open_orders")
        return []


class _FakeRedis:
    async def get_raw(self, _key: str) -> bytes:
        return b"test-order-token"


class _FakeState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.feed = _FakeFeed()
        self.loader = None
        self.redis = _FakeRedis()  # type: ignore[assignment]
        self.market_data_cache = None
        self.event_contracts = None
        self.fixed_income_reference_provider = None

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass


def _make_client() -> tuple[TestClient, _FakeState]:
    state = _FakeState(Settings())
    app = create_app(settings=state.settings, state=state)  # type: ignore[arg-type]
    return TestClient(app), state


def test_overview_all_sections_ok() -> None:
    client, state = _make_client()
    with client:
        response = client.get(API_PATH, params={"account_id": "DU123"})
        assert response.status_code == 200, response.text
        payload = response.json()

    assert payload["requested_at"] is not None
    assert payload["account_summary"][0]["account"] == "DU123"
    assert payload["account_summary"][0]["values"] == {}
    assert payload["account_pnl"]["account"] == "DU123"
    assert payload["account_pnl"]["daily_pnl"] == 12.5
    assert payload["positions"][0]["symbol"] == "AAPL"
    assert payload["portfolio"][0]["symbol"] == "AAPL"
    assert payload["open_orders"] == []

    sections = payload["sections"]
    assert set(sections) == SECTION_KEYS
    assert all(section["status"] == "ok" for section in sections.values())
    assert all(section["error"] is None for section in sections.values())
    assert sorted(state.feed.calls) == sorted(SECTION_KEYS)


def test_overview_isolates_failing_section() -> None:
    client, state = _make_client()
    state.feed.fail_positions = True
    with client:
        response = client.get(API_PATH)
        assert response.status_code == 200, response.text
        payload = response.json()

    assert payload["positions"] is None
    assert payload["account_summary"] is not None
    assert payload["account_pnl"] is not None
    assert payload["portfolio"] is not None
    assert payload["open_orders"] == []

    sections = payload["sections"]
    assert sections["positions"]["status"] == "error"
    assert "positions unavailable" in sections["positions"]["error"]
    for key in SECTION_KEYS - {"positions"}:
        assert sections[key]["status"] == "ok", key
    assert sorted(state.feed.calls) == sorted(SECTION_KEYS)


def test_overview_route_registered_under_api_v1() -> None:
    client, _ = _make_client()
    with client:
        response = client.get(API_PATH)
        assert response.status_code == 200, response.text
        assert response.request.url.path == API_PATH
