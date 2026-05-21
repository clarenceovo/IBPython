"""Market data router — stub after decomposition.

The original monolithic market_data.py has been decomposed into:
- market_data_equity.py — equity/index OHLCV endpoints + models
- market_data_futures.py — futures + commodity endpoints + models
- market_data_fx.py — FX endpoints + models
- market_data_options.py — options analytics/skew endpoints + models
- market_data_bonds.py — bond yield endpoints + models
- market_data_shared.py — shared models and helpers

All routers are registered in app.py directly.
"""

from __future__ import annotations

# This module is intentionally left as a documentation stub.
# All endpoints have been moved to domain-specific router modules.
# app.py includes each sub-router directly.
