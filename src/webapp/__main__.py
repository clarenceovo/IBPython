"""Run the webapp with ``python -m src.webapp``."""

from __future__ import annotations

import uvicorn


def main() -> None:
    uvicorn.run(
        "src.webapp.app:get_app",
        host="0.0.0.0",
        port=8000,
        factory=True,
        loop="asyncio",
        reload=False,
    )


if __name__ == "__main__":
    main()
