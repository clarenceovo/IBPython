"""MCP toolset registry, named presets, and registration helpers."""

from __future__ import annotations

from collections.abc import Callable, Iterable

from mcp.server.fastmcp import FastMCP

from src.mcp_app.tools import (
    account,
    core,
    discovery,
    market_data,
    options,
    reference,
    storage,
    trading,
)

RegisterFn = Callable[[FastMCP], None]

TOOLSET_REGISTRY: dict[str, RegisterFn] = {
    "core": core.register,
    "storage": storage.register,
    "market_data": market_data.register,
    "discovery": discovery.register,
    "options": options.register,
    "reference": reference.register,
    "account": account.register,
    "trading": trading.register,
}

TOOLSET_NAMES: tuple[str, ...] = tuple(TOOLSET_REGISTRY.keys())

TOOLSET_PRESETS: dict[str, tuple[str, ...]] = {
    "full": TOOLSET_NAMES,
    "readonly_all": ("core", "storage", "market_data", "discovery", "options", "reference", "account"),
    "research": ("core", "storage", "market_data", "reference"),
    "options_desk": ("core", "market_data", "options", "account"),
}


def resolve_toolsets(spec: str | Iterable[str] | None) -> tuple[str, ...]:
    """Resolve a preset name, comma-separated string, or iterable into canonical toolset names.

    ``core`` is always included so agents retain symbol resolution and health diagnostics.
    """
    if spec is None:
        raw_tokens: list[str] = ["full"]
    elif isinstance(spec, str):
        raw_tokens = [tok.strip().lower() for tok in spec.split(",") if tok.strip()]
        if not raw_tokens:
            raw_tokens = ["full"]
    else:
        raw_tokens = [str(tok).strip().lower() for tok in spec if str(tok).strip()]
        if not raw_tokens:
            raw_tokens = ["full"]

    ordered: list[str] = ["core"]
    seen: set[str] = {"core"}

    def _add(name: str) -> None:
        if name not in seen:
            seen.add(name)
            ordered.append(name)

    for token in raw_tokens:
        if token in TOOLSET_PRESETS:
            for member in TOOLSET_PRESETS[token]:
                _add(member)
        elif token in TOOLSET_REGISTRY:
            _add(token)
        else:
            valid = ", ".join((*TOOLSET_PRESETS.keys(), *TOOLSET_NAMES))
            raise ValueError(f"Unknown MCP toolset or preset '{token}'. Valid values: {valid}")

    return tuple(ordered)


def register_toolsets(mcp: FastMCP, toolsets: Iterable[str]) -> tuple[str, ...]:
    """Register every toolset in *toolsets* on *mcp* and return the registered names."""
    resolved = resolve_toolsets(toolsets)
    for name in resolved:
        TOOLSET_REGISTRY[name](mcp)
    return resolved
