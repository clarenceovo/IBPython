"""Consolidated market-data endpoints: one call, many sections, isolated failures."""

from fastapi import APIRouter

router = APIRouter(prefix="/market-data/consolidated", tags=["market-data"])
