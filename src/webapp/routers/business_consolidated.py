"""Consolidated business-data endpoints: one call, many sections, isolated failures."""

from fastapi import APIRouter

router = APIRouter(prefix="/business/consolidated", tags=["business"])
