from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


_MARKER_RE = re.compile(r"^<!--\s*openapi-example:\s+([A-Za-z0-9_.-]+)\s+([A-Za-z0-9_.-]+)\s*-->$")


def markdown_openapi_examples(group: str) -> dict[str, dict[str, Any]]:
    """Load OpenAPI examples from the business API Markdown guide."""

    return _load_examples().get(group, {})


@lru_cache(maxsize=1)
def _load_examples() -> dict[str, dict[str, dict[str, Any]]]:
    examples_path = Path(__file__).with_name("docs") / "business_api_examples.md"
    if not examples_path.exists():
        return {}
    return _parse_examples(examples_path.read_text(encoding="utf-8"))


def _parse_examples(markdown: str) -> dict[str, dict[str, dict[str, Any]]]:
    lines = markdown.splitlines()
    groups: dict[str, dict[str, dict[str, Any]]] = {}
    index = 0
    while index < len(lines):
        marker = _MARKER_RE.match(lines[index].strip())
        if marker is None:
            index += 1
            continue
        group, key = marker.groups()
        example, index = _parse_one_example(lines, index + 1)
        groups.setdefault(group, {})[key] = example
    return groups


def _parse_one_example(lines: list[str], index: int) -> tuple[dict[str, Any], int]:
    summary = ""
    description_lines: list[str] = []
    while index < len(lines):
        stripped = lines[index].strip()
        if stripped.startswith("```json"):
            break
        if stripped.startswith("###") and not summary:
            summary = stripped.lstrip("#").strip()
        elif stripped:
            description_lines.append(stripped)
        index += 1

    if index >= len(lines) or not lines[index].strip().startswith("```json"):
        raise ValueError("OpenAPI example marker must be followed by a JSON fenced block")
    index += 1
    json_lines: list[str] = []
    while index < len(lines) and not lines[index].strip().startswith("```"):
        json_lines.append(lines[index])
        index += 1
    if index < len(lines):
        index += 1

    example: dict[str, Any] = {"value": json.loads("\n".join(json_lines))}
    if summary:
        example["summary"] = summary
    if description_lines:
        example["description"] = " ".join(description_lines)
    return example, index
