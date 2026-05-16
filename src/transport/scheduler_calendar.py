from __future__ import annotations

from datetime import datetime, timedelta

_ONE_MINUTE = timedelta(minutes=1)


def next_cron_run(expression: str, after: datetime) -> datetime:
    cron = _parse_cron_expression(expression)
    candidate = (after + _ONE_MINUTE).replace(second=0, microsecond=0)
    for _ in range(366 * 24 * 60):
        if _cron_matches(cron, candidate):
            return candidate
        candidate += _ONE_MINUTE
    raise ValueError(f"could not find next cron run within one year for {expression!r}")


def _parse_cron_expression(expression: str) -> tuple[set[int], set[int], set[int], set[int], set[int], bool, bool]:
    fields = expression.strip().split()
    if len(fields) != 5:
        raise ValueError("cron must contain five fields: minute hour day_of_month month day_of_week")
    minutes = _parse_cron_field(fields[0], minimum=0, maximum=59)
    hours = _parse_cron_field(fields[1], minimum=0, maximum=23)
    days = _parse_cron_field(fields[2], minimum=1, maximum=31)
    months = _parse_cron_field(fields[3], minimum=1, maximum=12)
    weekdays = _parse_cron_weekday_field(fields[4])
    return minutes, hours, days, months, weekdays, fields[2].strip() == "*", fields[4].strip() == "*"


def _parse_cron_weekday_field(field: str) -> set[int]:
    normalized = field.lower()
    for name, number in {
        "sun": "0",
        "mon": "1",
        "tue": "2",
        "wed": "3",
        "thu": "4",
        "fri": "5",
        "sat": "6",
    }.items():
        normalized = normalized.replace(name, number)
    values = _parse_cron_field(normalized, minimum=0, maximum=7)
    return {0 if value == 7 else value for value in values}


def _parse_cron_field(field: str, *, minimum: int, maximum: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        token = part.strip()
        if not token:
            raise ValueError("empty cron field token")
        step = 1
        if "/" in token:
            token, step_text = token.split("/", 1)
            step = int(step_text)
            if step <= 0:
                raise ValueError("cron step must be positive")
        if token == "*":
            start, end = minimum, maximum
        elif "-" in token:
            start_text, end_text = token.split("-", 1)
            start, end = int(start_text), int(end_text)
        else:
            start = end = int(token)
        if start < minimum or end > maximum or start > end:
            raise ValueError(f"cron value out of range: {part!r}")
        values.update(range(start, end + 1, step))
    return values


def _cron_matches(cron: tuple[set[int], set[int], set[int], set[int], set[int], bool, bool], candidate: datetime) -> bool:
    minutes, hours, days, months, weekdays, day_wildcard, weekday_wildcard = cron
    if candidate.minute not in minutes or candidate.hour not in hours or candidate.month not in months:
        return False
    day_matches = candidate.day in days
    cron_weekday = (candidate.weekday() + 1) % 7
    weekday_matches = cron_weekday in weekdays
    if day_wildcard and weekday_wildcard:
        return True
    if day_wildcard:
        return weekday_matches
    if weekday_wildcard:
        return day_matches
    return day_matches or weekday_matches
