"""Request validation. Everything here raises BadRequest with a message fit to send back."""
import datetime as dt
import math
import re

from mdp.serving.models import FIRST_DAY, LAST_DAY, MAX_PRICE, MAX_VOLUME, Bar, Company, price

TICKER = re.compile(r"[A-Z][A-Z0-9.]{0,7}")
MAX_LIMIT = 100_000


class BadRequest(ValueError):
    pass


def ticker(value: str) -> str:
    if not TICKER.fullmatch(value):
        raise BadRequest("ticker must be 1 to 8 characters: capital letters, digits and dots, starting with a letter")
    return value


def iso_date(value: str, what: str = "date") -> dt.date:
    # fromisoformat alone also takes 20240131 and week dates; the API documents one form
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""):
        try:
            day = dt.date.fromisoformat(value)
        except ValueError:
            day = None
        if day and FIRST_DAY <= day <= LAST_DAY:
            return day
    raise BadRequest(f"{what} must be a calendar date like 2024-01-31, between 1900 and 2999")


def _text(body: dict, key: str, limit: int) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value.strip():
        raise BadRequest(f"{key} is required and must be a non-empty string")
    value = value.strip()
    if len(value) > limit:
        raise BadRequest(f"{key} is longer than {limit} characters")
    return value


def _number(body: dict, key: str) -> float:
    value = body.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BadRequest(f"{key} is required and must be a number")
    if not math.isfinite(value) or not 0 < value <= MAX_PRICE:
        raise BadRequest(f"{key} must be greater than 0 and at most {MAX_PRICE:.0f}")
    return price(value)


def company(tick: str, body) -> Company:
    if not isinstance(body, dict):
        raise BadRequest("body must be a JSON object")
    unknown = set(body) - {"name", "sector", "exchange"}
    if unknown:
        raise BadRequest(f"unknown fields: {', '.join(sorted(unknown))}")
    return Company(ticker(tick), _text(body, "name", 80), _text(body, "sector", 32), _text(body, "exchange", 16))


def bar(day: str, body) -> Bar:
    if not isinstance(body, dict):
        raise BadRequest("body must be a JSON object")
    unknown = set(body) - {"open", "high", "low", "close", "volume"}
    if unknown:
        raise BadRequest(f"unknown fields: {', '.join(sorted(unknown))}")
    o, h, lo, c = (_number(body, k) for k in ("open", "high", "low", "close"))
    if not (lo <= o <= h and lo <= c <= h):
        raise BadRequest("open and close must lie between low and high")
    volume = body.get("volume")
    if isinstance(volume, bool) or not isinstance(volume, int) or not 0 <= volume <= MAX_VOLUME:
        raise BadRequest("volume is required and must be a whole number of at least 0")
    return Bar(iso_date(day), o, h, lo, c, volume)


def window(args) -> tuple[dt.date | None, dt.date | None, int | None]:
    """start, end (both inclusive) and limit from a query string."""
    start = iso_date(args["start"], "start") if args.get("start") else None
    end = iso_date(args["end"], "end") if args.get("end") else None
    if start and end and start > end:
        raise BadRequest("start is after end")
    limit = None
    if args.get("limit") is not None:
        try:
            limit = int(args["limit"])
        except ValueError:
            raise BadRequest("limit must be a whole number") from None
        if not 1 <= limit <= MAX_LIMIT:
            raise BadRequest(f"limit must be between 1 and {MAX_LIMIT}")
    return start, end, limit
