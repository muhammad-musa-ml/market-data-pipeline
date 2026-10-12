"""Values the API and both backends exchange, and how a price is normalised."""
import datetime as dt
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal

PRICE_PLACES = Decimal("0.0001")
MAX_PRICE = 99_999_999.0
MAX_VOLUME = 2**63 - 1
# MySQL DATE and Cassandra date both hold more; this is what the API accepts
FIRST_DAY = dt.date(1900, 1, 1)
LAST_DAY = dt.date(2999, 12, 31)


def price(value: float | int | Decimal) -> float:
    """Round to the four places the MySQL columns hold.

    Both backends store this value (a double in Cassandra, a DECIMAL(12,4) in MySQL), so the same
    request reads back as the same JSON number from either.
    """
    return float(Decimal(str(value)).quantize(PRICE_PLACES, rounding=ROUND_HALF_EVEN))


@dataclass(frozen=True)
class Company:
    ticker: str
    name: str
    sector: str
    exchange: str

    def to_json(self) -> dict:
        return {"ticker": self.ticker, "name": self.name, "sector": self.sector, "exchange": self.exchange}


@dataclass(frozen=True)
class Bar:
    trade_date: dt.date
    open: float
    high: float
    low: float
    close: float
    volume: int

    def to_json(self) -> dict:
        return {"trade_date": self.trade_date.isoformat(), "open": self.open, "high": self.high,
                "low": self.low, "close": self.close, "volume": self.volume}
