"""Seeded daily OHLCV model. Pure numpy, no I/O.

Each day's returns come from an RNG seeded with (seed, day_index), so the series
does not depend on how it was chunked or whether the loader was restarted: the
live generator resumes from the database and produces the same bars an
uninterrupted run would have.
"""
import datetime as dt
from dataclasses import dataclass

import numpy as np

SECTORS = (
    "Technology", "Financials", "Health Care", "Energy",
    "Consumer", "Industrials", "Utilities", "Materials",
)
EXCHANGES = ("NASDAQ", "NYSE")
_SUFFIX = {
    "Technology": "Systems", "Financials": "Capital", "Health Care": "Therapeutics",
    "Energy": "Resources", "Consumer": "Brands", "Industrials": "Industries",
    "Utilities": "Power", "Materials": "Materials",
}

START_DATE = dt.date(2014, 1, 6)  # a Monday

MARKET_VOL = 0.0075
SECTOR_VOL = 0.0055


@dataclass(frozen=True)
class SymbolSpec:
    symbol_id: int
    ticker: str
    name: str
    sector: str
    exchange: str
    start_price: float
    drift: float
    idio_vol: float
    beta_market: float
    beta_sector: float
    base_volume: int


def trading_date(day_index: int) -> dt.date:
    """Day 0 is START_DATE; weekends are skipped, holidays are not modelled."""
    return np.busday_offset(START_DATE, day_index, roll="forward").astype(dt.date)


def day_index_of(date: dt.date) -> int:
    return int(np.busday_count(START_DATE, date))


def _tickers(rng: np.random.Generator, n: int) -> list[str]:
    cons, vowels = list("BCDFGHJKLMNPRSTVWZ"), list("AEIOU")
    seen: set[str] = set()
    out: list[str] = []
    while len(out) < n:
        length = int(rng.integers(3, 5))
        t = "".join(str(rng.choice(cons if i % 2 == 0 else vowels)) for i in range(length))
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def make_symbols(n: int, seed: int) -> list[SymbolSpec]:
    rng = np.random.default_rng([seed, 0xA11CE])
    tickers = _tickers(rng, n)
    sector_of = [SECTORS[i % len(SECTORS)] for i in range(n)]
    rng.shuffle(sector_of)
    specs = []
    for i in range(n):
        sector = str(sector_of[i])
        specs.append(SymbolSpec(
            symbol_id=i + 1,
            ticker=tickers[i],
            name=f"{tickers[i].title()} {_SUFFIX[sector]}",
            sector=sector,
            exchange=str(rng.choice(EXCHANGES)),
            start_price=round(float(rng.lognormal(4.1, 0.6)), 2),
            drift=float(rng.normal(0.0003, 0.0002)),
            idio_vol=float(rng.uniform(0.008, 0.018)),
            beta_market=float(rng.uniform(0.6, 1.5)),
            beta_sector=float(rng.uniform(0.5, 1.4)),
            base_volume=int(rng.lognormal(13.5, 0.7)),
        ))
    return specs


@dataclass
class DayBars:
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray


class Universe:
    def __init__(self, specs: list[SymbolSpec], seed: int):
        self.specs = specs
        self.seed = seed
        sectors = sorted({s.sector for s in specs})
        self._sector_idx = np.array([sectors.index(s.sector) for s in specs])
        self._n_sectors = len(sectors)
        self._drift = np.array([s.drift for s in specs])
        self._idio = np.array([s.idio_vol for s in specs])
        self._bm = np.array([s.beta_market for s in specs])
        self._bs = np.array([s.beta_sector for s in specs])
        self._base_vol = np.array([s.base_volume for s in specs], dtype=float)

    def __len__(self) -> int:
        return len(self.specs)

    def initial_close(self) -> np.ndarray:
        return np.array([s.start_price for s in self.specs])

    def next_day(self, prev_close: np.ndarray, day_index: int) -> DayBars:
        n = len(self)
        rng = np.random.default_rng([self.seed, day_index])
        market = rng.normal(0.0, MARKET_VOL)
        sector = rng.normal(0.0, SECTOR_VOL, size=self._n_sectors)
        eps = rng.normal(size=n)
        z_open, z_hi, z_lo, z_vol = rng.normal(size=(4, n))

        ret = self._drift + self._bm * market + self._bs * sector[self._sector_idx] + self._idio * eps
        close = np.maximum(np.round(prev_close * np.exp(ret), 4), 1.0)
        open_ = np.maximum(np.round(prev_close * np.exp(0.35 * ret + 0.003 * z_open), 4), 1.0)
        top = np.maximum(open_, close)
        bottom = np.minimum(open_, close)
        high = np.round(top * np.exp(np.abs(0.004 * z_hi)), 4)
        low = np.maximum(np.round(bottom * np.exp(-np.abs(0.004 * z_lo)), 4), 1.0)
        volume = np.maximum(1, (self._base_vol * np.exp(0.25 * z_vol + 12.0 * np.abs(ret))).astype(np.int64))
        return DayBars(open_, high, low, close, volume)
