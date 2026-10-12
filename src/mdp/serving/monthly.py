"""Monthly averages, computed from daily bars in the application.

Cassandra cannot GROUP BY a clustering column, and the same code runs for both backends so their
answers cannot drift apart. The extract from stage 2 has a monthly table too, but a bar added
through the API after the load would make a stored month wrong.
"""
import statistics
from collections.abc import Iterable

from mdp.serving.models import Bar


def monthly(bars: Iterable[Bar]) -> list[dict]:
    months: dict[str, list[Bar]] = {}
    for b in bars:
        months.setdefault(b.trade_date.strftime("%Y-%m"), []).append(b)
    out = []
    for month in sorted(months):
        rows = months[month]
        out.append({
            "month": month,
            "trading_days": len(rows),
            "avg_high": round(statistics.fmean(b.high for b in rows), 4),
            "avg_low": round(statistics.fmean(b.low for b in rows), 4),
            "avg_close": round(statistics.fmean(b.close for b in rows), 4),
            "total_volume": sum(b.volume for b in rows),
        })
    return out
