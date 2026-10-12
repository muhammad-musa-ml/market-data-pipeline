"""MySQL and Cassandra must give byte-for-byte the same answers to the same requests."""
import json

from helpers import COMPANY, bar

PRICES = [(f"2024-{m:02d}-{d:02d}", 50 + (m * 31 + d * 7) % 23 + (d % 5) / 7)
          for m in (1, 2, 3) for d in (1, 2, 3, 15, 28)]


def exchange(a, b, method, path, body=None):
    out = []
    for api in (a, b):
        r = getattr(api, method)(path, body) if body is not None else getattr(api, method)(path)
        out.append((r.status_code, r.json()))
    return out


def test_the_same_requests_give_the_same_responses(both, ticker):
    mysql, cassandra = both
    seq = [("post", f"companies/{ticker}", COMPANY), ("post", f"companies/{ticker}", COMPANY),
           ("post", f"companies/{ticker}", {"name": "x"})]
    seq += [("put", f"companies/{ticker}/records/{day}", bar(round(close, 6), volume=int(close * 1000)))
            for day, close in PRICES]
    seq += [("put", f"companies/{ticker}/records/2024-02-30", bar(10)), ("get", f"companies/{ticker}", None),
            ("get", "companies/ZNOSUCH1", None), ("get", f"companies/{ticker}/records", None),
            ("get", f"companies/{ticker}/records/monthly", None)]
    for method, path, body in seq:
        got = exchange(mysql, cassandra, method, path, body)
        assert got[0] == got[1], f"{method} {path}: mysql {got[0]} != cassandra {got[1]}"


def test_the_same_query_strings_give_the_same_responses(both, ticker):
    mysql, cassandra = both
    mysql.post(f"companies/{ticker}", COMPANY)
    cassandra.post(f"companies/{ticker}", COMPANY)
    for day, close in PRICES:
        for api in both:
            api.put(f"companies/{ticker}/records/{day}", bar(close))
    for q in ("", "?start=2024-02-01", "?end=2024-01-28", "?start=2024-01-15&end=2024-02-15&limit=4", "?limit=1"):
        for suffix in ("records", "records/monthly"):
            if suffix.endswith("monthly") and "limit" in q:
                continue
            a, b = (api.http.get(f"{api.url}/test/api/companies/{ticker}/{suffix}{q}", timeout=30) for api in both)
            assert a.status_code == b.status_code == 200
            assert json.dumps(a.json()) == json.dumps(b.json()), (suffix, q)


def test_prod_holds_identical_data_when_both_backends_were_loaded(both):
    mysql, cassandra = (api.on("prod") for api in both)
    a, b = (api.get("companies").json() for api in (mysql, cassandra))
    if not a or not b:
        import pytest
        pytest.skip("prod is not loaded in both backends")
    assert a == b
    for c in a[:5]:
        t = c["ticker"]
        assert mysql.get(f"companies/{t}/records").json() == cassandra.get(f"companies/{t}/records").json()
        assert mysql.get(f"companies/{t}/records/monthly").json() == cassandra.get(f"companies/{t}/records/monthly").json()
