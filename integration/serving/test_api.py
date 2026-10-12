"""The REST API against a running backend. Nothing in this file knows which backend it is talking to:
the `api` fixture runs every test once against MySQL and once against Cassandra."""
import pytest
from helpers import COMPANY, bar


def test_create_and_fetch_a_company(api, ticker):
    r = api.post(f"companies/{ticker}", COMPANY)
    assert r.status_code == 201
    assert r.json() == {"ticker": ticker, **COMPANY}
    assert r.headers["Location"].endswith(f"/test/api/companies/{ticker}")
    assert api.get(f"companies/{ticker}").json() == {"ticker": ticker, **COMPANY}


def test_a_second_create_conflicts_and_changes_nothing(api, company):
    r = api.post(f"companies/{company}", {**COMPANY, "name": "Impostor"})
    assert r.status_code == 409
    assert api.get(f"companies/{company}").json()["name"] == COMPANY["name"]


def test_unknown_company_is_404_on_every_read(api, ticker):
    for path in (f"companies/{ticker}", f"companies/{ticker}/records", f"companies/{ticker}/records/monthly"):
        assert api.get(path).status_code == 404, path
    assert api.put(f"companies/{ticker}/records/2024-01-02", bar(10)).status_code == 404
    assert api.get(f"companies/{ticker}").json()["error"]


def test_list_contains_the_company_and_filters(api, company):
    everything = api.get("companies").json()
    assert company in [c["ticker"] for c in everything]
    assert [c["ticker"] for c in everything] == sorted(c["ticker"] for c in everything)
    mine = api.get("companies", sector="Testing", exchange="TEST").json()
    assert company in [c["ticker"] for c in mine]
    assert all(c["sector"] == "Testing" and c["exchange"] == "TEST" for c in mine)
    assert api.get("companies", sector="No such sector").json() == []


def test_non_ascii_text_round_trips(api, ticker):
    # accents, an en dash and two CJK characters, spelled as code points so the file stays ASCII
    name = "".join(map(chr, [0x53, 0x6f, 0x63, 0x69, 0xe9, 0x74, 0xe9, 0x20, 0x47, 0xe9, 0x6e, 0xe9, 0x72, 0x61,
                            0x6c, 0x65, 0x20, 0x2013, 0x20, 0x6771, 0x4eac]))
    assert api.post(f"companies/{ticker}", {**COMPANY, "name": name}).status_code == 201
    assert api.get(f"companies/{ticker}").json()["name"] == name


def test_record_upsert(api, company):
    path = f"companies/{company}/records/2024-01-02"
    first = api.put(path, bar(10.5, volume=7))
    assert first.status_code == 200
    assert first.json() == {"trade_date": "2024-01-02", "open": 10.5, "high": 11.5, "low": 9.5, "close": 10.5,
                            "volume": 7}
    api.put(path, bar(12.0, volume=9))
    rows = api.get(f"companies/{company}/records").json()
    assert len(rows) == 1
    assert (rows[0]["close"], rows[0]["volume"]) == (12.0, 9)


def test_prices_are_rounded_to_four_places_the_same_way(api, company):
    api.put(f"companies/{company}/records/2024-01-02",
            {"open": 10.00005, "high": 10.123456, "low": 9.99995, "close": 10.00015, "volume": 1})
    (row,) = api.get(f"companies/{company}/records").json()
    assert (row["open"], row["high"], row["low"], row["close"]) == (10.0, 10.1235, 10.0, 10.0002)


def test_a_write_is_visible_to_the_very_next_read(api, company):
    for i in range(25):
        close = 100.0 + i
        assert api.put(f"companies/{company}/records/2024-03-{i + 1:02d}", bar(close)).status_code == 200
        rows = api.get(f"companies/{company}/records", start=f"2024-03-{i + 1:02d}",
                       end=f"2024-03-{i + 1:02d}").json()
        assert [r["close"] for r in rows] == [close], f"read after write {i} missed the write"


def test_records_come_back_oldest_first_with_inclusive_bounds_and_limit(api, company):
    for day in ("2024-01-05", "2024-01-02", "2024-01-04", "2024-01-03", "2024-01-06"):
        api.put(f"companies/{company}/records/{day}", bar(int(day[-2:])))
    days = lambda **q: [r["trade_date"] for r in api.get(f"companies/{company}/records", **q).json()]  # noqa: E731
    assert days() == ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-06"]
    assert days(start="2024-01-03", end="2024-01-05") == ["2024-01-03", "2024-01-04", "2024-01-05"]
    assert days(start="2024-01-05") == ["2024-01-05", "2024-01-06"]
    assert days(end="2024-01-02") == ["2024-01-02"]
    assert days(limit=2) == ["2024-01-02", "2024-01-03"]
    assert days(start="2030-01-01") == []


def test_company_with_no_records_returns_empty_lists(api, company):
    assert api.get(f"companies/{company}/records").json() == []
    assert api.get(f"companies/{company}/records/monthly").json() == []


def test_monthly_averages(api, company):
    data = {"2024-01-02": 10, "2024-01-03": 20, "2024-01-31": 30, "2024-02-29": 40, "2024-03-01": 50}
    for day, close in data.items():
        api.put(f"companies/{company}/records/{day}", {"open": close, "high": close + 2, "low": close - 2,
                                                        "close": close, "volume": close * 100})
    months = api.get(f"companies/{company}/records/monthly").json()
    assert months == [
        {"month": "2024-01", "trading_days": 3, "avg_high": 22.0, "avg_low": 18.0, "avg_close": 20.0,
         "total_volume": 6000},
        {"month": "2024-02", "trading_days": 1, "avg_high": 42.0, "avg_low": 38.0, "avg_close": 40.0,
         "total_volume": 4000},
        {"month": "2024-03", "trading_days": 1, "avg_high": 52.0, "avg_low": 48.0, "avg_close": 50.0,
         "total_volume": 5000},
    ]
    narrowed = api.get(f"companies/{company}/records/monthly", start="2024-02-01", end="2024-02-29").json()
    assert [m["month"] for m in narrowed] == ["2024-02"]


@pytest.mark.parametrize("body", [
    {"name": "x"}, {**COMPANY, "sector": ""}, {**COMPANY, "bonus": 1}, {**COMPANY, "name": "n" * 81}, [],
])
def test_bad_company_bodies_are_400(api, ticker, body):
    assert api.post(f"companies/{ticker}", body).status_code == 400
    assert api.get(f"companies/{ticker}").status_code == 404  # and nothing was created


@pytest.mark.parametrize("day,body", [
    ("2024-02-30", bar(10)),
    ("2024-01-02", bar(10, high=5)),
    ("2024-01-02", bar(10, volume=-1)),
    ("2024-01-02", bar(10, volume="many")),
    ("2024-01-02", {"close": 10}),
    ("2024-01-02", {"open": 0, "high": 1, "low": 0, "close": 0, "volume": 1}),
])
def test_bad_records_are_400_and_not_stored(api, company, day, body):
    assert api.put(f"companies/{company}/records/{day}", body).status_code == 400
    assert api.get(f"companies/{company}/records").json() == []


@pytest.mark.parametrize("query", [{"start": "2024-02-01", "end": "2024-01-01"}, {"limit": 0}, {"start": "soon"}])
def test_bad_query_strings_are_400(api, company, query):
    assert api.get(f"companies/{company}/records", **query).status_code == 400


def test_lowercase_ticker_is_400(api):
    assert api.get("companies/abc").status_code == 400


def test_prod_and_test_do_not_share_data(api, company):
    assert api.on("prod").get(f"companies/{company}").status_code == 404


def test_unknown_database_is_404(api):
    assert api.on("staging").get("companies").status_code == 404


def test_prod_holds_the_loaded_extract_and_its_months_add_up(api):
    prod = api.on("prod")
    companies = prod.get("companies").json()
    if not companies:
        pytest.skip("prod has not been loaded (docker compose --profile serving-tools run --rm serving-tools "
                    "python -m mdp.serving.loader)")
    tick = companies[0]["ticker"]
    rows = prod.get(f"companies/{tick}/records").json()
    months = prod.get(f"companies/{tick}/records/monthly").json()
    assert sum(m["trading_days"] for m in months) == len(rows) > 1000
    assert rows == sorted(rows, key=lambda r: r["trade_date"])
    assert all(r["low"] <= r["open"] <= r["high"] and r["low"] <= r["close"] <= r["high"] for r in rows)
