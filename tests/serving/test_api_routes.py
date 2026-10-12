import pytest

from mdp.serving.api import create_app
from serving_fakes import MemoryStore

BAR = {"open": 10, "high": 12, "low": 9, "close": 11, "volume": 500}
COMPANY = {"name": "Acme Corp", "sector": "Energy", "exchange": "NYSE"}


@pytest.fixture
def stores():
    return {"prod": MemoryStore(), "test": MemoryStore()}


@pytest.fixture
def client(stores):
    return create_app(lambda db: stores[db], "memory").test_client()


def add(client, tick="ACME", db="test", **over):
    return client.post(f"/{db}/api/companies/{tick}", json={**COMPANY, **over})


def test_healthz_names_the_backend(client):
    assert client.get("/healthz").get_json() == {"ok": True, "backend": "memory"}


def test_add_company_returns_201_with_location_and_body(client):
    r = add(client)
    assert r.status_code == 201
    assert r.headers["Location"] == "/test/api/companies/ACME"
    assert r.get_json() == {"ticker": "ACME", **COMPANY}


def test_adding_a_company_twice_is_a_conflict(client):
    add(client)
    r = add(client, name="Other")
    assert r.status_code == 409
    assert client.get("/test/api/companies/ACME").get_json()["name"] == "Acme Corp"


@pytest.mark.parametrize("body", [{}, {"name": "x"}, {**COMPANY, "extra": 1}, [], {**COMPANY, "sector": ""}])
def test_add_company_rejects_bad_bodies(client, body):
    assert client.post("/test/api/companies/ACME", json=body).status_code == 400


def test_add_company_rejects_a_body_that_is_not_json(client):
    assert client.post("/test/api/companies/ACME", data="nope", content_type="text/plain").status_code == 400


def test_bad_ticker_is_400_everywhere(client):
    assert client.get("/test/api/companies/acme").status_code == 400
    assert add(client, tick="acme").status_code == 400
    assert client.get("/test/api/companies/acme/records").status_code == 400


def test_get_unknown_company_is_404(client):
    assert client.get("/test/api/companies/NOPE").status_code == 404


def test_list_filters_by_sector_and_exchange_and_sorts(client):
    add(client, "BBB", sector="Tech")
    add(client, "AAA", sector="Energy")
    add(client, "CCC", sector="Tech", exchange="NASDAQ")
    names = lambda r: [c["ticker"] for c in r.get_json()]  # noqa: E731
    assert names(client.get("/test/api/companies")) == ["AAA", "BBB", "CCC"]
    assert names(client.get("/test/api/companies?sector=Tech")) == ["BBB", "CCC"]
    assert names(client.get("/test/api/companies?sector=Tech&exchange=NASDAQ")) == ["CCC"]
    assert names(client.get("/test/api/companies?sector=Nothing")) == []


def test_prod_and_test_are_separate_and_other_names_are_404(client):
    add(client, db="prod")
    assert client.get("/test/api/companies").get_json() == []
    assert client.get("/dev/api/companies").status_code == 404


def test_put_record_then_read_it_back_and_overwrite(client):
    add(client)
    r = client.put("/test/api/companies/ACME/records/2024-01-02", json=BAR)
    assert r.status_code == 200
    assert r.get_json() == {"trade_date": "2024-01-02", **{k: float(v) if k != "volume" else v
                                                            for k, v in BAR.items()}}
    client.put("/test/api/companies/ACME/records/2024-01-02", json={**BAR, "close": 11.5})
    rows = client.get("/test/api/companies/ACME/records").get_json()
    assert len(rows) == 1 and rows[0]["close"] == 11.5


def test_put_record_for_unknown_company_is_404_and_stores_nothing(client, stores):
    assert client.put("/test/api/companies/NOPE/records/2024-01-02", json=BAR).status_code == 404
    assert stores["test"].rows == {}


@pytest.mark.parametrize("day,body", [("2024-02-30", BAR), ("20240102", BAR), ("2024-01-02", {**BAR, "high": 1})])
def test_put_record_validation(client, day, body):
    add(client)
    assert client.put(f"/test/api/companies/ACME/records/{day}", json=body).status_code == 400


def test_records_window_limit_and_order(client):
    add(client)
    for d in range(1, 6):
        client.put(f"/test/api/companies/ACME/records/2024-01-0{d}", json=BAR)
    days = lambda q: [r["trade_date"] for r in client.get(f"/test/api/companies/ACME/records{q}").get_json()]  # noqa: E731
    assert days("") == [f"2024-01-0{d}" for d in range(1, 6)]
    assert days("?start=2024-01-02&end=2024-01-04") == ["2024-01-02", "2024-01-03", "2024-01-04"]
    assert days("?limit=2") == ["2024-01-01", "2024-01-02"]
    assert days("?start=2025-01-01") == []
    assert client.get("/test/api/companies/ACME/records?start=2024-01-05&end=2024-01-01").status_code == 400
    assert client.get("/test/api/companies/ACME/records?limit=0").status_code == 400


def test_a_company_without_records_has_an_empty_list_not_a_404(client):
    add(client)
    assert client.get("/test/api/companies/ACME/records").get_json() == []
    assert client.get("/test/api/companies/ACME/records/monthly").get_json() == []


def test_monthly_endpoint_groups_the_stored_bars(client):
    add(client)
    for day, close in (("2024-01-02", 10), ("2024-01-03", 12), ("2024-02-01", 20)):
        client.put(f"/test/api/companies/ACME/records/{day}", json={**BAR, "close": close, "high": 25, "low": 5, "open": 10})
    months = client.get("/test/api/companies/ACME/records/monthly").get_json()
    assert [(m["month"], m["trading_days"], m["avg_close"]) for m in months] == [("2024-01", 2, 11.0), ("2024-02", 1, 20.0)]
    only_feb = client.get("/test/api/companies/ACME/records/monthly?start=2024-02-01").get_json()
    assert [m["month"] for m in only_feb] == ["2024-02"]


def test_monthly_for_unknown_company_is_404(client):
    assert client.get("/test/api/companies/NOPE/records/monthly").status_code == 404


def test_unavailable_backend_is_503_on_every_endpoint(client, stores):
    add(client)
    stores["test"].down = True
    calls = [
        client.get("/test/api/companies"),
        client.get("/test/api/companies/ACME"),
        client.post("/test/api/companies/BBB", json=COMPANY),
        client.put("/test/api/companies/ACME/records/2024-01-02", json=BAR),
        client.get("/test/api/companies/ACME/records"),
        client.get("/test/api/companies/ACME/records/monthly"),
    ]
    assert [r.status_code for r in calls] == [503] * 6
    assert calls[0].get_json()["error"] == "backend unavailable"


def test_a_store_that_cannot_be_opened_is_503_and_is_tried_again(stores):
    from mdp.serving.store import Unavailable
    attempts = []

    def open_store(db):
        attempts.append(db)
        if len(attempts) == 1:
            raise Unavailable("not up yet")
        return stores[db]

    c = create_app(open_store).test_client()
    assert c.get("/test/api/companies").status_code == 503
    assert c.get("/test/api/companies").status_code == 200
    assert c.get("/test/api/companies").status_code == 200
    assert attempts == ["test", "test"]


def test_errors_are_json(client):
    assert client.get("/nowhere").get_json()["error"]
    r = client.delete("/test/api/companies/ACME")
    assert r.status_code == 405 and r.get_json()["error"]
