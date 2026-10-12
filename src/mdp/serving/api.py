"""REST API over companies and their daily bars.

    GET  /<db>/api/companies[?sector=&exchange=]
    GET  /<db>/api/companies/<ticker>
    POST /<db>/api/companies/<ticker>                  {"name", "sector", "exchange"}
    PUT  /<db>/api/companies/<ticker>/records/<date>   {"open", "high", "low", "close", "volume"}
    GET  /<db>/api/companies/<ticker>/records[?start=&end=&limit=]
    GET  /<db>/api/companies/<ticker>/records/monthly[?start=&end=]

<db> is "prod" or "test". The same app runs over either backend; see store.py.
"""
import threading
from collections.abc import Callable

from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

from mdp.serving import validate
from mdp.serving.monthly import monthly
from mdp.serving.settings import ServingSettings
from mdp.serving.store import Conflict, Store, StoreError, Unavailable

DATABASES = ("prod", "test")


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def create_app(open_store: Callable[[str], Store], backend: str = "") -> Flask:
    """`open_store(db)` returns the store for "prod" or "test"; it is called once per db."""
    app = Flask(__name__)
    app.json.sort_keys = False
    stores: dict[str, Store] = {}
    lock = threading.Lock()

    def store_for(db: str) -> Store:
        if db not in DATABASES:
            raise ApiError(404, f"unknown database {db!r}; use prod or test")
        with lock:
            if db not in stores:
                stores[db] = open_store(db)  # raises Unavailable if the backend is not reachable yet
            return stores[db]

    def company_or_404(store: Store, tick: str):
        c = store.get_company(tick)
        if c is None:
            raise ApiError(404, f"no company {tick}")
        return c

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "backend": backend}

    @app.get("/<db>/api/companies")
    def list_companies(db):
        sector, exchange = request.args.get("sector"), request.args.get("exchange")
        out = store_for(db).list_companies()
        return jsonify([c.to_json() for c in out
                        if (sector is None or c.sector == sector) and (exchange is None or c.exchange == exchange)])

    @app.get("/<db>/api/companies/<tick>")
    def get_company(db, tick):
        return company_or_404(store_for(db), validate.ticker(tick)).to_json()

    @app.post("/<db>/api/companies/<tick>")
    def add_company(db, tick):
        c = validate.company(tick, request.get_json(silent=True))
        store_for(db).add_company(c)
        resp = jsonify(c.to_json())
        resp.status_code = 201
        resp.headers["Location"] = f"/{db}/api/companies/{c.ticker}"
        return resp

    @app.put("/<db>/api/companies/<tick>/records/<day>")
    def put_record(db, tick, day):
        tick = validate.ticker(tick)
        bar = validate.bar(day, request.get_json(silent=True))
        store = store_for(db)
        company_or_404(store, tick)
        store.put_bar(tick, bar)
        return bar.to_json()

    @app.get("/<db>/api/companies/<tick>/records")
    def get_records(db, tick):
        tick = validate.ticker(tick)
        start, end, limit = validate.window(request.args)
        store = store_for(db)
        company_or_404(store, tick)
        return jsonify([b.to_json() for b in store.bars(tick, start, end, limit)])

    @app.get("/<db>/api/companies/<tick>/records/monthly")
    def get_monthly(db, tick):
        tick = validate.ticker(tick)
        start, end, _ = validate.window(request.args)
        store = store_for(db)
        company_or_404(store, tick)
        return jsonify(monthly(store.bars(tick, start, end)))

    @app.errorhandler(validate.BadRequest)
    def bad_request(exc):
        return jsonify(error=str(exc)), 400

    @app.errorhandler(ApiError)
    def api_error(exc):
        return jsonify(error=str(exc)), exc.status

    @app.errorhandler(Conflict)
    def conflict(exc):
        return jsonify(error=str(exc)), 409

    @app.errorhandler(Unavailable)
    def unavailable(exc):
        app.logger.warning("backend unavailable: %s", exc)
        return jsonify(error="backend unavailable", detail=str(exc)), 503

    @app.errorhandler(StoreError)
    def store_error(exc):
        app.logger.error("store error: %s", exc)
        return jsonify(error=str(exc)), 500

    @app.errorhandler(HTTPException)
    def http_error(exc):
        return jsonify(error=exc.description), exc.code

    return app


def create_app_from_env() -> Flask:
    from mdp.serving.backends import BACKENDS, open_store

    cfg = ServingSettings.from_env()
    if cfg.backend not in BACKENDS:
        raise SystemExit(f"API_BACKEND must be one of {BACKENDS}, not {cfg.backend!r}")
    return create_app(lambda db: open_store(cfg.backend, db, cfg), cfg.backend)
