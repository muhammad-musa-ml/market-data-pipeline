"""Execution accuracy: a model's SQL is right when it returns the same rows as the gold SQL."""
import datetime as dt
import decimal
import math
import time

from mdp.analytics.text2sql import ask as ask_mod
from mdp.analytics.text2sql import prompt, sqlcheck
from mdp.analytics.text2sql.evalset import CASES, Case

# ROUND(x, 2) in a model's answer should still match the exact gold value
REL_TOL = 1e-3
ABS_TOL = 0.01


def _canon(v):
    if isinstance(v, (decimal.Decimal, int, float)) and not isinstance(v, bool):
        return float(v)
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()
    return v


def _row_key(row: tuple) -> tuple:
    return tuple((0, round(v, 1)) if isinstance(v, float) else (1, str(v)) for v in row)


def _same_value(a, b) -> bool:
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=REL_TOL, abs_tol=ABS_TOL)
    return a == b


def same_rows(got: list[tuple], gold: list[tuple], ordered: bool) -> bool:
    """Equal row sets (equal sequences when `ordered`), column names ignored, numbers compared
    with a small tolerance."""
    a = [tuple(_canon(v) for v in r) for r in got]
    b = [tuple(_canon(v) for v in r) for r in gold]
    if len(a) != len(b) or any(len(x) != len(y) for x, y in zip(a, b)):
        return False
    if not ordered:
        a, b = sorted(a, key=_row_key), sorted(b, key=_row_key)
    return all(_same_value(u, v) for x, y in zip(a, b) for u, v in zip(x, y))


def score_case(spark, backend, case: Case, schema: str, repairs: int = 1) -> dict:
    gold_rows = [tuple(r) for r in spark.sql(case.gold).collect()]
    ordered = sqlcheck.has_top_level(case.gold, "order")
    t0 = time.time()
    out = {"id": case.id, "question": case.question, "backend": backend.name}
    try:
        ans = ask_mod.ask(spark, case.question, backend, schema=schema, repairs=repairs)
    except ask_mod.Text2SqlError as exc:
        return {**out, "correct": False, "first_try": False, "attempts": exc.attempts,
                "sql": exc.sql, "error": str(exc), "seconds": round(time.time() - t0, 2)}
    correct = same_rows(ans.rows, gold_rows, ordered)
    return {**out, "correct": correct, "first_try": correct and ans.attempts == 1,
            "attempts": ans.attempts, "sql": ans.sql, "error": None,
            "model_seconds": round(ans.model_seconds, 2), "seconds": round(ans.total_seconds, 2)}


def run(spark, backend, cases: list[Case] = CASES, repairs: int = 1) -> dict:
    schema = prompt.schema_text(spark)
    rows = [score_case(spark, backend, c, schema, repairs) for c in cases]
    n = len(rows)
    secs = sorted(r["seconds"] for r in rows)
    return {
        "backend": backend.name, "questions": n, "repairs": repairs,
        "correct": sum(r["correct"] for r in rows),
        "accuracy": round(sum(r["correct"] for r in rows) / n, 3),
        "first_try_accuracy": round(sum(r["first_try"] for r in rows) / n, 3),
        "median_seconds": secs[n // 2],
        "max_seconds": secs[-1],
        "cases": rows,
    }
