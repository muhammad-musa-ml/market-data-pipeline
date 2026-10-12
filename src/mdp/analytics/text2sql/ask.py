"""Natural-language question in, SQL and the rows it returns out."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from mdp.analytics.text2sql import prompt, sqlcheck
from mdp.analytics.text2sql.backends import Backend

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class Text2SqlError(RuntimeError):
    def __init__(self, message: str, sql: str | None = None, attempts: int = 0):
        super().__init__(message)
        self.sql = sql
        self.attempts = attempts


@dataclass
class Answer:
    question: str
    sql: str
    columns: list[str]
    rows: list[tuple]
    attempts: int
    model_seconds: float
    total_seconds: float
    history: list[dict] = field(default_factory=list)


def ask(spark: SparkSession, question: str, backend: Backend, *, schema: str | None = None,
        limit: int = sqlcheck.DEFAULT_LIMIT, repairs: int = 1) -> Answer:
    """Prompt the model with the live schema, validate its SQL, run it on Spark.

    When the SQL is rejected or fails to run, the model gets the error and one more try, up to
    `repairs` times. Raises Text2SqlError when none of the attempts produced a result.
    """
    t0 = time.time()
    schema = schema if schema is not None else prompt.schema_text(spark)
    text = prompt.build(question, schema)
    model_seconds = 0.0
    history: list[dict] = []
    sql, error = None, "no attempt"
    for attempt in range(1, repairs + 2):
        t1 = time.time()
        reply = backend.complete(text)
        model_seconds += time.time() - t1
        try:
            sql = sqlcheck.validate(sqlcheck.extract_sql(reply), limit)
            df = spark.sql(sql)
            rows = [tuple(r) for r in df.collect()]
            history.append({"attempt": attempt, "sql": sql, "error": None})
            return Answer(question, sql, df.columns, rows, attempt, model_seconds,
                          time.time() - t0, history)
        except (sqlcheck.SqlRejected, sqlcheck.NoSqlFound) as exc:
            error = str(exc)
        except Exception as exc:  # Spark raises several exception types for bad SQL
            error = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
        history.append({"attempt": attempt, "sql": sql, "error": error})
        text = prompt.build_repair(question, schema, sql or reply, error)
    raise Text2SqlError(f"no usable SQL after {len(history)} attempts: {error}", sql, len(history))
