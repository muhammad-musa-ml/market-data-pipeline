"""The prompt: live table schemas read from the Spark catalog, rules, a fixed output format."""
from __future__ import annotations

from typing import TYPE_CHECKING

from mdp.analytics.names import DB

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

ENUM_LIMIT = 10

RULES = """\
Rules:
- Use only the tables and columns above, and write table names with the database, e.g. market.bars.
- bars.symbol_id refers to symbols.symbol_id. Join on it; never hard-code symbol ids.
- Dates are DATE columns: compare with DATE 'YYYY-MM-DD' or use year(trade_date), month(trade_date).
- Return only the columns the question asks for, in the order it mentions them.
- Reply with exactly one Spark SQL statement inside a ```sql code block and nothing else."""


def _enum_values(spark: SparkSession, table: str, column: str, limit: int) -> list[str] | None:
    """The distinct values of a string column if there are at most `limit` of them."""
    rows = spark.sql(f"SELECT DISTINCT {column} FROM {table} LIMIT {limit + 1}").collect()
    return sorted(r[0] for r in rows if r[0] is not None) if len(rows) <= limit else None


def schema_text(spark: SparkSession, db: str = DB, enum_limit: int = ENUM_LIMIT) -> str:
    """Tables, views and columns of one database as the catalog describes them, with the value
    list of every low-cardinality string column of a table (not of a view, which may be costly)."""
    parts = []
    for t in sorted(spark.catalog.listTables(db), key=lambda t: t.name):
        full = f"{db}.{t.name}"
        head = f"{'VIEW' if t.tableType == 'VIEW' else 'TABLE'} {full}"
        if t.description:
            head += f"  -- {t.description}"
        lines = [head]
        for c in spark.catalog.listColumns(full):
            line = f"  {c.name} {c.dataType.upper()}"
            notes = [c.description] if c.description else []
            if c.dataType == "string" and t.tableType != "VIEW":
                values = _enum_values(spark, full, c.name, enum_limit)
                if values:
                    notes.append("values: " + ", ".join(values))
            if notes:
                line += "  -- " + "; ".join(notes)
            lines.append(line)
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def build(question: str, schema: str) -> str:
    return (f"You translate questions about a stock-market warehouse into Spark SQL.\n\n"
            f"{schema}\n\n{RULES}\n\nQuestion: {question.strip()}\n")


def build_repair(question: str, schema: str, sql: str, error: str) -> str:
    return (build(question, schema)
            + f"\nYour previous answer was:\n```sql\n{sql}\n```\nIt failed with: {error.strip()[:600]}\n"
              "Reply with a corrected statement in the same format.\n")
