import pytest

from mdp.analytics.text2sql import sqlcheck
from mdp.analytics.text2sql.sqlcheck import NoSqlFound, SqlRejected, extract_sql, has_top_level, validate


def test_extract_prefers_the_sql_fence():
    reply = "Here you go:\n```sql\nSELECT 1\n```\nand\n```python\nprint(1)\n```"
    assert extract_sql(reply) == "SELECT 1"


def test_extract_takes_the_first_sql_block_when_there_are_two():
    reply = "```sql\nSELECT a FROM t\n```\n```sql\nSELECT b FROM t\n```"
    assert extract_sql(reply) == "SELECT a FROM t"


def test_extract_falls_back_to_an_untagged_fence():
    assert extract_sql("```\nselect 2\n```") == "select 2"


def test_extract_unfenced_reply_starts_at_the_first_select():
    reply = "Sure.\nSELECT COUNT(*)\nFROM market.bars"
    assert extract_sql(reply) == "SELECT COUNT(*)\nFROM market.bars"


def test_extract_with_statement_unfenced():
    assert extract_sql("WITH t AS (SELECT 1 AS x)\nSELECT x FROM t").startswith("WITH t")


def test_extract_fails_without_sql():
    with pytest.raises(NoSqlFound):
        extract_sql("I cannot answer that.")
    with pytest.raises(NoSqlFound):
        extract_sql("```sql\n\n```")


def test_limit_is_added_when_missing():
    assert validate("SELECT a FROM t") == "SELECT a FROM t\nLIMIT 100"
    assert validate("SELECT a FROM t;", limit=5).endswith("LIMIT 5")


def test_existing_limit_is_kept():
    assert validate("SELECT a FROM t LIMIT 3") == "SELECT a FROM t LIMIT 3"
    assert validate("SELECT a FROM t ORDER BY a limit 3;") == "SELECT a FROM t ORDER BY a limit 3"


def test_limit_inside_a_subquery_does_not_count():
    sql = "SELECT * FROM (SELECT a FROM t ORDER BY a LIMIT 3) x"
    assert validate(sql).endswith("\nLIMIT 100")


def test_with_statements_are_allowed():
    sql = "WITH big AS (SELECT * FROM t WHERE a > 1) SELECT COUNT(*) FROM big"
    assert validate(sql).startswith("WITH big")


@pytest.mark.parametrize("sql", [
    "INSERT INTO t VALUES (1)",
    "DROP TABLE t",
    "CREATE TABLE x AS SELECT 1",
    "ALTER TABLE t ADD COLUMNS (z INT)",
    "TRUNCATE TABLE t",
    "DELETE FROM t",
    "UPDATE t SET a = 1",
    "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE",
    "SET spark.sql.shuffle.partitions=1",
    "USE other",
    "EXPLAIN SELECT 1",
    "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
    "SELECT 1; DROP TABLE t",
    "SELECT 1; SELECT 2",
    "(SELECT 1) UNION (SELECT 2)",
    "",
    "   ;  ",
])
def test_rejected(sql):
    with pytest.raises(SqlRejected):
        validate(sql)


def test_a_statement_hidden_after_a_comment_is_still_seen():
    with pytest.raises(SqlRejected):
        validate("SELECT 1 -- harmless\n; DROP TABLE t")
    with pytest.raises(SqlRejected):
        validate("SELECT 1 /* x */ ; /* y */ DELETE FROM t")


def test_comments_are_removed_so_they_cannot_swallow_the_limit():
    out = validate("SELECT a FROM t -- everything\n")
    assert out == "SELECT a FROM t\nLIMIT 100"
    assert "--" not in validate("SELECT a /* why */ FROM t")


def test_keywords_in_literals_and_identifiers_are_fine():
    assert validate("SELECT update_time, `drop` FROM t WHERE note = 'delete; drop table x'")
    assert validate("SELECT 'it''s; DROP' AS s FROM t")
    assert validate('SELECT "a;b" FROM t')
    assert validate("SELECT created_at, date_add(d, 1), insert_count FROM t")


def test_unbalanced_input_is_rejected():
    for bad in ("SELECT (1 FROM t", "SELECT 1) FROM t", "SELECT 'abc FROM t", "SELECT 1 /* open"):
        with pytest.raises(SqlRejected):
            validate(bad)


def test_has_top_level_ignores_nested_and_quoted_keywords():
    assert has_top_level("SELECT a FROM t ORDER BY a", "order")
    assert not has_top_level("SELECT a FROM (SELECT a FROM t ORDER BY a) x", "order")
    assert not has_top_level("SELECT 'order by' FROM t", "order")


def test_default_limit_constant_is_what_validate_uses():
    assert validate("SELECT 1").endswith(f"LIMIT {sqlcheck.DEFAULT_LIMIT}")
