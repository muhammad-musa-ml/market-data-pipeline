import pytest

from mdp.analytics.text2sql import ask as ask_mod
from mdp.analytics.text2sql import evaluate, prompt, sqlcheck
from mdp.analytics.text2sql.evalset import CASES


class ScriptedBackend:
    """Returns canned replies in order and remembers the prompts."""
    name = "scripted"

    def __init__(self, *replies):
        self.replies = list(replies)
        self.prompts = []

    def complete(self, text):
        self.prompts.append(text)
        return self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]


def fenced(sql):
    return f"```sql\n{sql}\n```"


def test_schema_text_is_read_from_the_catalog(spark, warehouse):
    text = prompt.schema_text(spark)
    assert "TABLE market.bars" in text and "TABLE market.symbols" in text
    assert "VIEW market.daily_returns" in text
    assert "ticker STRING" in text and "volume BIGINT" in text
    assert "closing price, USD" in text  # column comments travel with the table
    assert "values: Consumer, Energy" in text  # low-cardinality strings list their values
    assert "values: NASDAQ, NYSE" in text
    assert "market_lab" not in text


def test_prompt_has_the_question_the_schema_and_the_output_format(spark, warehouse):
    schema = prompt.schema_text(spark)
    p = prompt.build("How many bars?", schema)
    assert p.endswith("Question: How many bars?\n")
    assert schema in p
    assert "```sql" in p and "exactly one" in p


def test_ask_runs_the_query_and_returns_sql_and_rows(spark, warehouse, raw):
    bars, _ = raw
    backend = ScriptedBackend(fenced("SELECT COUNT(*) AS n FROM market.bars"))
    ans = ask_mod.ask(spark, "How many bars?", backend)
    assert ans.rows == [(len(bars),)]
    assert ans.columns == ["n"]
    assert ans.sql.endswith("LIMIT 100")
    assert ans.attempts == 1
    assert "market.bars" in backend.prompts[0]


def test_limit_is_enforced_on_the_rows_returned(spark, warehouse):
    ans = ask_mod.ask(spark, "all bars", ScriptedBackend(fenced("SELECT id FROM market.bars")), limit=7)
    assert len(ans.rows) == 7


def test_a_failing_query_is_sent_back_once_with_its_error(spark, warehouse):
    backend = ScriptedBackend(fenced("SELECT nope FROM market.bars"),
                              fenced("SELECT COUNT(*) FROM market.symbols"))
    ans = ask_mod.ask(spark, "How many symbols?", backend)
    assert ans.attempts == 2 and ans.rows == [(10,)]
    assert "failed with" in backend.prompts[1] and "nope" in backend.prompts[1]
    assert ans.history[0]["error"] and ans.history[1]["error"] is None


def test_a_rejected_statement_is_never_executed(spark, warehouse):
    backend = ScriptedBackend(fenced("DROP TABLE market.bars"))
    with pytest.raises(ask_mod.Text2SqlError) as err:
        ask_mod.ask(spark, "drop it", backend, repairs=1)
    assert err.value.attempts == 2
    assert spark.table("market.bars").count() > 0


def test_repairs_can_be_switched_off(spark, warehouse):
    backend = ScriptedBackend(fenced("SELECT nope FROM market.bars"), fenced("SELECT 1"))
    with pytest.raises(ask_mod.Text2SqlError):
        ask_mod.ask(spark, "q", backend, repairs=0)
    assert len(backend.prompts) == 1


def test_a_reply_without_sql_is_an_error(spark, warehouse):
    with pytest.raises(ask_mod.Text2SqlError, match="no SQL"):
        ask_mod.ask(spark, "q", ScriptedBackend("Sorry, I can't."), repairs=0)


@pytest.mark.parametrize("case", CASES, ids=[c.id for c in CASES])
def test_every_gold_query_is_valid_and_runs(spark, warehouse, case):
    ok = sqlcheck.validate(case.gold)
    rows = spark.sql(ok).collect()
    assert rows, case.id


def test_gold_questions_are_unique_and_a_few_dozen():
    assert len({c.id for c in CASES}) == len(CASES)
    assert len({c.question for c in CASES}) == len(CASES)
    assert 20 <= len(CASES) <= 30


def test_scoring_a_backend_that_returns_the_gold_sql(spark, warehouse):
    class Oracle:
        name = "oracle"

        def complete(self, text):
            if "Question: " not in text:
                return "ok"  # the warm-up prompt
            q = text.rsplit("Question: ", 1)[1].strip()
            return fenced(next(c.gold for c in CASES if c.question == q))

    out = evaluate.run(spark, Oracle(), CASES, repairs=0)
    assert out["correct"] == out["questions"] == len(CASES)
    assert out["accuracy"] == 1.0 and out["first_try_accuracy"] == 1.0
    assert out["warmup_seconds"] >= 0


def test_scoring_marks_wrong_answers_and_errors(spark, warehouse):
    case = next(c for c in CASES if c.id == "count-symbols")
    schema = prompt.schema_text(spark)
    wrong = evaluate.score_case(spark, ScriptedBackend(fenced("SELECT COUNT(*) FROM market.bars")),
                                case, schema, repairs=0)
    assert wrong["correct"] is False and wrong["error"] is None
    broken = evaluate.score_case(spark, ScriptedBackend("no sql"), case, schema, repairs=0)
    assert broken["correct"] is False and broken["error"]
    repaired = evaluate.score_case(
        spark, ScriptedBackend(fenced("SELECT x FROM market.symbols"), fenced(case.gold)), case, schema, repairs=1)
    assert repaired["correct"] and not repaired["first_try"] and repaired["attempts"] == 2


def test_extra_columns_do_not_count_as_correct(spark, warehouse):
    case = next(c for c in CASES if c.id == "count-symbols")
    r = evaluate.score_case(spark, ScriptedBackend(fenced("SELECT COUNT(*), 1 FROM market.symbols")),
                            case, prompt.schema_text(spark), repairs=0)
    assert r["correct"] is False


def test_same_rows_rules():
    f = evaluate.same_rows
    assert f([(1, "a"), (2, "b")], [(2, "b"), (1, "a")], ordered=False)
    assert not f([(1, "a"), (2, "b")], [(2, "b"), (1, "a")], ordered=True)
    assert f([(1.23456,)], [(1.23,)], ordered=True)  # a rounded answer is accepted
    assert not f([(1.30,)], [(1.23,)], ordered=True)
    assert f([(5,)], [(5.0,)], ordered=True)
    assert not f([(1,)], [(1,), (2,)], ordered=False)
    assert not f([(1, 2)], [(1,)], ordered=False)
    assert f([], [], ordered=False)
