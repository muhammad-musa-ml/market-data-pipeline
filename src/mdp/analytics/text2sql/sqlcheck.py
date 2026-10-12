"""Pulling SQL out of a model reply and checking that it is safe to run.

The check is lexical on purpose: it works without a Spark session. Comments are removed and
string literals and quoted identifiers are masked before any keyword is looked for, so a column
called `update_time` or a literal 'drop table' is fine.
"""
import re

DEFAULT_LIMIT = 100

FORBIDDEN = frozenset({
    "insert", "update", "delete", "drop", "create", "alter", "truncate", "merge", "grant",
    "revoke", "load", "export", "refresh", "msck", "analyze", "cache", "uncache", "call",
    "set", "reset", "use", "add", "optimize", "vacuum", "copy",
})


class SqlRejected(ValueError):
    pass


class NoSqlFound(ValueError):
    pass


_FENCE = re.compile(r"```[ \t]*([A-Za-z]*)[ \t]*\r?\n(.*?)```", re.DOTALL)
_START = re.compile(r"(?im)^[ \t]*(select|with)\b")


def extract_sql(reply: str) -> str:
    """The SQL in a model reply: the first ```sql block, else the first fenced block, else
    everything from the first line that starts with SELECT or WITH."""
    blocks = _FENCE.findall(reply)
    for lang, body in blocks:
        if lang.lower() == "sql" and body.strip():
            return body.strip()
    for _, body in blocks:
        if body.strip():
            return body.strip()
    m = _START.search(reply)
    if m:
        return reply[m.start():].strip()
    raise NoSqlFound("the reply has no SQL in it")


def _scan(sql: str) -> tuple[str, str]:
    """Return (code, masked): the SQL without comments, and the same with every string literal and
    quoted identifier replaced by a placeholder of the same kind."""
    code: list[str] = []
    masked: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
            code.append(" ")
            masked.append(" ")
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            if j < 0:
                raise SqlRejected("unterminated comment")
            i = j + 2
            code.append(" ")
            masked.append(" ")
        elif c in "'\"`":
            j = i + 1
            while j < n:
                if sql[j] == "\\" and c != "`":
                    j += 2
                    continue
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # doubled quote
                        j += 2
                        continue
                    break
                j += 1
            if j >= n:
                raise SqlRejected("unterminated quote")
            code.append(sql[i:j + 1])
            masked.append("`q`" if c == "`" else "'s'")
            i = j + 1
        else:
            code.append(c)
            masked.append(c)
            i += 1
    return "".join(code), "".join(masked)


def _words(masked: str) -> list[tuple[str, int]]:
    """Lower-cased words with the parenthesis depth they sit at."""
    out, depth = [], 0
    for m in re.finditer(r"\(|\)|[A-Za-z_][A-Za-z_0-9]*", masked):
        tok = m.group()
        if tok == "(":
            depth += 1
        elif tok == ")":
            depth -= 1
        else:
            out.append((tok.lower(), depth))
    return out


def has_top_level(sql: str, keyword: str) -> bool:
    _, masked = _scan(sql)
    return any(w == keyword and d == 0 for w, d in _words(masked))


def validate(sql: str, limit: int = DEFAULT_LIMIT) -> str:
    """One SELECT or WITH statement, nothing that writes or changes anything, and a LIMIT.

    Returns the statement without comments or a trailing semicolon, with `LIMIT n` appended when
    the outermost query has none. Raises SqlRejected otherwise.
    """
    code, masked = _scan(sql)
    code, masked = code.strip(), masked.strip()
    while masked.endswith(";"):
        code, masked = code[:-1].rstrip(), masked[:-1].rstrip()
    if not masked:
        raise SqlRejected("empty statement")
    if ";" in masked:
        raise SqlRejected("more than one statement")
    words = _words(masked)
    if words[0][0] not in ("select", "with") or masked.lstrip().startswith("("):
        raise SqlRejected("only SELECT or WITH statements are allowed")
    bad = sorted({w for w, _ in words if w in FORBIDDEN})
    if bad:
        raise SqlRejected("not allowed: " + ", ".join(bad))
    depth = 0
    for ch in masked:
        depth += (ch == "(") - (ch == ")")
        if depth < 0:
            raise SqlRejected("unbalanced parentheses")
    if depth != 0:
        raise SqlRejected("unbalanced parentheses")
    if not any(w == "limit" and d == 0 for w, d in words):
        code = f"{code}\nLIMIT {limit}"
    return code
