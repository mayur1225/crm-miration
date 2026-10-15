"""
Shared, paren/quote-aware helpers for SOQL and COQL text.

Why this exists: several places (time-filter merge, count queries, "make sure
mapped fields are selected") used to find FROM / WHERE / LIMIT with plain regex
searches over the whole string. That breaks when the keyword shows up inside a
sub-select (`SELECT Id, (SELECT Id FROM Contacts WHERE ...) FROM Account`), inside
a quoted literal, or when the query is written over several lines.

Everything here works on a "masked" copy of the query in which quoted literals and
anything nested inside parentheses is blanked out (same length, so indexes line up
with the original text). Keyword searches run on the mask; slicing runs on the
original.
"""
import re
from typing import List, Optional, Tuple

_QUOTED_RE = re.compile(r"'(?:[^'\\]|\\.)*'")
_TAIL_RE = re.compile(r"(?i)\b(?:group\s+by|order\s+by|having|limit|offset)\b")
_WHERE_RE = re.compile(r"(?i)\bwhere\b")
_FROM_RE = re.compile(r"(?i)\bfrom\b")


def mask_nested(sql: str) -> str:
    """Same-length copy of `sql` where quoted literals and everything inside
    parentheses (below the top level) is replaced by '_'."""
    masked = _QUOTED_RE.sub(lambda m: "'" + "_" * (len(m.group(0)) - 2) + "'", sql)
    out: List[str] = []
    depth = 0
    for ch in masked:
        if ch == "(":
            out.append("(" if depth == 0 else "_")
            depth += 1
        elif ch == ")":
            depth = max(depth - 1, 0)
            out.append(")" if depth == 0 else "_")
        else:
            out.append(ch if depth == 0 else "_")
    return "".join(out)


def find_top_level_from(sql: str) -> Optional[int]:
    """Index of the FROM that belongs to the outer SELECT (not a sub-select's)."""
    match = _FROM_RE.search(mask_nested(sql))
    return match.start() if match else None


def split_top_level_commas(select_list: str) -> List[str]:
    """Splits a SELECT list on commas that are not inside parentheses/quotes."""
    masked = mask_nested(select_list)
    parts: List[str] = []
    last = 0
    for i, ch in enumerate(masked):
        if ch == ",":
            parts.append(select_list[last:i])
            last = i + 1
    parts.append(select_list[last:])
    return parts


def split_query(query: str) -> Tuple[str, str, str]:
    """Splits a SELECT into (head, where_condition, tail).
      head = 'SELECT ... FROM X [alias]'
      tail = 'GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET ...' (may be empty)
    Works for multi-line queries, sub-selects and keywords inside literals."""
    q = (query or "").strip().rstrip(";").strip()
    masked = mask_nested(q)

    from_idx = _FROM_RE.search(masked)
    search_from = from_idx.start() if from_idx else 0
    tail_match = _TAIL_RE.search(masked, search_from)
    cut = tail_match.start() if tail_match else len(q)

    body, tail = q[:cut].rstrip(), q[cut:].strip()
    where_match = _WHERE_RE.search(masked[:cut], search_from)
    if where_match:
        return body[:where_match.start()].rstrip(), body[where_match.end():].strip(), tail
    return body, "", tail


def has_limit(query: str) -> bool:
    return bool(re.search(r"(?i)\blimit\b", split_query(query)[2]))


def replace_select_list(head: str, select_expr: str) -> str:
    """'SELECT a, b FROM X' -> 'SELECT <select_expr> FROM X' (top-level FROM only)."""
    idx = find_top_level_from(head)
    if idx is None:
        return head
    return f"SELECT {select_expr} {head[idx:]}"


def build_count_query(
    query: str,
    count_expr: str,
    time_clause: str = "",
    where_kw: str = "WHERE",
    and_kw: str = "AND",
    default_where: str = "",
) -> str:
    """Turns any SELECT into a count query: swaps the select list, keeps the user's
    condition (parenthesised when combined), drops ORDER BY/LIMIT/OFFSET."""
    head, cond, _tail = split_query(query)
    head = replace_select_list(head, count_expr)
    parts: List[str] = []
    if cond:
        parts.append(f"({cond})" if time_clause else cond)
    if time_clause:
        parts.append(time_clause)
    where = f" {and_kw} ".join(parts) or default_where
    return f"{head} {where_kw} {where}" if where else head