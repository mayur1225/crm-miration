"""
Guarantees a SELECT query actually returns every field the UI has mapped,
even when the query text itself only lists a subset of them (hand-typed, or
auto-generated from just a couple of sample fields).

Without this, a field that's mapped in the UI but NOT in the query's own
SELECT list comes back completely empty in the extracted/previewed data --
silently, with no error, because the CRM was simply never asked for it. The
query editor is meant to control filtering (WHERE) and which extra columns
to eyeball; it was never meant to be able to silently cap what data actually
ends up in the migration. So the SELECT list a user (or the frontend's
auto-generated default query) writes is treated as a MINIMUM they can
narrow the WHERE clause around, not a ceiling on what gets fetched.

Both the live-preview path (crm_query_service.py) and every CRM's
extraction path (salesforce_migrator.py, zoho_migrator.py) call into this
so the preview table and the actual migration always agree on what data
shows up for a given set of mappings.
"""
import re
from typing import List

from app.services.sql_utils import find_top_level_from, split_top_level_commas


def ensure_fields_selected(select_query: str, required_fields: List[str]) -> str:
    """
    Given a full "SELECT <cols> FROM ..." query (SOQL or COQL -- both use
    the same comma-separated-list-before-FROM shape), unions
    `required_fields` into the SELECT list for any that aren't already
    present (case-insensitive, whitespace-trimmed comparison). Leaves
    FROM/WHERE/LIMIT and everything else untouched.

    The FROM that ends the select list is the TOP-LEVEL one, and the list is split
    on top-level commas only, so a sub-select such as
    `SELECT Id, (SELECT Id FROM Contacts) FROM Account` is treated as one column
    and never mistaken for the end of the list.

    No-ops (returns the query unchanged) if:
      - `required_fields` is empty -- nothing to guarantee
      - `select_query` doesn't actually start with SELECT
      - it's already a wildcard `SELECT *` -- the caller is expected to
        have already expanded `*` into a real field list before calling
        this, since a wildcard already implies "everything mapped"
      - there is no top-level FROM
    """
    if not required_fields:
        return select_query
    select_match = re.match(r'(?i)^\s*select\s+', select_query)
    if not select_match:
        return select_query
    if re.match(r'(?i)^\s*select\s+\*\s', select_query):
        return select_query

    from_idx = find_top_level_from(select_query)
    if from_idx is None:
        return select_query

    select_list = select_query[select_match.end():from_idx]
    existing = {f.strip().lower() for f in split_top_level_commas(select_list) if f.strip()}
    missing = [f for f in required_fields if f.strip().lower() not in existing]
    if not missing:
        return select_query

    return f"{select_query[:select_match.end()]}{', '.join(missing)}, {select_query[select_match.end():]}"