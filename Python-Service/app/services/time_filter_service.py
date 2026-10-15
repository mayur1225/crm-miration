"""
Single source of truth for turning the UI's `migrationTimeFilter` payload
into a CRM-specific WHERE/COQL clause fragment, and for merging that clause
into an existing query string.

Both the live-preview path (crm_query_service.py) and every CRM's extraction
path (salesforce_migrator.py, zoho_migrator.py) import from here instead of
keeping their own copy of this logic in sync by hand. Any future change --
a new CRM, a different default field, inclusive/exclusive boundary handling,
timezone handling, or reintroducing a relative mode -- only has to be made
in this one file.

Only `mode: "range"` (explicit startDate/endDate) is supported. Relative
filtering (LAST_N_DAYS / LAST_N_MONTHS / LAST_N_YEARS) has been removed.
The range can be open-ended on the "To" side only: a `startDate` with no
`endDate` defaults the end to the caller's current local date; an `endDate`
with no `startDate` is rejected (see `_parse_date_range`).

Salesforce and Zoho both filter on an absolute instant (a datetime literal),
so their builders apply `utcOffsetMinutes` to convert the caller's local
calendar day into the correct UTC instant. Zendesk's Search API resolves
plain YYYY-MM-DD date literals using the account's own time zone (it isn't
given an instant at all), so `build_zendesk_time_clause` intentionally skips
the offset math -- shifting it would double-convert the boundary. HubSpot
needs the same absolute-instant treatment as Salesforce/Zoho, just expressed
as epoch milliseconds instead of a literal string.
"""
import re
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from app.services.sql_utils import split_query

DEFAULT_SALESFORCE_DATE_FIELD = "LastModifiedDate"
DEFAULT_ZOHO_DATE_FIELD = "Modified_Time"
DEFAULT_ZENDESK_DATE_FIELD = "updated"        # Zendesk Search API query token
DEFAULT_HUBSPOT_DATE_FIELD = "hs_lastmodifieddate"  # HubSpot property name

_DATE_FMT = "%Y-%m-%d"


class TimeFilterError(ValueError):
    """Raised when a migrationTimeFilter payload is present but malformed
    (e.g. only one of startDate/endDate supplied, unparseable dates, or
    startDate after endDate). Callers should surface this as a 400 to the
    user rather than let it fall through as a generic 500."""


def merge_time_clause(base_query: str, time_clause: str, where_kw: str = "WHERE", and_kw: str = "AND") -> str:
    """
    Safely merges a time_clause into a query that already starts with SELECT.
    Handles:
      - no WHERE at all             -> adds "WHERE <time_clause>"
      - WHERE with a real condition -> "WHERE <time_clause> AND (<condition>)"
        (the user's condition is parenthesised so `a = 1 OR b = 2` can't escape
        the date filter through AND/OR precedence)
      - a *dangling* WHERE with     -> just fills it in, no trailing AND/and
        nothing after it
    The query is split with the paren/quote-aware splitter in sql_utils, so:
      - a WHERE inside a sub-select is never mistaken for the outer WHERE,
      - GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET stay AFTER the WHERE,
      - keywords inside quoted values are ignored, and multi-line queries work.
    """
    if not time_clause:
        return base_query

    head, condition, tail = split_query(base_query)
    where = f"{time_clause} {and_kw} ({condition})" if condition else time_clause
    merged = f"{head} {where_kw} {where}"
    return f"{merged} {tail}".strip() if tail else merged.strip()


def _parse_date_range(
    time_filter: Optional[dict],
    default_field: str,
    apply_offset: bool = True,
) -> Optional[Tuple[str, datetime, datetime]]:
    """
    Extracts (field, start_dt, end_dt) from a migrationTimeFilter payload.

    Returns None when no filter should be applied (filter absent, or an
    unsupported/legacy `mode` such as the old 'relative' -- treated as
    "no filter" rather than an error so a stale cached frontend build
    degrades gracefully instead of hard-failing mid-rollout).

    The range can be open-ended on the "To" side: if only `startDate` is
    given, `endDate` defaults to the caller's current local date (using
    `utcOffsetMinutes`) -- "From Jan 1" quietly means "from Jan 1 through
    today" rather than forcing the user to type in today's date by hand.
    The reverse isn't allowed: `endDate` alone with no `startDate` raises,
    since "everything up to X" isn't a range this filter can express (every
    builder here ANDs two bounds together, so there's no sentinel meaning
    "unbounded start").

    `utcOffsetMinutes` (optional, defaults to 0/UTC) shifts the calendar-day
    boundaries so "Jan 1" means midnight in the caller's local timezone
    rather than midnight UTC -- otherwise a record modified in the last few
    hours of a local day can fall outside the range because the day
    boundary was computed in UTC. Positive values are ahead of UTC (e.g.
    India = +330). Pass `apply_offset=False` for a target (like Zendesk)
    whose API already resolves plain date literals in the account's own
    time zone -- applying the offset there would shift the boundary twice.
    The offset is still read (and validated) even when apply_offset=False,
    since it's also used to compute "today" for the open-ended-range default
    above, which should reflect the caller's local day regardless of how
    the target CRM consumes the resulting literal.

    Raises TimeFilterError when a range filter IS present but incomplete
    or invalid, so the caller can surface a real 400 instead of silently
    ignoring bad input.
    """
    if not time_filter:
        return None

    mode = time_filter.get("mode")
    if mode not in (None, "range"):
        return None

    start_raw = (time_filter.get("startDate") or "").strip()
    end_raw = (time_filter.get("endDate") or "").strip()

    if not start_raw and not end_raw:
        return None

    try:
        offset_minutes = int(time_filter.get("utcOffsetMinutes") or 0)
    except (TypeError, ValueError) as exc:
        raise TimeFilterError(f"migrationTimeFilter.utcOffsetMinutes must be an integer: {exc}") from exc

    if end_raw and not start_raw:
        raise TimeFilterError(
            "Select a 'From' date as well -- an end date on its own isn't enough to build a range."
        )

    if start_raw and not end_raw:
        local_now = datetime.utcnow() + timedelta(minutes=offset_minutes)
        end_raw = local_now.strftime(_DATE_FMT)

    try:
        start_dt = datetime.strptime(start_raw, _DATE_FMT)
        end_dt = datetime.strptime(end_raw, _DATE_FMT).replace(hour=23, minute=59, second=59)
    except ValueError as exc:
        raise TimeFilterError(f"migrationTimeFilter dates must be in YYYY-MM-DD format: {exc}") from exc

    if start_dt > end_dt:
        raise TimeFilterError("migrationTimeFilter.startDate must be on or before endDate.")

    if apply_offset:
        offset = timedelta(minutes=offset_minutes)
        start_dt -= offset
        end_dt -= offset

    field = (time_filter.get("field") or default_field).strip() or default_field
    return field, start_dt, end_dt


def _epoch_millis(dt: datetime) -> str:
    """dt is a naive datetime representing a UTC instant (see _parse_date_range
    with apply_offset=True). Computed manually rather than via dt.timestamp(),
    which would incorrectly interpret a naive datetime as the server's local
    time instead of UTC."""
    return str(int((dt - datetime(1970, 1, 1)).total_seconds() * 1000))


def build_salesforce_time_clause(time_filter: Optional[dict]) -> str:
    """Returns a SOQL boolean expression fragment (already parenthesized),
    or '' if no filter should be applied."""
    parsed = _parse_date_range(time_filter, DEFAULT_SALESFORCE_DATE_FIELD)
    if not parsed:
        return ""
    field, start_dt, end_dt = parsed
    start_lit = start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_lit = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return f"({field} >= {start_lit} AND {field} <= {end_lit})"


def build_zoho_time_clause(time_filter: Optional[dict]) -> str:
    """Returns a COQL boolean expression fragment (already parenthesized),
    or '' if no filter should be applied."""
    parsed = _parse_date_range(time_filter, DEFAULT_ZOHO_DATE_FIELD)
    if not parsed:
        return ""
    field, start_dt, end_dt = parsed
    start_lit = start_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    end_lit = end_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    return f"({field} >= '{start_lit}' and {field} <= '{end_lit}')"


def build_zendesk_time_clause(time_filter: Optional[dict]) -> str:
    """Returns a Zendesk Search API query fragment (e.g. "updated>=2024-01-01
    updated<=2024-01-31"), or '' if no filter should be applied. Space is an
    implicit AND in Zendesk's search syntax, so this can just be appended to
    an existing query string with a space. Date-only literals -- see the
    apply_offset note on _parse_date_range for why no UTC shift happens here.

    Only meaningful for STANDARD objects (tickets/users/organizations/...),
    which go through the Search API. Custom object records use a different,
    JSON-based filter -- see build_zendesk_custom_object_time_filter."""
    parsed = _parse_date_range(time_filter, DEFAULT_ZENDESK_DATE_FIELD, apply_offset=False)
    if not parsed:
        return ""
    field, start_dt, end_dt = parsed
    start_lit = start_dt.strftime("%Y-%m-%d")
    end_lit = end_dt.strftime("%Y-%m-%d")
    return f"{field}>={start_lit} {field}<={end_lit}"


def build_zendesk_custom_object_time_filter(time_filter: Optional[dict]) -> List[dict]:
    """Returns a list of Zendesk custom-object record-search filter clauses
    (field/operator/value triplets, to be AND-ed into the request body's
    `filter.and` array), or [] if no filter should be applied.

    NOTE: verify `greater_than`/`less_than` are the exact operator names your
    Zendesk API version expects for custom object record search before
    relying on this in production -- Zendesk's filter DSL for custom objects
    isn't uniform across every Zendesk product/version, and this hasn't been
    exercised against a live org.
    """
    parsed = _parse_date_range(time_filter, "created_at", apply_offset=False)
    if not parsed:
        return []
    field, start_dt, end_dt = parsed
    if not field.endswith("_at"):
        field = f"{field}_at"
    start_lit = start_dt.strftime("%Y-%m-%dT00:00:00Z")
    end_lit = end_dt.strftime("%Y-%m-%dT23:59:59Z")
    return [
        {"field": field, "operator": "greater_than", "value": start_lit},
        {"field": field, "operator": "less_than", "value": end_lit},
    ]


def build_hubspot_time_filters(time_filter: Optional[dict]) -> List[dict]:
    """Returns a list of HubSpot Search API filter clauses (GTE/LTE on the
    configured date property, values as epoch-millisecond strings), or []
    if no filter should be applied. Merge these into every existing
    filterGroup (HubSpot ORs across groups, ANDs within one) -- adding a new
    standalone group would OR the date range with everything else instead of
    intersecting it."""
    parsed = _parse_date_range(time_filter, DEFAULT_HUBSPOT_DATE_FIELD)
    if not parsed:
        return []
    field, start_dt, end_dt = parsed
    return [
        {"propertyName": field, "operator": "GTE", "value": _epoch_millis(start_dt)},
        {"propertyName": field, "operator": "LTE", "value": _epoch_millis(end_dt)},
    ]