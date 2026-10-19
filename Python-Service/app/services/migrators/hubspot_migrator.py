import asyncio
import json
import logging
import os
import re
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple

from app.services.crm_service import CrmService
from app.services.resilience.retry import ApiQuotaExhausted, CircuitOpen, OUTCOME_UNKNOWN
from app.services.time_filter_service import build_hubspot_time_filters, TimeFilterError

DEFAULT_DOMAIN = "https://api.hubapi.com"
SEARCH_PAGE_SIZE = 100
SEARCH_RESULT_CAP = 10_000        # Search API refuses paging past 10,000 results per query
BATCH_SIZE = 100                  # max inputs per batch create/update/upsert/archive call
MAX_RETRIES = 6
MAX_PROBES = 5
BATCH_CONCURRENCY = 3
SINGLE_CONCURRENCY = 5
FIND_CHUNK = 100                  # the Search API's IN operator accepts up to 100 values
SEARCH_SETTLE_SECONDS = float(os.environ.get("HUBSPOT_SEARCH_SETTLE_SECONDS", "8"))

_SINGULAR_TO_OBJECT = {"contact": "contacts", "company": "companies", "deal": "deals", "ticket": "tickets"}
_ID_FIELDS = {"id", "hs_object_id"}
_READ_ONLY_PROPS = _ID_FIELDS
_PROPAGATE = (ApiQuotaExhausted, CircuitOpen, asyncio.CancelledError)   # stop the run, never swallow

logger = logging.getLogger(__name__)


def chunk_dataset(data: list, chunk_size: int = 100):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def normalize_hubspot_object(name: str) -> str:
    """API name of an object. Maps common singular labels; custom objects
    (`p123_thing` / `2-12345` objectTypeId) pass through untouched."""
    n = (name or "").strip()
    return _SINGULAR_TO_OBJECT.get(n.lower(), n if re.match(r"^\d+-\d+$", n) else n.lower())


def _merge_hubspot_time_filters(filter_groups, time_filters: list) -> list:
    """ANDs time_filters into every existing filterGroup (HubSpot ORs across
    groups, ANDs within one), or creates a single group if none existed.
    Mirrors CrmQueryService._merge_hubspot_time_filters so live preview and
    actual extraction apply the identical date range."""
    if not filter_groups:
        return [{"filters": list(time_filters)}]
    return [
        {**group, "filters": list(group.get("filters", [])) + time_filters}
        for group in filter_groups
    ]


def _is_daily_limit(res) -> bool:
    try:
        return (res.json() or {}).get("policyName") == "DAILY"
    except Exception:
        return False


async def hubspot_search_all(
    post: Callable[[dict], Awaitable[Any]], base_payload: dict, send_log,
) -> AsyncIterator[List[dict]]:
    """Yields pages of Search API results. Shared by extraction and the file
    pre-check so both see every record.

    The Search API cannot page past 10,000 results. When no custom sort is
    requested this sorts by hs_object_id and restarts with `hs_object_id > last`
    after every 10,000 records (keyset pagination), so there is no ceiling. With a
    custom sort the cap can't be avoided safely; it stops and says so."""
    payload = dict(base_payload)
    payload["limit"] = SEARCH_PAGE_SIZE
    keyset = not payload.get("sorts")
    if keyset:
        payload["sorts"] = [{"propertyName": "hs_object_id", "direction": "ASCENDING"}]
    groups = base_payload.get("filterGroups")
    window = 0

    while True:
        res = await post(payload)
        if res.status_code != 200:
            raise RuntimeError(f"HubSpot search failed ({res.status_code}): {res.text[:300]}")
        body = res.json()
        results = body.get("results") or []
        if not results:
            return
        yield results
        window += len(results)

        after = ((body.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            return
        if window >= SEARCH_RESULT_CAP:
            if not keyset:
                await send_log(f"[HubSpot] Search API caps a sorted query at {SEARCH_RESULT_CAP:,} records; "
                               f"remove the custom 'sorts' to fetch everything. Stopping here.")
                return
            last_id = str(results[-1]["id"])
            payload = {**payload, "filterGroups": _merge_hubspot_time_filters(
                groups, [{"propertyName": "hs_object_id", "operator": "GT", "value": last_id}])}
            payload.pop("after", None)
            window = 0
        else:
            payload["after"] = after


class HubspotMigrator:

    # ------------------------------------------------------------------ http
    async def _request(self, client, method: str, url: str, headers: dict, user_id, role: str, send_log,
                       idempotent: bool = True, **kwargs):
        """One silent token refresh, Retry-After backoff on 429 / 5xx. `headers`
        is mutated on refresh so later calls in the same run reuse the new token.
        Returns the last response; callers decide what a non-2xx means."""
        refreshed = False
        res = None
        for attempt in range(MAX_RETRIES + 1):
            res = await client.request(method, url, headers=headers, **kwargs)
            if res.status_code == 401 and not refreshed:
                await send_log(f"HubSpot {role} token expired. Silently refreshing...")
                token = await CrmService.refresh_crm_token(user_id, "hubspot", role)
                headers["Authorization"] = f"Bearer {token}"
                refreshed = True
                continue
            if res.status_code in (429, 503) or (res.status_code >= 500 and idempotent):
                if res.status_code == 429 and _is_daily_limit(res):
                    return res
                try:
                    wait = int(res.headers.get("Retry-After") or 0)
                except ValueError:
                    wait = 0
                wait = min(wait or 2 ** (attempt + 1), 60)
                await send_log(f" [HubSpot {'Rate Limit' if res.status_code == 429 else 'Server Error'}] "
                               f"Pausing {wait}s...")
                await asyncio.sleep(wait)
                continue
            return res
        return res

    # ==========================================
    # EXTRACT (Pull from HubSpot)
    # ==========================================
    async def extract(self, client, creds, obj_name, query, mappings, send_log, time_filter=None):
        token = creds.get("access_token")
        user_id = creds.get("user_id")
        domain = (creds.get("api_domain") or DEFAULT_DOMAIN).rstrip("/")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        obj = normalize_hubspot_object(obj_name)
        source_records: List[dict] = []

        properties = ["hs_object_id"]
        for mapping in mappings:
            source_field = mapping.get("sourceField") or mapping.get("csvField")
            if source_field and source_field not in properties:
                properties.append(source_field)

        try:
            time_filters = build_hubspot_time_filters(time_filter)
        except TimeFilterError as e:
            await send_log(f"[{obj_name}] Invalid migration filter: {e}")
            raise

        try:
            url = f"{domain}/crm/v3/objects/{obj}/search"
            payload: Dict[str, Any] = {"limit": SEARCH_PAGE_SIZE, "properties": properties[:100]}

            if query and query.strip():
                try:
                    query_dict = json.loads(query)
                    if "filterGroups" in query_dict:
                        payload["filterGroups"] = query_dict["filterGroups"]
                    if "sorts" in query_dict:
                        payload["sorts"] = query_dict["sorts"]
                except json.JSONDecodeError:
                    await send_log(" [HubSpot Extraction] Invalid query format. Ignoring.")

            if time_filters:
                payload["filterGroups"] = _merge_hubspot_time_filters(payload.get("filterGroups"), time_filters)

            effective_query = json.dumps({k: v for k, v in payload.items() if k != "after"}, sort_keys=True)

            async def post(p: dict):
                return await self._request(client, "POST", url, headers, user_id, "source", send_log, json=p)

            next_log = 1000
            async for page in hubspot_search_all(post, payload, send_log):
                for rec in page:
                    flat_rec = {"id": rec.get("id")}
                    for k, v in (rec.get("properties") or {}).items():
                        if isinstance(v, dict):
                            flat_rec[k] = str(v)
                        elif isinstance(v, list):
                            flat_rec[k] = ";".join(str(i) for i in v)
                        else:
                            flat_rec[k] = v
                    source_records.append(flat_rec)
                if len(source_records) >= next_log:
                    await send_log(f"[{obj_name}] Extracted {len(source_records)} records...")
                    next_log = (len(source_records) // 1000 + 1) * 1000

            await send_log(f"[{obj_name}] Extraction Complete! Total: {len(source_records)}")
            return source_records, effective_query

        except Exception as e:
            await send_log(f"[{obj_name}] Extract Failed: {str(e)}")
            raise

    # ==========================================
    # UPLOAD (Push to HubSpot)
    # ==========================================
    @staticmethod
    def _clean_properties(rec: dict) -> dict:
        """Accepts either a flat property dict or HubSpot's {"properties": {...}} shape."""
        props = rec.get("properties") if isinstance(rec.get("properties"), dict) else rec
        out: Dict[str, Any] = {}
        for k, v in props.items():
            if k in _READ_ONLY_PROPS or v is None or v == "":
                continue
            if isinstance(v, bool):
                v = "true" if v else "false"
            elif isinstance(v, (list, tuple, set)):
                v = ";".join(str(i) for i in v)
            elif isinstance(v, dict):
                v = json.dumps(v)
            elif not isinstance(v, (str, int, float)):
                v = str(v)
            out[k] = v
        return out

    @staticmethod
    def _fmt_error(err: Optional[dict]) -> str:
        if not err:
            return "HubSpot returned no result for this record."
        cat, msg = err.get("category") or "", err.get("message") or json.dumps(err)[:300]
        return f"[{cat}] {msg}"[:500] if cat else msg[:500]

    def _fmt_http(self, res) -> str:
        try:
            body = res.json()
            errs = body.get("errors")
            return self._fmt_error(errs[0] if errs else body)
        except Exception:
            return f"({res.status_code}) {res.text[:300]}"

    async def upload(self, client, payload, op_mode, pass_name, options, send_log):
        """Returns (success, error, skipped, success_rows, error_rows, skipped_rows) -- the
        same 6-tuple contract as the Salesforce/Zoho migrators. Every success row carries
        Target_Id (the HubSpot record id) so the file pass can attach files to it."""
        if not payload:
            return 0, 0, 0, [], [], []

        obj = normalize_hubspot_object(options["targetObject"])
        ext = (options.get("targetExtIdField") or "").strip()
        source_records = options["sourceRecords"]
        user_id = options.get("userId")
        base = (options.get("instance_url") or DEFAULT_DOMAIN).rstrip("/")
        if not base.startswith("http"):
            base = f"https://{base}"
        headers = {"Authorization": f"Bearer {options['token']}", "Content-Type": "application/json"}
        objects_url = f"{base}/crm/v3/objects/{obj}"

        succ: List[dict] = []
        errs: List[dict] = []
        skips: List[dict] = []

        decided: set = set()   

        def ok(it, hs_id):
            rec = source_records[it["idx"]]
            rec["Target_Id"] = str(hs_id)
            decided.add(it["idx"])
            succ.append(rec)

        def fail(it, msg):
            rec = source_records[it["idx"]]
            rec["Target_Error"] = f"HubSpot Error: {msg}"
            decided.add(it["idx"])
            errs.append(rec)

        def skip(it, msg):
            rec = source_records[it["idx"]]
            rec["Target_SkipReason"] = msg
            decided.add(it["idx"])
            skips.append(rec)

        def unknown(it, msg):
            """INSERT whose outcome is ambiguous: the record may exist. The runner looks it up (or keeps it as an
            unconfirmed failure) instead of re-sending blindly."""
            rec = source_records[it["idx"]]
            rec["Target_Error"] = f"{OUTCOME_UNKNOWN}HubSpot did not confirm this write ({msg}); the record may already exist."
            decided.add(it["idx"])
            errs.append(rec)

        def fail_http(mode, it, res):
            msg = self._fmt_http(res)
            if mode == "insert" and res.status_code >= 500 and res.status_code != 503:
                unknown(it, msg)
            else:
                fail(it, msg)

        def settle_exception(mode, chunk, exc):
            """A network error mid-request. Rows that already have an answer keep it; the rest are 'unknown' for an
            INSERT, and plainly retryable for the replay-safe modes."""
            detail = f"{type(exc).__name__}: {exc}"[:200]
            for c in chunk:
                if c["idx"] in decided:
                    continue
                if mode == "insert":
                    unknown(c, detail)
                else:
                    fail(c, f"Temporary network failure, safe to retry ({detail})")

        async def guarded(mode, chunk, coro):
            try:
                await coro
            except _PROPAGATE:
                raise
            except Exception as exc:
                settle_exception(mode, chunk, exc)

        async def gather_all(coros):
            """Let every sibling finish (no orphaned tasks writing after we return); then re-raise a stop signal."""
            for r in await asyncio.gather(*coros, return_exceptions=True):
                if isinstance(r, BaseException):
                    raise r

        def result():
            return len(succ), len(errs), len(skips), succ, errs, skips

        needs_match = op_mode in ("update", "upsert")
        if needs_match and not ext:
            await send_log(f"[{obj}] {pass_name}: No unique/external ID field configured -- "
                           f"cannot match existing records for {op_mode.upper()}.")
            return 0, len(payload), 0, [], [source_records[e["originalIndex"]] for e in payload], []

        await send_log(f"[{obj}] {pass_name}: Initializing {op_mode.upper()} to HubSpot...")

        # ---- normalise payload rows
        items: List[dict] = []
        for entry in payload:
            raw = entry["targetRecord"]
            container = raw.get("properties") if isinstance(raw.get("properties"), dict) else raw
            props = self._clean_properties(raw)
            if op_mode == "delete" or ext.lower() in _ID_FIELDS:
                ext_val = (raw.get("id") or raw.get("Id") or raw.get("hs_object_id")
                           or container.get("hs_object_id") or container.get("id"))
            else:
                ext_val = props.get(ext) if ext else None
            items.append({"idx": entry["originalIndex"], "trace": str(entry["originalIndex"]),
                          "props": props, "ext_val": None if ext_val in (None, "") else str(ext_val)})

        if op_mode != "delete":
            writable = []
            for it in items:
                if it["props"]:
                    writable.append(it)
                else:
                    skip(it, "No writable properties for HubSpot in this row (reference-only pass; "
                             "HubSpot associations are not migrated).")
            items = writable

        async def req(method, url, idempotent=True, **kw):
            return await self._request(client, method, url, headers, user_id, "target", send_log, idempotent=idempotent, **kw)

        # ---- delete
        if op_mode == "delete":
            for chunk in chunk_dataset(items, BATCH_SIZE):
                targets = [c for c in chunk if c["ext_val"]]
                for c in chunk:
                    if not c["ext_val"]:
                        fail(c, "No record id to delete.")
                if not targets:
                    continue
                try:
                    res = await req("POST", f"{objects_url}/batch/archive",
                                    json={"inputs": [{"id": c["ext_val"]} for c in targets]})
                except _PROPAGATE:
                    raise
                except Exception as exc:
                    settle_exception("delete", targets, exc)
                    continue
                for c in targets:
                    (ok(c, c["ext_val"]) if res.status_code in (200, 202, 204) else fail(c, self._fmt_http(res)))
            return result()

        def wire_input(mode, it):
            inp: Dict[str, Any] = {"properties": it["props"], "objectWriteTraceId": it["trace"]}
            if mode in ("update", "upsert"):
                inp["id"] = it["ext_val"]
                if ext.lower() not in _ID_FIELDS:
                    inp["idProperty"] = ext
            return inp

        def parse_batch(body: dict, chunk: List[dict]):
            """Attribute a batch response to records via objectWriteTraceId -- never by position."""
            by_trace = {c["trace"]: c for c in chunk}
            good: Dict[str, str] = {}
            for r in body.get("results") or []:
                t = str(r.get("objectWriteTraceId") or "")
                if t in by_trace:
                    good[t] = str(r.get("id"))
            errors = body.get("errors") or []
            err_for: Dict[str, dict] = {}
            for e in errors:
                tr = (e.get("context") or {}).get("objectWriteTraceId")
                for t in (tr if isinstance(tr, list) else [tr] if tr else []):
                    err_for[str(t)] = e
            bad = {t: self._fmt_error(err_for.get(t) or (errors[0] if errors else None))
                   for t in by_trace if t not in good}
            return good, bad

        async def send_batch(mode, chunk):
            endpoint = {"insert": "create", "update": "update", "upsert": "upsert"}[mode]
            return await req("POST", f"{objects_url}/batch/{endpoint}", idempotent=(mode != "insert"),
                             json={"inputs": [wire_input(mode, c) for c in chunk]})

        def settle_missing(mode, it, msg):
            if mode == "update" and "OBJECT_NOT_FOUND" in msg:
                skip(it, f"[{ext}] No matching record found in HubSpot. "
                         f"Skipped because Update mode does not create new records.")
            else:
                fail(it, msg)

        async def write_single(mode, it, sem):
            """Exact attribution (one request, one record). Used for the probe and as the
            safe path when the API doesn't echo objectWriteTraceId."""
            async with sem:
                if mode == "insert":
                    res = await req("POST", objects_url, idempotent=False, json={"properties": it["props"]})
                    (ok(it, res.json().get("id")) if res.status_code in (200, 201) else fail_http(mode, it, res))
                    return
                res = await send_batch(mode, [it])
                if res.status_code not in (200, 201, 207):
                    return fail_http(mode, it, res)
                body = res.json()
                if body.get("results"):
                    return ok(it, body["results"][0].get("id"))
                errors = body.get("errors") or []
                settle_missing(mode, it, self._fmt_error(errors[0] if errors else None))

        trace_ok: Optional[bool] = None

        async def run_group(mode, group):
            nonlocal trace_ok
            remaining = list(group)
            single_sem = asyncio.Semaphore(SINGLE_CONCURRENCY)

            
            probes = 0
            while trace_ok is None and remaining and probes < MAX_PROBES:
                it = remaining.pop(0)
                probes += 1
                try:
                    res = await send_batch(mode, [it])
                except _PROPAGATE:
                    raise
                except Exception as exc:
                    settle_exception(mode, [it], exc)
                    continue
                if res.status_code not in (200, 201, 207):
                    fail_http(mode, it, res)
                    continue
                body = res.json()
                results = body.get("results") or []
                if results:
                    ok(it, results[0].get("id"))
                    trace_ok = bool(results[0].get("objectWriteTraceId"))
                else:
                    errors = body.get("errors") or []
                    settle_missing(mode, it, self._fmt_error(errors[0] if errors else None))
            if trace_ok is None and remaining:
                trace_ok = False
            if not remaining:
                return

            if not trace_ok:   # can't attribute batch results safely -> one request per record
                await send_log(f"[{obj}] {pass_name}: batch results can't be matched to records; "
                               f"writing {len(remaining)} record(s) individually.")
                await gather_all([guarded(mode, [it], write_single(mode, it, single_sem)) for it in remaining])
                return

            batch_sem = asyncio.Semaphore(BATCH_CONCURRENCY)

            async def do_chunk(chunk):
                async with batch_sem:
                    res = await send_batch(mode, chunk)
                    if res.status_code in (200, 201, 207):
                        good, bad = parse_batch(res.json(), chunk)
                        for c in chunk:
                            if c["trace"] in good:
                                ok(c, good[c["trace"]])
                            else:
                                settle_missing(mode, c, bad[c["trace"]])
                    elif len(chunk) > 1 and res.status_code < 500 and res.status_code != 429:
                        # whole batch rejected (nothing committed): isolate the offending rows
                        await gather_all([guarded(mode, [c], write_single(mode, c, single_sem)) for c in chunk])
                    else:
                        msg = self._fmt_http(res)
                        ambiguous = mode == "insert" and res.status_code >= 500 and res.status_code != 503
                        for c in chunk:
                            (unknown if ambiguous else fail)(c, msg)

            await gather_all([guarded(mode, c, do_chunk(c)) for c in chunk_dataset(remaining, BATCH_SIZE)])

        # ---- group rows by wire mode
        groups: Dict[str, List[dict]] = {"insert": [], "update": [], "upsert": []}
        for it in items:
            if op_mode == "insert":
                groups["insert"].append(it)
            elif op_mode == "update":
                if it["ext_val"]:
                    groups["update"].append(it)
                else:
                    fail(it, f"No value for match field '{ext}'.")
            else:  # upsert; rows without a key can only be created
                groups["upsert" if it["ext_val"] else "insert"].append(it)

        for mode in ("insert", "update", "upsert"):
            await run_group(mode, groups[mode])

        return result()

    # ==========================================
    # RECONCILE (used by an interrupted INSERT on resume)
    # ==========================================
    async def find_existing(self, client, options, key_field, values):
        """
        {correlation value: HubSpot id} for records that already carry one of `values` in the custom property
        `key_field` (the property the runner stamps with the source key on every INSERT). This is what lets Resume /
        Retry tell "created but not recorded" from "never created", so an interrupted INSERT creates no duplicates.

        Needs that custom property to exist on the HubSpot object. The oldest record wins when a key matches several.
        """
        obj = normalize_hubspot_object(options["targetObject"])
        base = (options.get("instance_url") or DEFAULT_DOMAIN).rstrip("/")
        if not base.startswith("http"):
            base = f"https://{base}"
        headers = {"Authorization": f"Bearer {options['token']}", "Content-Type": "application/json"}
        url = f"{base}/crm/v3/objects/{obj}/search"

        async def log(msg):
            logger.info("[HubSpot find_existing] %s", msg)

        wanted = {str(v).lower(): str(v) for v in dict.fromkeys(values) if v not in (None, "")}
        found: Dict[str, str] = {}
        if not wanted:
            return found
        if SEARCH_SETTLE_SECONDS > 0:
            await asyncio.sleep(SEARCH_SETTLE_SECONDS)

        for chunk in chunk_dataset(list(wanted), FIND_CHUNK):   # lower-cased: Search compares text case-insensitively
            payload: Dict[str, Any] = {
                "filterGroups": [{"filters": [{"propertyName": key_field, "operator": "IN", "values": chunk}]}],
                "properties": [key_field],
                "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}],
                "limit": SEARCH_PAGE_SIZE,
            }
            after = None
            while True:
                body_payload = dict(payload, after=after) if after else payload
                res = await self._request(client, "POST", url, headers, options.get("userId"), "target", log, json=body_payload)
                if res.status_code != 200:
                    raise RuntimeError(f"HubSpot lookup failed ({res.status_code}): {res.text[:300]}")
                body = res.json()
                for rec in body.get("results") or []:
                    key = (rec.get("properties") or {}).get(key_field)
                    original = wanted.get(str(key).lower()) if key not in (None, "") else None
                    if original is not None:
                        found.setdefault(original, str(rec["id"]))   # ascending ids: the first one is the oldest
                after = ((body.get("paging") or {}).get("next") or {}).get("after")
                if not after:
                    break
        return found