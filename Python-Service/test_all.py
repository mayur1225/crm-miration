"""
SureShift - ONE test file for every fix and scenario (round 1 + round 2).

Run from the project root (the folder that contains the `app/` package):

    python test_all.py

Nothing talks to a real CRM, database or network. Modules that need FastAPI / HTTPX / Supabase / your settings are
replaced by tiny stubs; the code under test (payload_builder, store, resumable_runner, hubspot_migrator,
zendesk_migrator, and the _delete_records of the Salesforce / Zoho migrators) is YOUR real, patched code.

Exit code 0 = everything passed, 1 = at least one check failed (all failures are listed at the end).
"""
import ast, asyncio, glob, importlib.util, json, os, sys, tempfile, textwrap, time, traceback, types
from contextlib import contextmanager

sys.path.insert(0, os.getcwd())
os.environ["SURESHIFT_RUNS_DIR"] = tempfile.mkdtemp()   # run databases go to a throw-away folder


# =====================================================================================================================
# stubs (only for things that need a server / credentials)
# =====================================================================================================================
def mod(name, **attrs):
    m = types.ModuleType(name); m.__dict__.update(attrs); sys.modules[name] = m; return m


def importable(name):
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False


class HTTPException(Exception):
    def __init__(self, status_code=500, detail=None):
        super().__init__(detail); self.status_code, self.detail = status_code, detail


class WebSocketDisconnect(Exception): ...
class ApiQuotaExhausted(Exception): ...
class CircuitOpen(Exception): ...


@contextmanager
def idempotent_posts(enabled=True):
    yield


OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN: "


class CrmService:
    refreshed = 0

    @staticmethod
    async def refresh_crm_token(user_id, crm, role):
        CrmService.refreshed += 1
        return "NEWTOKEN"


def crm_session_expired(crm, side):
    return HTTPException(401, {"code": "CRM_SESSION_EXPIRED", "crm": crm, "side": side})


if not importable("fastapi"):
    mod("fastapi", HTTPException=HTTPException)
if not importable("starlette"):
    mod("starlette"); mod("starlette.websockets", WebSocketDisconnect=WebSocketDisconnect)
else:
    from starlette.websockets import WebSocketDisconnect  # noqa: F811  (use the real one if installed)
if "fastapi" in sys.modules and getattr(sys.modules["fastapi"], "HTTPException", None) is not HTTPException:
    from fastapi import HTTPException as _RealHTTP   # real FastAPI present: use its exception class everywhere
    HTTPException = _RealHTTP

    def crm_session_expired(crm, side):  # noqa: F811
        return HTTPException(status_code=401, detail={"code": "CRM_SESSION_EXPIRED", "crm": crm, "side": side})

mod("app.services.crm_service", is_crm_session_expired=lambda e: False, CrmService=CrmService,
    crm_session_expired=crm_session_expired)
mod("app.services.resilience.retry", ApiQuotaExhausted=ApiQuotaExhausted, CircuitOpen=CircuitOpen,
    idempotent_posts=idempotent_posts, OUTCOME_UNKNOWN=OUTCOME_UNKNOWN)
mod("app.services.time_filter_service", build_hubspot_time_filters=lambda f: [], TimeFilterError=Exception,
    build_zendesk_time_clause=lambda f: "", build_zendesk_custom_object_time_filter=lambda f: [])

# the stub constant must equal the real one
_real_retry = glob.glob("**/resilience/retry.py", recursive=True)
if _real_retry:
    assert f'OUTCOME_UNKNOWN = "{OUTCOME_UNKNOWN}"' in open(_real_retry[0], encoding="utf-8").read(), \
        "retry.py is missing the OUTCOME_UNKNOWN marker (round 2 not applied?)"

from app.services.payload_builder import PayloadBuilderService as P                       # noqa: E402
from app.services import resumable_runner as rr                                           # noqa: E402
from app.services.store import RunStore, RowResult                                        # noqa: E402
from app.services.resumable_runner import ResumableRunner, PassSpec, DiskSpaceLow, CheckpointWriteFailed  # noqa: E402
from app.services.migrators import hubspot_migrator as hm                                 # noqa: E402
from app.services.migrators import zendesk_migrator as zm                                 # noqa: E402

import sqlite3                                                                            # noqa: E402

# =====================================================================================================================
# tiny test framework
# =====================================================================================================================
PASSED, FAILED, SECTIONS = 0, [], []
_current = {"name": ""}


def check(name, cond):
    global PASSED
    if cond:
        PASSED += 1
        print(f"    ok   {name}")
    else:
        FAILED.append(f"[{_current['name']}] {name}")
        print(f"    FAIL {name}")


def section(title):
    _current["name"] = title
    SECTIONS.append(title)
    print(f"\n== {title}")


async def fast_sleep(_):      # no real waiting inside retry/backoff loops
    return None


asyncio.sleep = fast_sleep
hm.SEARCH_SETTLE_SECONDS = 0
LOGS = []


async def send_log(msg, status="Running"):
    LOGS.append(msg)


def new_store(queue=None):
    return RunStore.create("user1", {"x": 1}, queue or [{"targetObject": "Account", "mappings": [], "operationMode": "insert"}])


class Resp:
    def __init__(self, code, body=None, headers=None):
        self.status_code, self._b, self.headers, self.text = code, body if body is not None else {}, headers or {}, json.dumps(body or {})

    def json(self):
        return self._b

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeMigrator:
    """Generic adapter that always succeeds; records are mutated with Target_Id like the real adapters do."""

    def __init__(self):
        self.calls = 0

    async def upload(self, client, items, op_mode, pass_name, options, send_log):
        self.calls += 1
        recs = options["sourceRecords"]
        succ = []
        for it in items:
            r = recs[it["originalIndex"]]
            r["Target_Id"] = f"T{it['originalIndex']}"
            succ.append(r)
        return len(succ), 0, 0, succ, [], []


def make_spec(n=3, mig=None, precheck=None, op="insert", corr=""):
    recs = [{"Id": f"S{i}", "Name": f"n{i}"} for i in range(n)]
    payload = [{"originalIndex": i, "targetRecord": {"Name": f"n{i}"}} for i in range(n)]
    return PassSpec(job_idx=0, pass_no=1, pass_name="Standard Sync", op_mode=op, target_object="Account", ext_id_field="",
                    segment_size=2, payload=payload, source_records=recs, migrator=mig or FakeMigrator(),
                    options_base={}, precheck_errors=precheck or [], correlation_field=corr)


def runner(st):
    return ResumableRunner(st, None, send_log, lambda: False)


# =====================================================================================================================
# 1. PAYLOAD BUILDER  (Pass 2 / Pass 3, sanitizer)
# =====================================================================================================================
def test_payload_builder():
    section("1a. Payload builder - self-reference Pass 2 keeps the match key")
    maps = [{"sourceField": "Name", "targetField": "Name", "type": "string"},
            {"sourceField": "Id", "targetField": "Ext__c", "type": "string"},
            {"sourceField": "ParentId", "targetField": "ParentId", "type": "reference", "referenceTo": ["Account"]}]
    rows = [{"Name": "A", "Id": "001A", "ParentId": "0019999"}, {"Name": "B", "Id": "001B", "ParentId": None}]
    o2 = {"targetObject": "Account", "targetExtIdField": "Ext__c", "operationMode": "upsert", "onlySelfReferencing": True}
    p = P.build_payload(rows, maps, o2)
    check("plain reference: row with a parent is sent with its key",
          p == [{"originalIndex": 0, "targetRecord": {"ParentId": "0019999", "Ext__c": "001A"}}])
    m2 = [dict(x) for x in maps]; m2[2]["relationalExtIdField"] = "Id"
    p = P.build_payload(rows, m2, o2)
    check("relationalExtIdField=Id: key populated, rows without a parent are not sent", len(p) == 1 and p[0]["targetRecord"]["Ext__c"] == "001A")
    errs = []
    P.build_payload([{"Name": "C", "Id": None, "ParentId": "0019"}], maps, o2, errors=errs)
    check("patch row with no match key is reported, not sent", len(errs) == 1 and "Missing value" in errs[0]["message"])
    p1 = P.build_payload(rows, maps, {"targetObject": "Account", "targetExtIdField": "Ext__c", "operationMode": "insert", "skipSelfReferencing": True})
    check("pass 1 is unchanged (no ParentId)", all("ParentId" not in x["targetRecord"] for x in p1) and p1[0]["targetRecord"]["Ext__c"] == "001A")
    zmaps = [{"sourceField": "Id", "targetField": "external_id", "type": "string"},
             {"sourceField": "Parent", "targetField": "parent_id", "type": "reference", "referenceTo": ["Organization"]}]
    zp = P.build_payload([{"Id": "9", "Parent": None}, {"Id": "8", "Parent": "7"}], zmaps,
                         {"targetObject": "Organization", "targetExtIdField": "external_id", "operationMode": "upsert", "onlySelfReferencing": True}, "zendesk")
    check("zendesk patch pass: only rows with a parent, external_id kept", len(zp) == 1 and zp[0]["targetRecord"]["external_id"] == "8")

    section("1b. Payload builder - sanitizer and type enforcement")
    smaps = [{"sourceField": "v", "targetField": "Version__c", "type": "string"},
             {"sourceField": "l", "targetField": "LastName", "type": "string"},
             {"sourceField": "n", "targetField": "Num__c", "type": "number"},
             {"sourceField": "f", "targetField": "Flag__c", "type": "boolean"},
             {"sourceField": "ph", "targetField": "Phone", "type": "phone"}]
    opt = {"targetObject": "Contact", "operationMode": "insert"}
    good = P.build_payload([{"v": "3.0", "l": "Nan", "n": "12.50", "f": "yes", "ph": "4155551234.0"}], smaps, opt)[0]["targetRecord"]
    check("string '3.0' stays '3.0'", good["Version__c"] == "3.0")
    check("surname 'Nan' is kept", good["LastName"] == "Nan")
    check("number / boolean / phone are coerced", good["Num__c"] == 12.5 and good["Flag__c"] is True and good["Phone"] == "4155551234")
    check("pandas artifact 'nan' still becomes null", "LastName" not in P.build_payload([{"v": "x", "l": "nan"}], smaps, opt)[0]["targetRecord"])
    check("custom nullStrings are honoured", "LastName" not in P.build_payload([{"v": "x", "l": "NULL"}], smaps, {**opt, "nullStrings": ["NULL"]})[0]["targetRecord"])
    errs = []
    out = P.build_payload([{"v": "x", "f": "maybe"}, {"v": "y", "n": "$1,200"}, {"v": "z", "n": "inf"}, {"v": "ok"}], smaps, opt, errors=errs)
    check("bad boolean / currency text / inf rows are reported with a reason",
          [e["originalIndex"] for e in errs] == [0, 1, 2] and "Flag__c" in errs[0]["message"])
    check("bad rows are not sent, the good row is", [x["originalIndex"] for x in out] == [3])
    legacy = P.build_payload([{"v": "x", "f": "maybe"}], smaps, opt)
    check("legacy callers (no errors list): value omitted, never coerced to True", "Flag__c" not in legacy[0]["targetRecord"])


# =====================================================================================================================
# 2. STORE: locks, two tabs, crash takeover, edit lock, frozen source
# =====================================================================================================================
def test_store_and_locks():
    section("2a. Two tabs resume the same run")
    s = new_store()
    check("first tab gets the lock", s.acquire_lock("tab-1"))
    check("second tab is rejected", not s.acquire_lock("tab-2"))
    check("same owner may re-acquire", s.acquire_lock("tab-1"))
    s.release_lock("tab-1")
    check("after release another session can take it", s.acquire_lock("tab-2"))
    s.close()

    section("2b. Process dies holding the lock (TTL takeover)")
    s = new_store()
    s.acquire_lock("dead-process", ttl=0.05)
    check("while the lock is fresh the run counts as running", s.effective_status() == "running")
    time.sleep(0.12)
    check("after the TTL the run is shown as interrupted (resumable)", s.effective_status() == "interrupted")
    check("a new socket can take over", s.acquire_lock("new-process"))
    s.close()

    section("2c. Edit lock (edits cannot race a Resume)")
    s = new_store(); s.set_status("completed")
    check("edit lock keeps the status 'completed'", s.acquire_lock("edit-1", ttl=60, set_running=False) and s.status() == "completed")
    check("a resume is refused while an edit holds the lock", not s.acquire_lock("ws-2"))
    s.release_lock("edit-1")
    check("lock released -> run can be locked again", s.acquire_lock("ws-2"))
    check("a normal acquire marks the run running", s.status() == "running")
    s.close()

    section("2d. Source changed during the pause (frozen snapshot)")
    s = new_store()
    source = [{"Id": "S1", "Name": "old value"}]
    s.save_extracted(0, source)
    source[0]["Name"] = "changed in the CRM during the pause"
    check("extract is marked done, so resume will NOT re-extract", s.is_extracted(0))
    check("resume migrates the frozen snapshot, not the changed source", s.load_extracted(0)[0]["Name"] == "old value")
    s.close()

    section("2e. Durability setting")
    s = new_store()
    check("synchronous=FULL (commits survive power loss)", s._conn.execute("PRAGMA synchronous").fetchone()[0] == 2)
    s.close()


# =====================================================================================================================
# 3. RUNNER: disk full / checkpoint failures / pre-send errors
# =====================================================================================================================
async def test_runner_durability():
    section("3a. Disk almost full - stop BEFORE sending")
    st = new_store(); mig = FakeMigrator(); spec = make_spec(mig=mig)
    old = rr.MIN_FREE_BYTES; rr.MIN_FREE_BYTES = 1 << 60
    try:
        await runner(st).run_pass(spec); raised = False
    except DiskSpaceLow:
        raised = True
    finally:
        rr.MIN_FREE_BYTES = old
    check("DiskSpaceLow is raised and nothing was sent", raised and mig.calls == 0)
    check("rows stay pending (run is resumable)", st.counts().get("pending") == 3)

    section("3b. Checkpoint write fails after the send (disk full)")
    st = new_store(); mig = FakeMigrator(); spec = make_spec(mig=mig)
    real = st.save_results; state = {"n": 0}

    def always_full(*a, **k):
        state["n"] += 1
        raise sqlite3.OperationalError("database or disk is full")
    st.save_results = always_full
    LOGS.clear()
    try:
        await runner(st).run_pass(spec); raised = None
    except CheckpointWriteFailed as e:
        raised = e
    check("persistent failure -> CheckpointWriteFailed (never swallowed)", raised is not None)
    check("the write was retried before giving up", state["n"] >= rr.ResumableRunner.CHECKPOINT_RETRIES)
    check("created target ids are logged for manual reconciliation", any("CRITICAL" in m and "T0" in m for m in LOGS))
    check("nothing more was sent after the failure", mig.calls == 1)
    check("rows stay in_flight so Resume reconciles them", st.counts().get("in_flight") == 2)

    section("3c. Transient lock error while checkpointing")
    st = new_store(); spec = make_spec(); real = st.save_results; box = {"n": 0}

    def once(*a, **k):
        box["n"] += 1
        if box["n"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return real(*a, **k)
    st.save_results = once
    await runner(st).run_pass(spec)
    check("retried and the run completes", st.counts().get("success") == 3)

    section("3d. Rows rejected before sending (bad type / missing key)")
    st = new_store(); mig = FakeMigrator(); spec = make_spec(n=3, mig=mig)
    spec.payload = [x for x in spec.payload if x["originalIndex"] != 1]
    spec.precheck_errors = [{"originalIndex": 1, "message": "[Flag__c] Cannot convert 'maybe' to boolean."}]
    st.save_extracted(0, spec.source_records)
    await runner(st).run_pass(spec)
    c = st.counts()
    check("bad row stored as an error with its message; the others were sent", c.get("error") == 1 and c.get("success") == 2)
    total, rows = st.failed_rows(0, "error", "", 0, 10)
    check("it appears (editable) in the failed-rows list", total == 1 and "Cannot convert" in rows[0]["error"])
    mig.calls = 0
    await runner(st).run_pass(spec)
    check("replaying the pass sends nothing again", mig.calls == 0)


# =====================================================================================================================
# 4. REVERTS (records auto-created by UPDATE mode)
# =====================================================================================================================
async def test_reverts():
    section("4a. Reverts are forgotten only when the CRM confirmed the delete")
    st = new_store()

    class M(FakeMigrator):
        async def revert_created(self, client, options, ids, send_log):
            return ["b"]
    st.ensure_pass(0, 1, "Standard Sync", "update"); st.add_pending_reverts(0, 1, ["a", "b"])
    LOGS.clear()
    await runner(st).run_pass(make_spec(n=0, mig=M(), op="update"))
    check("confirmed revert removed, failed one stays queued", st.get_pending_reverts(0, 1) == ["b"])
    check("a warning names the record that could not be deleted", any("could not be deleted" in m and "b" in m for m in LOGS))

    st2 = new_store()

    class M2(FakeMigrator):
        async def revert_created(self, client, options, ids, send_log):
            return []
    st2.ensure_pass(0, 1, "Standard Sync", "update"); st2.add_pending_reverts(0, 1, ["a", "b"])
    await runner(st2).run_pass(make_spec(n=0, mig=M2(), op="update"))
    check("everything confirmed -> queue is empty", st2.get_pending_reverts(0, 1) == [])

    st3 = new_store()

    class M3(FakeMigrator):
        async def revert_created(self, client, options, ids, send_log):
            return []

        async def upload(self, client, items, op_mode, pass_name, options, send_log):
            options["onRevertPending"](["x1", "x2"])
            options["onRevertDone"](["x1"])
            return await super().upload(client, items, op_mode, pass_name, options, send_log)
    await runner(st3).run_pass(make_spec(n=2, mig=M3(), op="update"))
    check("an unconfirmed in-run revert survives the segment checkpoint", st3.get_pending_reverts(0, 1) == ["x2"])

    st4 = new_store(); st4.set_status("completed")
    st4.ensure_pass(0, 1, "p", "update"); st4.seed(0, 1, [(0, "S0", "pending", None)])
    st4.save_results(0, 1, [RowResult(0, "error", error="boom")]); st4.add_pending_reverts(0, 1, ["x2"]); st4.mark_job_done(0)
    child = st4.fork_for_retry()
    check("Retry fork keeps the pending reverts", child.get_pending_reverts(0, 1) == ["x2"])
    check("...and re-opens the job so they are processed", not child.is_job_done(0))


def _grab_method(path, name):
    src = open(path, encoding="utf-8").read().replace("\r\n", "\n")
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return textwrap.dedent(ast.get_source_segment(src, node))
    raise AssertionError(f"{name} not found in {path}")


def _find(filename):
    hits = glob.glob(f"**/{filename}", recursive=True)
    assert hits, f"{filename} not found under {os.getcwd()}"
    return hits[0]


async def test_migrator_revert_functions():
    section("4b. Salesforce / Zoho _delete_records report exactly what was NOT deleted")

    def chunk_dataset(data, n=100):
        return [data[i:i + n] for i in range(0, len(data), n)]
    logs = []

    async def log(m):
        logs.append(m)

    ns = {"asyncio": asyncio, "chunk_dataset": chunk_dataset}
    exec("class SF:\n" + textwrap.indent(_grab_method(_find("salesforce_migrator.py"), "_delete_records"), "    "), ns)

    class SFClient:
        def __init__(self, results, state="Completed"):
            self.results, self.state = results, state

        async def post(self, url, json=None, headers=None):
            if url.endswith("/job"):
                return Resp(201, {"id": "J1"})
            if url.endswith("/batch"):
                return Resp(201, {"id": "B1"})
            return Resp(200, {})

        async def get(self, url, headers=None):
            return Resp(200, self.results) if url.endswith("/result") else Resp(200, {"state": self.state})
    sf = ns["SF"]()
    r = await sf._delete_records(SFClient([{"success": True}, {"success": False, "errors": [{"statusCode": "ENTITY_IS_DELETED"}]},
                                           {"success": False, "errors": [{"statusCode": "INSUFFICIENT_ACCESS"}]}]),
                                 "i", {}, "https://x/async", "Account", ["a", "b", "c"], log)
    check("Salesforce: only the really-failed id is returned (already-deleted counts as done)", r == ["c"])
    check("Salesforce: a Failed batch -> all its ids unconfirmed", await sf._delete_records(SFClient([], "Failed"), "i", {}, "https://x", "Account", ["a", "b"], log) == ["a", "b"])

    class Down(SFClient):
        async def post(self, *a, **k):
            return Resp(503, "down")
    check("Salesforce: job cannot be opened -> all ids unconfirmed", await sf._delete_records(Down([]), "i", {}, "https://x", "Account", ["a"], log) == ["a"])
    check("Salesforce: nothing to delete -> []", await sf._delete_records(Down([]), "i", {}, "https://x", "Account", [], log) == [])

    ns2 = {"asyncio": asyncio, "chunk_dataset": chunk_dataset, "CrmService": CrmService}
    exec("class Z:\n" + textwrap.indent(_grab_method(_find("zoho_migrator.py"), "_delete_records"), "    "), ns2)

    class ZClient:
        def __init__(self, resp):
            self.resp = resp

        async def delete(self, url, headers=None):
            return self.resp
    z = ns2["Z"]()
    body = {"data": [{"status": "success", "details": {"id": "1"}}, {"status": "error", "message": "record not exist", "details": {"id": "2"}},
                     {"status": "error", "message": "permission denied", "details": {"id": "3"}}]}
    r = await z._delete_records(ZClient(Resp(200, body)), "d", {}, "Leads", ["1", "2", "3", "4"], 1, log)
    check("Zoho: failed id and an id missing from the response are unconfirmed", r == ["3", "4"])
    check("Zoho: HTTP 500 -> everything unconfirmed", await z._delete_records(ZClient(Resp(500, "boom")), "d", {}, "Leads", ["1"], 1, log) == ["1"])


# =====================================================================================================================
# 5. HUBSPOT (round 2)
# =====================================================================================================================
class FakeHubSpot:
    """Minimal HubSpot: batch create/update/upsert with trace ids, single create, search. `fault` injects failures."""

    def __init__(self):
        self.calls, self.n, self.fault, self.search_results = [], 100, None, []

    async def request(self, method, url, headers=None, json=None, **kw):
        self.calls.append((method, url, json))
        if self.fault:
            r = self.fault(method, url, json)
            if r == "RAISE":
                raise TimeoutError("read timed out")
            if r is not None:
                return r
        if url.endswith("/search"):
            return Resp(200, {"results": self.search_results})
        if "/batch/" in url:
            out = []
            for inp in json["inputs"]:
                self.n += 1
                out.append({"id": str(self.n), "objectWriteTraceId": inp.get("objectWriteTraceId"), "properties": inp.get("properties")})
            return Resp(200, {"results": out})
        self.n += 1
        return Resp(201, {"id": str(self.n)})


def hs_rows(n):
    recs = [{"Id": f"S{i}", "Name": f"n{i}"} for i in range(n)]
    pay = [{"originalIndex": i, "targetRecord": {"name": f"n{i}", "src_id": f"S{i}"}} for i in range(n)]
    return recs, pay


def hs_opts(recs, **kw):
    o = {"targetObject": "contacts", "sourceRecords": recs, "token": "T", "userId": "u1", "targetExtIdField": ""}
    o.update(kw)
    return o


async def test_hubspot():
    M = hm.HubspotMigrator()

    section("5a. HubSpot insert - normal path")
    c = FakeHubSpot(); recs, pay = hs_rows(250)
    s, e, k, sr, er, skr = await M.upload(c, pay, "insert", "p", hs_opts(recs), send_log)
    check("250 rows created, every success row carries Target_Id", (s, e) == (250, 0) and all(r.get("Target_Id") for r in sr))

    section("5b. HubSpot insert - a 502 must NOT be replayed")
    c = FakeHubSpot(); recs, pay = hs_rows(250); st = {"n": 0}

    def f502(method, url, js):
        if url.endswith("/batch/create") and len(js["inputs"]) > 1:
            st["n"] += 1
            if st["n"] == 2:
                return Resp(502, {"message": "bad gateway"})
    c.fault = f502
    s, e, k, sr, er, skr = await M.upload(c, pay, "insert", "p", hs_opts(recs), send_log)
    creates = [x for x in c.calls if x[1].endswith("/batch/create") and len(x[2]["inputs"]) > 1]
    check("every big batch was sent exactly once (no replay of an INSERT)", len(creates) == 3)
    check("the 100 rows of the 502 chunk are flagged OUTCOME_UNKNOWN, 150 succeeded",
          s == 150 and len([r for r in er if OUTCOME_UNKNOWN in r["Target_Error"]]) == 100)

    section("5c. HubSpot update - the same 502 IS replayed (replay-safe)")
    c = FakeHubSpot(); recs, pay = hs_rows(5); left = {"n": 2}

    def f502u(method, url, js):
        if left["n"] > 0:
            left["n"] -= 1
            return Resp(502, {"message": "bad gateway"})
    c.fault = f502u
    s, e, k, sr, er, skr = await M.upload(c, pay, "update", "p", hs_opts(recs, targetExtIdField="src_id"), send_log)
    check("update succeeds after retrying through 502", s == 5 and e == 0)

    section("5d. HubSpot network exception mid-run")
    st = {"n": 0}

    def boom(method, url, js):
        if "/batch/" in url and len(js["inputs"]) > 1:
            st["n"] += 1
            if st["n"] == 1:
                return "RAISE"
    c = FakeHubSpot(); recs, pay = hs_rows(250); c.fault = boom
    s, e, k, sr, er, skr = await M.upload(c, pay, "insert", "p", hs_opts(recs), send_log)
    check("upload returns normally and keeps the 150 finished rows", s == 150 and e == 100)
    check("the failed chunk is OUTCOME_UNKNOWN (record may exist)", all(OUTCOME_UNKNOWN in r["Target_Error"] for r in er))
    st["n"] = 0
    c = FakeHubSpot(); recs, pay = hs_rows(250); c.fault = boom
    s, e, k, sr, er, skr = await M.upload(c, pay, "upsert", "p", hs_opts(recs, targetExtIdField="src_id"), send_log)
    check("replay-safe mode: becomes a retryable 'Temporary' row error", e == 100 and all("Temporary" in r["Target_Error"] for r in er))

    section("5e. HubSpot quota exhausted stops the run")
    c = FakeHubSpot(); recs, pay = hs_rows(250)

    def quota(method, url, js):
        if url.endswith("/batch/create") and len(js["inputs"]) > 1:
            raise ApiQuotaExhausted("daily")
    c.fault = quota
    try:
        await M.upload(c, pay, "insert", "p", hs_opts(recs), send_log); raised = False
    except ApiQuotaExhausted:
        raised = True
    check("ApiQuotaExhausted is not swallowed", raised)

    section("5f. HubSpot find_existing (duplicate-free insert resume)")
    c = FakeHubSpot()
    c.search_results = [{"id": "20", "properties": {"src_id": "s1"}}, {"id": "10", "properties": {"src_id": "S1"}}, {"id": "30", "properties": {"src_id": "S2"}}]
    found = await M.find_existing(c, hs_opts([]), "src_id", ["S1", "S2", "S3"])
    sent = c.calls[0][2]["filterGroups"][0]["filters"][0]
    check("searches the correlation property with IN and lower-cased values", sent["operator"] == "IN" and sent["values"] == ["s1", "s2", "s3"])
    check("results are mapped back to the original source keys", set(found) == {"S1", "S2"})
    c2 = FakeHubSpot(); c2.search_results = []
    await M.find_existing(c2, hs_opts([]), "src_id", [f"S{i}" for i in range(250)])
    check("250 keys -> 3 searches of at most 100 values", len(c2.calls) == 3 and all(len(x[2]["filterGroups"][0]["filters"][0]["values"]) <= 100 for x in c2.calls))
    c3 = FakeHubSpot(); flag = {"done": False}

    def once_401(m, u, j):
        if not flag["done"]:
            flag["done"] = True
            return Resp(401, {})
    c3.fault = once_401; c3.search_results = [{"id": "5", "properties": {"src_id": "S1"}}]
    CrmService.refreshed = 0
    f3 = await M.find_existing(c3, hs_opts([]), "src_id", ["S1"])
    check("401 during lookup refreshes the token and still answers", CrmService.refreshed == 1 and f3 == {"S1": "5"})


# =====================================================================================================================
# 6. RUNNER: ambiguous writes (OUTCOME_UNKNOWN)
# =====================================================================================================================
async def test_resolve_unknown():
    section("6. Runner resolves ambiguous INSERT rows instead of re-sending blindly")
    st = new_store()
    recs = [{"Id": f"S{i}", "Name": f"n{i}"} for i in range(4)]
    st.save_extracted(0, recs)
    st.ensure_pass(0, 1, "Standard Sync", "insert"); st.seed(0, 1, [(i, f"S{i}", "pending", None) for i in range(4)])

    class Mig:
        def __init__(self):
            self.lookups = 0

        async def find_existing(self, client, options, key, values):
            self.lookups += 1
            return {"S1": "H-77"}

    def spec(corr, mig):
        s = make_spec(n=4, mig=mig, corr=corr)
        s.source_records = recs
        return s
    unk = lambda i: RowResult(i, "error", error=f"HubSpot Error: {OUTCOME_UNKNOWN}HubSpot did not confirm this write (ReadTimeout)")
    results = [RowResult(0, "success", target_id="1"), unk(1), unk(2), RowResult(3, "error", error="HubSpot Error: bad value")]
    R = runner(st)

    out = {r.row_idx: r for r in await R._resolve_unknown(spec("src_id", Mig()), list(results))}
    check("with a correlation field: the row found in the target becomes a success (no duplicate)", out[1].state == "success" and out[1].target_id == "H-77")
    check("looked up but not found -> 'Temporary', re-sent by the retry rounds", out[2].state == "error" and bool(rr.TRANSIENT_ROW_RE.search(out[2].error)))
    check("other rows are untouched", out[0].state == "success" and out[3].error == "HubSpot Error: bad value")

    m2 = Mig()
    out = {r.row_idx: r for r in await R._resolve_unknown(spec("", m2), list(results))}
    check("no correlation field: no lookup and NEVER re-sent blindly", m2.lookups == 0 and out[1].state == "error" and not rr.TRANSIENT_ROW_RE.search(out[1].error))
    check("the user is told the record may already exist", "may already exist" in out[1].error and OUTCOME_UNKNOWN not in out[1].error)

    class Bad(Mig):
        async def find_existing(self, *a, **k):
            raise RuntimeError("search down")
    out = await R._resolve_unknown(spec("src_id", Bad()), list(results))
    check("lookup failure keeps the rows as unconfirmed failures (no blind re-send)",
          all(not rr.TRANSIENT_ROW_RE.search(r.error or "") for r in out if r.row_idx in (1, 2)))


# =====================================================================================================================
# 7. ZENDESK extract (round 2)
# =====================================================================================================================
async def test_zendesk():
    section("7. Zendesk extraction never returns a partial snapshot")

    class ZClient:
        def __init__(self, seq):
            self.seq, self.calls = list(seq), 0

        async def get(self, url, headers=None):
            self.calls += 1
            return self.seq.pop(0) if self.seq else Resp(401)
    Z = zm.ZendeskMigrator(); creds = {"access_token": "t", "subdomain": "acme", "user_id": "u1"}
    page1 = Resp(200, {"results": [{"id": 1}], "links": {"next": "https://acme.zendesk.com/next"}, "meta": {"has_more": True}})
    try:
        await Z.extract(ZClient([page1, Resp(500)]), creds, "users", "", [], send_log); raised = False
    except RuntimeError:
        raised = True
    check("an error after page 1 is raised (not returned as a 1-row snapshot)", raised)
    zc = ZClient([Resp(401), Resp(401), Resp(401)]); CrmService.refreshed = 0
    try:
        await Z.extract(zc, creds, "users", "", [], send_log); err = None
    except HTTPException as e:
        err = e
    check("repeated 401 -> one refresh, then the session-expired error (no infinite loop)",
          err is not None and err.status_code == 401 and CrmService.refreshed == 1 and zc.calls == 2)
    good = Resp(200, {"results": [{"id": 1}, {"id": 2}], "links": {}, "meta": {"has_more": False}})
    got = await Z.extract(ZClient([Resp(401), good]), creds, "users", "", [], send_log)
    check("one expired token is refreshed transparently and extraction completes", len(got) == 2)


# =====================================================================================================================
async def main():
    sections = [
        ("payload builder", test_payload_builder, False),
        ("store / locks", test_store_and_locks, False),
        ("runner durability", test_runner_durability, True),
        ("reverts", test_reverts, True),
        ("migrator revert functions", test_migrator_revert_functions, True),
        ("hubspot", test_hubspot, True),
        ("runner ambiguous writes", test_resolve_unknown, True),
        ("zendesk", test_zendesk, True),
    ]
    for name, fn, is_async in sections:
        try:
            await fn() if is_async else fn()
        except Exception:
            FAILED.append(f"[{name}] crashed: {traceback.format_exc().strip().splitlines()[-1]}")
            print(f"    CRASH in '{name}':\n{textwrap.indent(traceback.format_exc(), '      ')}")


asyncio.run(main())
print("\n" + "=" * 70)
print(f"{PASSED} checks passed, {len(FAILED)} failed, {len(SECTIONS)} scenario groups")
if FAILED:
    print("\nFAILED:")
    for f in FAILED:
        print("  -", f)
    sys.exit(1)
print("ALL SCENARIOS PASSED")