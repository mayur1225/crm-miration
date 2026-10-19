import asyncio
import logging
import os
import random
import re
import shutil
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional

from fastapi import HTTPException
from starlette.websockets import WebSocketDisconnect

from app.services.crm_service import is_crm_session_expired
from app.services.payload_builder import PayloadBuilderService
from app.services.resilience.retry import ApiQuotaExhausted, CircuitOpen, OUTCOME_UNKNOWN, idempotent_posts
from app.services.store import RowResult, RunStore, TARGET_RESULT_KEYS

logger = logging.getLogger(__name__)

SendLog = Callable[..., Awaitable[None]]

# Row-level failures that are safe to resend (the CRM rejected the row without creating it).
TRANSIENT_ROW_RE = re.compile(
    r"UNABLE_TO_LOCK_ROW|ROW_LOCK|lock wait|concurrent|timed? ?out|temporar|try again|too many requests|rate limit",
    re.I,
)
# DELETE is replayed after a crash: a row the interrupted attempt already deleted is done, not an error.
ALREADY_GONE_RE = re.compile(r"ENTITY_IS_DELETED|entity is deleted|already (been )?deleted|NOT_FOUND|record not found|no record found", re.I)


class RunInterrupted(Exception):
    """Raised at a segment boundary when the client went away; everything finished so far is checkpointed."""


class DiskSpaceLow(RunInterrupted):
    """Raised BEFORE a segment is sent when the runs folder is nearly full; the run stays resumable."""


class CheckpointWriteFailed(Exception):
    """A segment WAS sent but its result could not be written to the run database (disk full / locked)."""


# Minimum free space on the runs volume before another segment may be sent.
MIN_FREE_BYTES = int(os.environ.get("SURESHIFT_MIN_FREE_MB", "200")) * 1024 * 1024

_PROPAGATE = (ApiQuotaExhausted, CircuitOpen, WebSocketDisconnect, asyncio.CancelledError, CheckpointWriteFailed, DiskSpaceLow)


@dataclass
class PassSpec:
    job_idx: int
    pass_no: int                      # 1 = standard / pass 1, 2 = self-reference pass 2, 3 = deferred-reference patch
    pass_name: str
    op_mode: str
    target_object: str
    ext_id_field: str
    segment_size: int
    payload: List[Dict[str, Any]]     # PayloadBuilderService output: [{originalIndex, targetRecord}]
    source_records: List[Dict[str, Any]]
    migrator: Any
    options_base: Dict[str, Any]
    dedupe_key_field: str = ""        # set to dedupe update/upsert batches by unique key
    correlation_field: str = ""       # insert mode: target field stamped with the source key for reconcile
    parent_job_idx: Optional[int] = None  # job whose failed rows block this pass (Pass 2 -> own job, Pass 3 -> original job)
    precheck_errors: List[Dict[str, Any]] = field(default_factory=list)  # rows PayloadBuilder rejected: [{originalIndex, message}]


def source_key(rec: Dict[str, Any]) -> str:
    return str(rec.get("Id") or rec.get("id") or "")


class ResumableRunner:
    MAX_ROW_RETRY_ROUNDS = 2
    CHECKPOINT_RETRIES = 5

    def __init__(self, store: RunStore, client: Any, send_log: SendLog, should_stop: Callable[[], bool]):
        self.store = store
        self.client = client
        self.send_log = send_log
        self.should_stop = should_stop

    # ------------------------------------------------------------------ durability helpers
    def _check_disk(self) -> None:
        """Refuse to send another segment when there is no room to checkpoint its result."""
        try:
            free = shutil.disk_usage(os.path.dirname(os.path.abspath(self.store.path))).free
        except OSError:
            return
        if free < MIN_FREE_BYTES:
            raise DiskSpaceLow(
                f"Only {free // (1024 * 1024)} MB free on the runs disk (need {MIN_FREE_BYTES // (1024 * 1024)} MB). "
                f"Free up disk space, then resume."
            )

    async def _save(self, spec: "PassSpec", results: List[RowResult], clear_bulk: bool = True) -> None:
        """
        The checkpoint write. Never swallowed: retried a few times (disk briefly full / database locked), then
        CheckpointWriteFailed. The rows stay 'in_flight', so a resume reconciles them instead of re-sending blindly.
        """
        delay = 1.0
        last: Optional[Exception] = None
        for attempt in range(1, self.CHECKPOINT_RETRIES + 1):
            try:
                self.store.save_results(spec.job_idx, spec.pass_no, results, clear_bulk=clear_bulk)
                return
            except sqlite3.OperationalError as exc: 
                last = exc
                logger.error("[RUNNER] checkpoint write failed (attempt %d/%d): %s", attempt, self.CHECKPOINT_RETRIES, exc)
                if attempt < self.CHECKPOINT_RETRIES:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 15.0)

        created = [(r.row_idx, r.target_id) for r in results if r.state == "success" and r.target_id]
        try:  
            sample = ", ".join(f"row {i} -> {tid}" for i, tid in created[:200])
            await self.send_log(
                f"[{spec.target_object}] {spec.pass_name}: CRITICAL could not save {len(results)} result(s) ({last}). "
                f"{len(created)} record(s) WERE created in the target"
                + (f" (first {min(len(created), 200)}: {sample})." if created else ".")
                + " Free disk space and resume; check the target for these records before re-sending."
            )
        except Exception:
            pass
        raise CheckpointWriteFailed(
            f"Could not save the results of {len(results)} row(s) to the run database ({last}). "
            f"The run was stopped before sending anything else. Free disk space and resume."
        )

    # ------------------------------------------------------------------ public
    async def run_pass(self, spec: PassSpec) -> None:
        store = self.store
        store.ensure_pass(spec.job_idx, spec.pass_no, spec.pass_name, spec.op_mode)

        if spec.correlation_field and spec.op_mode == "insert":
            for item in spec.payload:
                key = source_key(spec.source_records[item["originalIndex"]])
                if key and item["targetRecord"].get(spec.correlation_field) in (None, ""):
                    item["targetRecord"][spec.correlation_field] = key

        active = spec.payload
        seed_rows = []
        if spec.dedupe_key_field:
            active, dup_skips = PayloadBuilderService.dedupe_by_unique_key(
                spec.payload, spec.source_records, spec.dedupe_key_field
            )
            kept = {i["originalIndex"] for i in active}
            dropped = [i for i in spec.payload if i["originalIndex"] not in kept]
            for item, skipped_copy in zip(dropped, dup_skips):
                idx = item["originalIndex"]
                seed_rows.append((idx, source_key(spec.source_records[idx]), "skipped", skipped_copy["Target_SkipReason"]))
            if dup_skips:
                await self.send_log(
                    f"[{spec.target_object}] {spec.pass_name}: {len(dup_skips)} record(s) shared the same "
                    f"'{spec.ext_id_field}' value within this batch -- keeping the last one, skipping the rest."
                )
        seed_rows += [(i["originalIndex"], source_key(spec.source_records[i["originalIndex"]]), "pending", None) for i in active]
        precheck = list(spec.precheck_errors or [])
        seed_rows += [
            (e["originalIndex"], source_key(spec.source_records[e["originalIndex"]]), "pending", None) for e in precheck
        ]
        store.seed(spec.job_idx, spec.pass_no, seed_rows)
        if precheck:  # rejected before sending (bad value type, missing match key): visible, editable, retryable
            store.fail_pending_rows(spec.job_idx, spec.pass_no, [(e["originalIndex"], e["message"]) for e in precheck])

        by_idx = {i["originalIndex"]: i for i in active}
        await self._reconcile(spec, by_idx)

        blocking = set()
        if spec.parent_job_idx is not None:
            blocking = store.unresolved_source_keys(spec.parent_job_idx)

        total = len(seed_rows)
        while True:
            if self.should_stop():
                raise RunInterrupted()
            self._check_disk()

            rows = store.next_pending(spec.job_idx, spec.pass_no, spec.segment_size)
            if not rows:
                break

            if blocking:
                keys = store.source_keys(spec.job_idx, spec.pass_no, rows)
                blocked = [i for i in rows if keys.get(i) in blocking]
                if blocked:
                    await self._save(spec, [
                        RowResult(i, "skipped", skip_reason="parent_failed: the record this one depends on failed to migrate.")
                        for i in blocked
                    ], clear_bulk=False)
                    rows = [i for i in rows if i not in set(blocked)]
                    if not rows:
                        continue

            missing = [i for i in rows if i not in by_idx]
            if missing:
                await self._save(spec, [
                    RowResult(i, "error", error="Row could not be rebuilt into a payload on resume.") for i in missing
                ], clear_bulk=False)
                rows = [i for i in rows if i in by_idx]
                if not rows:
                    continue

            store.mark_in_flight(spec.job_idx, spec.pass_no, rows)
            await self._send_with_bisect(spec, [by_idx[i] for i in rows])

            counts = self._pass_counts(spec)
            await self.send_log(
                f"[{spec.target_object}] {spec.pass_name}: {counts['done']:,}/{total:,} processed "
                f"({counts['success']:,} ok, {counts['error']:,} failed, {counts['skipped']:,} skipped)."
            )

    # ------------------------------------------------------------------ reconcile after a crash
    async def _reconcile(self, spec: PassSpec, by_idx: Dict[int, Dict[str, Any]]) -> None:
        store = self.store
        reverts = store.get_pending_reverts(spec.job_idx, spec.pass_no)
        reverter = getattr(spec.migrator, "revert_created", None)
        if reverts and reverter:
            await self.send_log(
                f"[{spec.target_object}] {spec.pass_name}: finishing {len(reverts)} pending revert(s) from the interrupted run..."
            )
            failed = set(await reverter(self.client, self._options(spec), reverts, self.send_log) or [])
            # forget only what the target confirmed; the rest is retried on the next resume
            store.remove_pending_reverts(spec.job_idx, spec.pass_no, [t for t in reverts if t not in failed])
            if failed:
                await self.send_log(
                    f"[{spec.target_object}] {spec.pass_name}: WARNING {len(failed)} auto-created record(s) could not be "
                    f"deleted yet and stay queued for the next resume: {', '.join(sorted(failed)[:20])}"
                )
        elif reverts:
            await self.send_log(
                f"[{spec.target_object}] {spec.pass_name}: WARNING {len(reverts)} pending revert(s) kept; "
                f"this CRM adapter cannot revert them automatically."
            )

        stuck = store.in_flight_rows(spec.job_idx, spec.pass_no)
        if not stuck:
            return
        await self.send_log(f"[{spec.target_object}] {spec.pass_name}: reconciling {len(stuck)} row(s) that were mid-flight.")

        bulk = store.get_bulk(spec.job_idx, spec.pass_no)
        if bulk or spec.op_mode != "insert":
            # Bulk job id known -> its results are ingested on resend. upsert/update/delete are replay-safe.
            store.reset_to_pending(spec.job_idx, spec.pass_no, stuck)
            return

        resolved = await self._lookup_created(spec, stuck)
        if resolved:
            await self._save(spec, [RowResult(i, "success", target_id=tid) for i, tid in resolved.items()], clear_bulk=False)
            await self.send_log(f"[{spec.target_object}] {spec.pass_name}: {len(resolved)} row(s) already existed in the target; not re-sent.")
        remaining = [i for i in stuck if i not in resolved]
        if remaining and not self._can_lookup(spec):
            await self.send_log(
                f"[{spec.target_object}] {spec.pass_name}: WARNING insert resume is at-least-once for {len(remaining)} row(s) "
                f"(no correlation field / lookup available) -- duplicates are possible."
            )
        store.reset_to_pending(spec.job_idx, spec.pass_no, remaining)

    def _can_lookup(self, spec: PassSpec) -> bool:
        return bool(spec.correlation_field and getattr(spec.migrator, "find_existing", None))

    async def _lookup_created(self, spec: PassSpec, row_idxs: List[int]) -> Dict[int, str]:
        if not self._can_lookup(spec):
            return {}
        keys = self.store.source_keys(spec.job_idx, spec.pass_no, row_idxs)
        wanted = [k for k in keys.values() if k]
        if not wanted:
            return {}
        found = await spec.migrator.find_existing(self.client, self._options(spec), spec.correlation_field, wanted)
        return {i: found[k] for i, k in keys.items() if k in found}

    # ------------------------------------------------------------------ sending
    def _options(self, spec: PassSpec) -> Dict[str, Any]:
        options = dict(spec.options_base)
        options["sourceRecords"] = spec.source_records
        return options

    async def _send_with_bisect(self, spec: PassSpec, items: List[Dict[str, Any]]) -> None:
        idxs = [i["originalIndex"] for i in items]
        try:
            results = await self._send_with_row_retries(spec, items)
        except _PROPAGATE:
            raise
        except HTTPException as exc:
            if is_crm_session_expired(exc):
                raise
            await self._on_segment_failure(spec, items, idxs, exc)
            return
        except Exception as exc:
            await self._on_segment_failure(spec, items, idxs, exc)
            return
        await self._save(spec, results)
        if not getattr(spec.migrator, "revert_created", None):
            # adapters without revert support never confirm reverts, so do not let them pile up
            self.store.clear_pending_reverts(spec.job_idx, spec.pass_no)

    async def _on_segment_failure(self, spec: PassSpec, items: List[Dict[str, Any]], idxs: List[int], exc: Exception) -> None:
        detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
        message = f"{type(exc).__name__}: {detail}"
        logger.warning("[RUNNER] %s pass %s segment of %d failed: %s", spec.target_object, spec.pass_name, len(idxs), message)

        remaining_items = items
        if spec.op_mode == "insert":
            # The failed attempt may have created some rows; resolve those before resending anything.
            resolved = await self._lookup_created(spec, idxs)
            if resolved:
                await self._save(spec, [RowResult(i, "success", target_id=tid) for i, tid in resolved.items()], clear_bulk=False)
                remaining_items = [i for i in items if i["originalIndex"] not in resolved]
            if remaining_items and not self._can_lookup(spec):
                await self._save(spec, [
                    RowResult(i["originalIndex"], "error",
                              error=f"Segment failed mid-flight and was not retried to avoid duplicates ({message}). Use Retry failed.")
                    for i in remaining_items
                ])
                return

        if not remaining_items:
            await self._save(spec, [], clear_bulk=True)
            return
        if len(remaining_items) == 1:
            await self._save(spec, [
                RowResult(remaining_items[0]["originalIndex"], "error", error=message)
            ])
            return

        mid = len(remaining_items) // 2
        await self.send_log(f"[{spec.target_object}] {spec.pass_name}: segment failed ({message[:160]}); bisecting {len(remaining_items)} rows to isolate the bad record(s)...")
        await self._send_with_bisect(spec, remaining_items[:mid])
        await self._send_with_bisect(spec, remaining_items[mid:])

    UNCONFIRMED_WRITE = (
        "The target did not confirm this write (network / server error) and no duplicate-protection field is "
        "configured, so it was NOT sent again: the record may already exist. Check the target before using Retry failed."
    )

    async def _resolve_unknown(self, spec: PassSpec, results: List[RowResult]) -> List[RowResult]:
        """
        Adapters mark a write whose outcome is ambiguous (timeout / 500 / 502 / 504 on an INSERT) with OUTCOME_UNKNOWN.
        Such a row is never re-sent blindly (that is how duplicates happen): it is looked up by its correlation key.
          found in the target     -> success (nothing is sent again)
          looked up, not found    -> safe to send again (rejoins the normal transient-retry rounds)
          lookup not possible     -> stays a failed row, with an explicit warning
        """
        unknown = {r.row_idx for r in results if r.state == "error" and OUTCOME_UNKNOWN in (r.error or "")}
        if not unknown:
            return results

        resolved: Dict[int, str] = {}
        looked_up = False
        if spec.op_mode == "insert" and self._can_lookup(spec):
            try:
                resolved = await self._lookup_created(spec, sorted(unknown))
                looked_up = True
            except _PROPAGATE:
                raise
            except Exception:
                logger.warning("[RUNNER] lookup of %d unconfirmed write(s) failed; they stay failed rows", len(unknown), exc_info=True)

        out: List[RowResult] = []
        for r in results:
            if r.row_idx not in unknown:
                out.append(r)
            elif r.row_idx in resolved:
                out.append(RowResult(r.row_idx, "success", target_id=resolved[r.row_idx]))
            elif looked_up:
                out.append(RowResult(r.row_idx, "error",
                                     error="Temporary: the write was not confirmed and the record was not found in the target; sending it again."))
            else:
                out.append(RowResult(r.row_idx, "error", error=self.UNCONFIRMED_WRITE))
        if looked_up:
            await self.send_log(
                f"[{spec.target_object}] {spec.pass_name}: {len(resolved)} unconfirmed write(s) were already in the target "
                f"(not re-sent); {len(unknown) - len(resolved)} were not and will be sent again."
            )
        return out

    async def _send_with_row_retries(self, spec: PassSpec, items: List[Dict[str, Any]]) -> List[RowResult]:
        by_idx = {i["originalIndex"]: i for i in items}
        results = await self._resolve_unknown(spec, await self._send(spec, items))
        for round_no in range(1, self.MAX_ROW_RETRY_ROUNDS + 1):
            again = [r for r in results if r.state == "error" and TRANSIENT_ROW_RE.search(r.error or "")]
            if not again:
                break
            await self.send_log(f"[{spec.target_object}] {spec.pass_name}: re-sending {len(again)} row(s) after a transient error (round {round_no}).")
            await asyncio.sleep(random.uniform(0.0, 2.0 ** round_no))
            retried = {r.row_idx: r for r in await self._send(spec, [by_idx[r.row_idx] for r in again])}
            results = await self._resolve_unknown(spec, [retried.get(r.row_idx, r) for r in results])
        return results

    async def _send(self, spec: PassSpec, items: List[Dict[str, Any]]) -> List[RowResult]:
        store = self.store
        records = spec.source_records
        idxs = [i["originalIndex"] for i in items]

        for i in idxs:
            for key in TARGET_RESULT_KEYS:
                records[i].pop(key, None)
        by_identity = {id(records[i]): i for i in idxs}
        by_key = {k: i for i, k in store.source_keys(spec.job_idx, spec.pass_no, idxs).items() if k}

        options = self._options(spec)
        options["onBulkJobCreated"] = lambda job_id: store.set_bulk(spec.job_idx, spec.pass_no, job_id, sorted(idxs))
        options["onRevertPending"] = lambda ids: store.add_pending_reverts(spec.job_idx, spec.pass_no, ids)
        options["onRevertDone"] = lambda ids: store.remove_pending_reverts(spec.job_idx, spec.pass_no, ids)
        bulk = store.get_bulk(spec.job_idx, spec.pass_no)
        if bulk and bulk[1] == sorted(idxs):
            options["resumeBulkJobId"] = bulk[0]

        with idempotent_posts(spec.op_mode in ("upsert", "delete")):
            raw = await spec.migrator.upload(self.client, items, spec.op_mode, spec.pass_name, options, self.send_log)

        if len(raw) == 6:
            succ, err, skipped = raw[3], raw[4], raw[5]
        else:
            succ, err, skipped = raw[2], raw[3], []

        def locate(rec: Dict[str, Any]) -> Optional[int]:
            idx = by_identity.get(id(rec))
            return idx if idx is not None else by_key.get(source_key(rec))

        results: Dict[int, RowResult] = {}
        for rec in succ:
            idx = locate(rec)
            if idx is not None:
                tid = rec.get("Target_Id")
                results[idx] = RowResult(idx, "success", target_id=str(tid) if tid is not None else None)
        for rec in skipped:
            idx = locate(rec)
            if idx is not None:
                results[idx] = RowResult(idx, "skipped", skip_reason=rec.get("Target_SkipReason") or "Skipped: no matching record found.")
        for rec in err:
            idx = locate(rec) if isinstance(rec, dict) else None
            if idx is not None:
                results[idx] = RowResult(idx, "error", error=rec.get("Target_Error") or "API rejected this record.")
        for idx in idxs:
            results.setdefault(idx, RowResult(idx, "error", error="Target returned no result for this row."))
        if spec.op_mode == "delete":
            for idx, res in list(results.items()):
                if res.state == "error" and ALREADY_GONE_RE.search(res.error or ""):
                    results[idx] = RowResult(idx, "skipped", skip_reason="Already deleted in the target (likely by an interrupted earlier attempt).")
        return [results[i] for i in idxs]

    def _pass_counts(self, spec: PassSpec) -> Dict[str, int]:
        rows = self.store._conn.execute(
            "SELECT state, COUNT(*) AS n FROM row_state WHERE job_idx=? AND pass_no=? GROUP BY state",
            (spec.job_idx, spec.pass_no),
        ).fetchall()
        c = {r["state"]: r["n"] for r in rows}
        success, error, skipped = c.get("success", 0), c.get("error", 0), c.get("skipped", 0)
        return {"success": success, "error": error, "skipped": skipped, "done": success + error + skipped}