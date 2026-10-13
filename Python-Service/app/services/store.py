import glob
import json
import os
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

BASE_RUNS_DIR = os.path.join(os.getcwd(), "SureShift_staging_databases", "runs")
_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_STRIPPED_REQUEST_KEYS = {"authToken", "sourceRecords"}
RESULT_STATES = ("success", "error", "skipped")
TARGET_RESULT_KEYS = ("Target_Id", "Target_Error", "Target_SkipReason")

SCHEMA = """
CREATE TABLE IF NOT EXISTS run (
    run_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL,
    request_json TEXT, queue_json TEXT, parent_run_id TEXT,
    created_at TEXT, updated_at TEXT, lock_owner TEXT, lock_expires_at REAL
);
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS job (
    job_idx INTEGER NOT NULL, pass_no INTEGER NOT NULL, pass_name TEXT, op_mode TEXT,
    bulk_job_id TEXT, bulk_rows TEXT,
    PRIMARY KEY (job_idx, pass_no)
);
CREATE TABLE IF NOT EXISTS extracted (
    job_idx INTEGER NOT NULL, row_idx INTEGER NOT NULL, source_json TEXT NOT NULL,
    PRIMARY KEY (job_idx, row_idx)
);
CREATE TABLE IF NOT EXISTS row_state (
    job_idx INTEGER NOT NULL, pass_no INTEGER NOT NULL, row_idx INTEGER NOT NULL,
    state TEXT NOT NULL, target_id TEXT, error TEXT, skip_reason TEXT,
    attempts INTEGER NOT NULL DEFAULT 0, source_key TEXT, updated_at REAL,
    PRIMARY KEY (job_idx, pass_no, row_idx)
);
CREATE INDEX IF NOT EXISTS idx_row_state_lookup ON row_state (job_idx, pass_no, state);
CREATE INDEX IF NOT EXISTS idx_row_state_key ON row_state (job_idx, source_key);
CREATE TABLE IF NOT EXISTS pending_revert (
    job_idx INTEGER NOT NULL, pass_no INTEGER NOT NULL, target_id TEXT NOT NULL,
    PRIMARY KEY (job_idx, pass_no, target_id)
);
"""


class RunNotFound(Exception):
    pass


class RunForbidden(Exception):
    pass


@dataclass
class RowResult:
    row_idx: int
    state: str  # success | error | skipped | pending
    target_id: Optional[str] = None
    error: Optional[str] = None
    skip_reason: Optional[str] = None


def _safe_id(value: str, label: str) -> str:
    if not value or not _ID_RE.fullmatch(str(value)):
        raise ValueError(f"Invalid {label}.")
    return str(value)


def _db_path(user_id: str, run_id: str) -> str:
    user_dir = os.path.join(BASE_RUNS_DIR, _safe_id(str(user_id), "user id"))
    os.makedirs(user_dir, exist_ok=True)
    return os.path.join(user_dir, f"{_safe_id(run_id, 'run id')}.db")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dumps(value: Any) -> str:
    return json.dumps(value, default=str)


class RunStore:
    def __init__(self, user_id: str, run_id: str, path: str):
        self.user_id = str(user_id)
        self.run_id = run_id
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)

    # ------------------------------------------------------------------ lifecycle
    @classmethod
    def create(cls, user_id: str, request: Dict[str, Any], queue: List[Dict[str, Any]],
               parent_run_id: Optional[str] = None) -> "RunStore":
        run_id = f"run_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        store = cls(user_id, run_id, _db_path(user_id, run_id))

        clean_request = {k: v for k, v in request.items() if k not in _STRIPPED_REQUEST_KEYS and k != "queue"}
        clean_queue: List[Dict[str, Any]] = []
        for idx, job in enumerate(queue):
            job_copy = {k: v for k, v in job.items() if k not in _STRIPPED_REQUEST_KEYS}
            if "sourceRecords" in job:
                store.set_kv(f"csv:{idx}", _dumps(job["sourceRecords"]))
            clean_queue.append(job_copy)

        now = _now_iso()
        with store._tx() as c:
            c.execute(
                "INSERT INTO run (run_id, user_id, status, request_json, queue_json, parent_run_id, created_at, updated_at) "
                "VALUES (?, ?, 'running', ?, ?, ?, ?, ?)",
                (run_id, store.user_id, _dumps(clean_request), _dumps(clean_queue), parent_run_id, now, now),
            )
        return store

    @classmethod
    def open(cls, user_id: str, run_id: str) -> "RunStore":
        path = _db_path(user_id, run_id)
        if not os.path.exists(path):
            raise RunNotFound(run_id)
        store = cls(user_id, run_id, path)
        row = store._conn.execute("SELECT user_id FROM run").fetchone()
        if not row:
            store.close()
            raise RunNotFound(run_id)
        if row["user_id"] != str(user_id):
            store.close()
            raise RunForbidden(run_id)
        return store

    @classmethod
    def list_runs(cls, user_id: str) -> List[Dict[str, Any]]:
        user_dir = os.path.join(BASE_RUNS_DIR, _safe_id(str(user_id), "user id"))
        out: List[Dict[str, Any]] = []
        for path in glob.glob(os.path.join(user_dir, "*.db")):
            run_id = os.path.basename(path)[:-3]
            try:
                store = cls.open(user_id, run_id)
            except (RunNotFound, RunForbidden, sqlite3.Error):
                continue
            try:
                out.append(store.summary())
            finally:
                store.close()
        out.sort(key=lambda r: r["updatedAt"] or "", reverse=True)
        return out

    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error:
            pass

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")

    # ------------------------------------------------------------------ run row, lock, status
    def _run_row(self) -> sqlite3.Row:
        return self._conn.execute("SELECT * FROM run").fetchone()

    def status(self) -> str:
        return self._run_row()["status"]

    def effective_status(self) -> str:
        row = self._run_row()
        if row["status"] == "running" and (row["lock_expires_at"] or 0) < time.time():
            return "interrupted"
        return row["status"]

    def set_status(self, status: str) -> None:
        with self._tx() as c:
            c.execute("UPDATE run SET status=?, updated_at=?", (status, _now_iso()))

    def acquire_lock(self, owner: str, ttl: float = 30.0) -> bool:
        now = time.time()
        with self._tx() as c:
            row = c.execute("SELECT lock_owner, lock_expires_at FROM run").fetchone()
            if row["lock_owner"] and row["lock_owner"] != owner and (row["lock_expires_at"] or 0) > now:
                return False
            c.execute("UPDATE run SET lock_owner=?, lock_expires_at=?, status='running', updated_at=?",
                      (owner, now + ttl, _now_iso()))
        return True

    def heartbeat(self, owner: str, ttl: float = 30.0) -> None:
        with self._tx() as c:
            c.execute("UPDATE run SET lock_expires_at=?, updated_at=? WHERE lock_owner=?",
                      (time.time() + ttl, _now_iso(), owner))

    def release_lock(self, owner: str) -> None:
        with self._tx() as c:
            c.execute("UPDATE run SET lock_owner=NULL, lock_expires_at=NULL WHERE lock_owner=?", (owner,))

    def load_request(self) -> Dict[str, Any]:
        return json.loads(self._run_row()["request_json"] or "{}")

    def load_queue(self) -> List[Dict[str, Any]]:
        return json.loads(self._run_row()["queue_json"] or "[]")

    def summary(self) -> Dict[str, Any]:
        row = self._run_row()
        counts = self.counts()
        status = self.effective_status()
        return {
            "runId": row["run_id"],
            "parentRunId": row["parent_run_id"],
            "status": status,
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "counts": counts,
            "resumable": status == "interrupted",
            "retryable": status == "completed" and counts.get("error", 0) > 0,
        }

    def counts(self) -> Dict[str, int]:
        rows = self._conn.execute("SELECT state, COUNT(*) AS n FROM row_state GROUP BY state").fetchall()
        return {r["state"]: r["n"] for r in rows}

    def job_progress(self) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT job_idx, pass_no, state, COUNT(*) AS n FROM row_state GROUP BY job_idx, pass_no, state "
            "ORDER BY job_idx, pass_no"
        ).fetchall()
        progress: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for r in rows:
            entry = progress.setdefault((r["job_idx"], r["pass_no"]),
                                        {"jobIdx": r["job_idx"], "passNo": r["pass_no"], "counts": {}})
            entry["counts"][r["state"]] = r["n"]
        return list(progress.values())

    # ------------------------------------------------------------------ key/value (job-level flags)
    def set_kv(self, key: str, value: str) -> None:
        self._conn.execute("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", (key, value))

    def get_kv(self, key: str) -> Optional[str]:
        row = self._conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def delete_kv(self, key: str) -> None:
        self._conn.execute("DELETE FROM kv WHERE key=?", (key,))

    def is_extracted(self, job_idx: int) -> bool:
        return self.get_kv(f"extracted:{job_idx}") == "1"

    def is_job_done(self, job_idx: int) -> bool:
        return self.get_kv(f"done:{job_idx}") == "1"

    def mark_job_done(self, job_idx: int) -> None:
        self.set_kv(f"done:{job_idx}", "1")

    def files_done(self, job_idx: int) -> bool:
        return self.get_kv(f"files_done:{job_idx}") == "1"

    def mark_files_done(self, job_idx: int) -> None:
        self.set_kv(f"files_done:{job_idx}", "1")

    def get_job_meta(self, job_idx: int) -> Dict[str, Any]:
        return json.loads(self.get_kv(f"meta:{job_idx}") or "{}")

    def set_job_meta(self, job_idx: int, **values: Any) -> None:
        meta = self.get_job_meta(job_idx)
        meta.update(values)
        self.set_kv(f"meta:{job_idx}", _dumps(meta))

    def get_file_checkpoint(self, job_idx: int) -> Dict[str, List[str]]:
        raw = json.loads(self.get_kv(f"file_ckpt:{job_idx}") or "{}")
        return {"attachments": raw.get("attachments", []), "contentVersions": raw.get("contentVersions", [])}

    def save_file_checkpoint(self, job_idx: int, attachment_ids: Sequence[str], content_version_ids: Sequence[str]) -> None:
        self.set_kv(f"file_ckpt:{job_idx}", _dumps({
            "attachments": list(attachment_ids), "contentVersions": list(content_version_ids),
        }))

    def get_csv_rows(self, job_idx: int) -> List[Dict[str, Any]]:
        return json.loads(self.get_kv(f"csv:{job_idx}") or "[]")

    # ------------------------------------------------------------------ extracted source rows
    def save_extracted(self, job_idx: int, records: Sequence[Dict[str, Any]]) -> None:
        with self._tx() as c:
            c.execute("DELETE FROM extracted WHERE job_idx=?", (job_idx,))
            for start in range(0, len(records), 2000):
                chunk = records[start:start + 2000]
                c.executemany(
                    "INSERT INTO extracted (job_idx, row_idx, source_json) VALUES (?, ?, ?)",
                    [(job_idx, start + i, _dumps(rec)) for i, rec in enumerate(chunk)],
                )
            c.execute("INSERT OR REPLACE INTO kv (key, value) VALUES (?, '1')", (f"extracted:{job_idx}",))

    def load_extracted(self, job_idx: int) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT source_json FROM extracted WHERE job_idx=? ORDER BY row_idx", (job_idx,)
        ).fetchall()
        return [json.loads(r["source_json"]) for r in rows]

    # ------------------------------------------------------------------ pass / row state
    def ensure_pass(self, job_idx: int, pass_no: int, pass_name: str, op_mode: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO job (job_idx, pass_no, pass_name, op_mode) VALUES (?, ?, ?, ?)",
            (job_idx, pass_no, pass_name, op_mode),
        )

    def seed(self, job_idx: int, pass_no: int, rows: Sequence[Tuple[int, str, str, Optional[str]]]) -> None:
        """rows = (row_idx, source_key, state, skip_reason). INSERT OR IGNORE keeps resumed progress intact."""
        now = time.time()
        with self._tx() as c:
            c.executemany(
                "INSERT OR IGNORE INTO row_state (job_idx, pass_no, row_idx, state, skip_reason, source_key, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(job_idx, pass_no, idx, state, reason, key, now) for idx, key, state, reason in rows],
            )

    def next_pending(self, job_idx: int, pass_no: int, limit: int) -> List[int]:
        rows = self._conn.execute(
            "SELECT row_idx FROM row_state WHERE job_idx=? AND pass_no=? AND state='pending' ORDER BY row_idx LIMIT ?",
            (job_idx, pass_no, limit),
        ).fetchall()
        return [r["row_idx"] for r in rows]

    def in_flight_rows(self, job_idx: int, pass_no: int) -> List[int]:
        rows = self._conn.execute(
            "SELECT row_idx FROM row_state WHERE job_idx=? AND pass_no=? AND state='in_flight' ORDER BY row_idx",
            (job_idx, pass_no),
        ).fetchall()
        return [r["row_idx"] for r in rows]

    def mark_in_flight(self, job_idx: int, pass_no: int, row_idxs: Sequence[int]) -> None:
        self._set_state(job_idx, pass_no, row_idxs, "in_flight")

    def reset_to_pending(self, job_idx: int, pass_no: int, row_idxs: Sequence[int]) -> None:
        self._set_state(job_idx, pass_no, row_idxs, "pending")

    def _set_state(self, job_idx: int, pass_no: int, row_idxs: Sequence[int], state: str) -> None:
        now = time.time()
        with self._tx() as c:
            c.executemany(
                "UPDATE row_state SET state=?, updated_at=? WHERE job_idx=? AND pass_no=? AND row_idx=?",
                [(state, now, job_idx, pass_no, i) for i in row_idxs],
            )

    def save_results(self, job_idx: int, pass_no: int, results: Sequence[RowResult], clear_bulk: bool = True) -> None:
        """One transaction per segment: this is the checkpoint."""
        now = time.time()
        with self._tx() as c:
            c.executemany(
                "UPDATE row_state SET state=?, target_id=?, error=?, skip_reason=?, attempts=attempts+1, updated_at=? "
                "WHERE job_idx=? AND pass_no=? AND row_idx=?",
                [(r.state, r.target_id, r.error, r.skip_reason, now, job_idx, pass_no, r.row_idx) for r in results],
            )
            if clear_bulk:
                c.execute("UPDATE job SET bulk_job_id=NULL, bulk_rows=NULL WHERE job_idx=? AND pass_no=?", (job_idx, pass_no))

    def source_keys(self, job_idx: int, pass_no: int, row_idxs: Sequence[int]) -> Dict[int, str]:
        out: Dict[int, str] = {}
        for start in range(0, len(row_idxs), 500):
            chunk = list(row_idxs[start:start + 500])
            marks = ",".join("?" * len(chunk))
            rows = self._conn.execute(
                f"SELECT row_idx, source_key FROM row_state WHERE job_idx=? AND pass_no=? AND row_idx IN ({marks})",
                [job_idx, pass_no, *chunk],
            ).fetchall()
            out.update({r["row_idx"]: r["source_key"] or "" for r in rows})
        return out

    def attempts(self, job_idx: int, pass_no: int, row_idxs: Sequence[int]) -> Dict[int, int]:
        out: Dict[int, int] = {}
        for start in range(0, len(row_idxs), 500):
            chunk = list(row_idxs[start:start + 500])
            marks = ",".join("?" * len(chunk))
            rows = self._conn.execute(
                f"SELECT row_idx, attempts FROM row_state WHERE job_idx=? AND pass_no=? AND row_idx IN ({marks})",
                [job_idx, pass_no, *chunk],
            ).fetchall()
            out.update({r["row_idx"]: r["attempts"] for r in rows})
        return out

    def unresolved_source_keys(self, job_idx: int) -> set:
        """Source keys that failed (or were skipped because a parent failed) in any pass of this job."""
        rows = self._conn.execute(
            "SELECT DISTINCT source_key FROM row_state WHERE job_idx=? AND source_key != '' AND "
            "(state='error' OR (state='skipped' AND skip_reason LIKE 'parent_failed%'))",
            (job_idx,),
        ).fetchall()
        return {r["source_key"] for r in rows}

    # ------------------------------------------------------------------ Bulk job + revert tracking
    def set_bulk(self, job_idx: int, pass_no: int, bulk_job_id: str, row_idxs: Sequence[int]) -> None:
        self._conn.execute(
            "UPDATE job SET bulk_job_id=?, bulk_rows=? WHERE job_idx=? AND pass_no=?",
            (bulk_job_id, _dumps(list(row_idxs)), job_idx, pass_no),
        )

    def get_bulk(self, job_idx: int, pass_no: int) -> Optional[Tuple[str, List[int]]]:
        row = self._conn.execute(
            "SELECT bulk_job_id, bulk_rows FROM job WHERE job_idx=? AND pass_no=?", (job_idx, pass_no)
        ).fetchone()
        if not row or not row["bulk_job_id"]:
            return None
        return row["bulk_job_id"], json.loads(row["bulk_rows"] or "[]")

    def add_pending_reverts(self, job_idx: int, pass_no: int, target_ids: Sequence[str]) -> None:
        with self._tx() as c:
            c.executemany(
                "INSERT OR IGNORE INTO pending_revert (job_idx, pass_no, target_id) VALUES (?, ?, ?)",
                [(job_idx, pass_no, str(t)) for t in target_ids if t],
            )

    def get_pending_reverts(self, job_idx: int, pass_no: int) -> List[str]:
        rows = self._conn.execute(
            "SELECT target_id FROM pending_revert WHERE job_idx=? AND pass_no=?", (job_idx, pass_no)
        ).fetchall()
        return [r["target_id"] for r in rows]

    def clear_pending_reverts(self, job_idx: int, pass_no: int) -> None:
        self._conn.execute("DELETE FROM pending_revert WHERE job_idx=? AND pass_no=?", (job_idx, pass_no))

    # ------------------------------------------------------------------ results
    def _result_records(self, where: str = "", params: Sequence[Any] = ()) -> List[Tuple[RowResult, Dict[str, Any], int, int]]:
        rows = self._conn.execute(
            "SELECT r.job_idx, r.pass_no, r.row_idx, r.state, r.target_id, r.error, r.skip_reason, e.source_json "
            "FROM row_state r JOIN extracted e ON e.job_idx = r.job_idx AND e.row_idx = r.row_idx "
            f"WHERE r.state IN ('success','error','skipped') {where} ORDER BY r.job_idx, r.pass_no, r.row_idx",
            params,
        ).fetchall()
        out = []
        for r in rows:
            rec = json.loads(r["source_json"])
            res = RowResult(r["row_idx"], r["state"], r["target_id"], r["error"], r["skip_reason"])
            out.append((res, rec, r["job_idx"], r["pass_no"]))
        return out

    @staticmethod
    def _decorate(res: RowResult, rec: Dict[str, Any]) -> Dict[str, Any]:
        for key in TARGET_RESULT_KEYS:
            rec.pop(key, None)
        if res.state == "success":
            rec["Target_Id"] = res.target_id
        elif res.state == "error":
            rec["Target_Error"] = res.error or "Unknown Error"
        else:
            rec["Target_SkipReason"] = res.skip_reason or "Skipped: no matching record found."
        return rec

    def job_success_records(self, job_idx: int) -> List[Dict[str, Any]]:
        return [self._decorate(res, rec) for res, rec, _, _ in self._result_records("AND r.job_idx=? AND r.state='success'", (job_idx,))]

    def collect_results(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        success: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []
        for res, rec, _, _ in self._result_records():
            target = {"success": success, "error": errors}.get(res.state, skipped)
            target.append(self._decorate(res, rec))
        return success, errors, skipped

    # ------------------------------------------------------------------ retry-failed fork
    def fork_for_retry(self) -> "RunStore":
        """
        Child run = full copy of this run (source rows, queue, finished rows) with failed rows reset to
        pending. Because finished rows are carried over, the child's final report is the merged latest
        state of every row, and the passes re-run in the original order.
        """
        failed = self._conn.execute("SELECT COUNT(*) AS n FROM row_state WHERE state='error'").fetchone()["n"]
        if not failed:
            raise ValueError("This run has no failed rows to retry.")

        new_id = f"run_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        path = _db_path(self.user_id, new_id)
        dst = sqlite3.connect(path)
        try:
            self._conn.backup(dst)
        finally:
            dst.close()

        child = RunStore(self.user_id, new_id, path)
        now_iso, now = _now_iso(), time.time()
        with child._tx() as c:
            c.execute(
                "UPDATE run SET run_id=?, parent_run_id=?, status='interrupted', lock_owner=NULL, "
                "lock_expires_at=NULL, created_at=?, updated_at=?",
                (new_id, self.run_id, now_iso, now_iso),
            )
            c.execute(
                "UPDATE row_state SET state='pending', error=NULL, skip_reason=NULL, attempts=0, updated_at=? "
                "WHERE state='error' OR (state='skipped' AND skip_reason LIKE 'parent_failed%')",
                (now,),
            )
            c.execute("UPDATE job SET bulk_job_id=NULL, bulk_rows=NULL")
            c.execute("DELETE FROM pending_revert")
            c.execute("DELETE FROM kv WHERE key='cancel'")
            for r in c.execute("SELECT DISTINCT job_idx FROM row_state WHERE state='pending'").fetchall():
                c.execute("DELETE FROM kv WHERE key IN (?, ?)", (f"done:{r['job_idx']}", f"files_done:{r['job_idx']}"))
        return child