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
CREATE TABLE IF NOT EXISTS row_edit (
    id INTEGER PRIMARY KEY AUTOINCREMENT, job_idx INTEGER NOT NULL, row_idx INTEGER NOT NULL,
    field TEXT NOT NULL, old_value TEXT, new_value TEXT, edited_by TEXT, edited_at TEXT
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
        self.user_id = str(user_id)   # owner: the folder the run lives in
        self.actor_id = self.user_id  # who is operating on it right now (differs for a colleague)
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

    @classmethod
    def open_shared(cls, run_id: str, actor_id: str) -> "RunStore":
        """Open a run wherever it lives, with NO ownership check. Callers (RunAccess) must authorize by org."""
        run_id = _safe_id(run_id, "run id")
        matches = glob.glob(os.path.join(BASE_RUNS_DIR, "*", f"{run_id}.db"))
        if not matches:
            raise RunNotFound(run_id)
        path = matches[0]
        store = cls(os.path.basename(os.path.dirname(path)), run_id, path)
        store.actor_id = str(actor_id)
        return store

    @staticmethod
    def recent_run_paths(max_age_days: int = 14) -> List[str]:
        cutoff = time.time() - max_age_days * 86400
        out = []
        for path in glob.glob(os.path.join(BASE_RUNS_DIR, "*", "*.db")):
            try:
                newest = max(os.path.getmtime(p) for p in (path, path + "-wal") if os.path.exists(p))
            except OSError:
                continue
            if newest >= cutoff:
                out.append(path)
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
        meta = self.run_meta()
        stopped = status in ("interrupted", "cancelled", "failed")
        return {
            "runId": row["run_id"],
            "parentRunId": row["parent_run_id"],
            "status": status,
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "counts": counts,
            "resumable": status in ("interrupted", "cancelled"),
            "retryable": status == "completed" and (counts.get("error", 0) > 0 or counts.get("pending", 0) > 0),
            "startedBy": meta.get("startedBy"),
            "participants": (meta.get("participants") or [])[-5:],
            "sourceCrm": meta.get("sourceCrm"),
            "targetCrm": meta.get("targetCrm"),
            "jobTargets": meta.get("jobTargets") or [],
            "position": self.position() if status != "completed" else None,
            "stopReason": self.get_stop_reason() if stopped else None,
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
        failed = self._conn.execute("SELECT COUNT(*) AS n FROM row_state WHERE state IN ('error','pending')").fetchone()["n"]
        if not failed:
            raise ValueError("This run has no failed rows to retry.")

        new_id = f"run_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        path = _db_path(self.actor_id, new_id)
        dst = sqlite3.connect(path)
        try:
            self._conn.backup(dst)
        finally:
            dst.close()

        child = RunStore(self.actor_id, new_id, path)
        now_iso, now = _now_iso(), time.time()
        with child._tx() as c:
            c.execute(
                "UPDATE run SET run_id=?, user_id=?, parent_run_id=?, status='interrupted', lock_owner=NULL, "
                "lock_expires_at=NULL, created_at=?, updated_at=?",
                (new_id, self.actor_id, self.run_id, now_iso, now_iso),
            )
            c.execute(
                "UPDATE row_state SET state='pending', error=NULL, skip_reason=NULL, attempts=0, updated_at=? "
                "WHERE state='error' OR (state='skipped' AND skip_reason LIKE 'parent_failed%')",
                (now,),
            )
            c.execute("UPDATE job SET bulk_job_id=NULL, bulk_rows=NULL")
            c.execute("DELETE FROM pending_revert")
            c.execute("DELETE FROM kv WHERE key IN ('cancel','stop_reason')")
            for r in c.execute("SELECT DISTINCT job_idx FROM row_state WHERE state='pending'").fetchall():
                c.execute("DELETE FROM kv WHERE key IN (?, ?)", (f"done:{r['job_idx']}", f"files_done:{r['job_idx']}"))
        return child

    # ------------------------------------------------------------------ org identity / ownership / stop reason
    def run_meta(self) -> Dict[str, Any]:
        return json.loads(self.get_kv("run_meta") or "{}")

    def set_run_meta(self, **values: Any) -> None:
        meta = self.run_meta()
        meta.update(values)
        self.set_kv("run_meta", _dumps(meta))

    def add_participant(self, user_id: str, name: Optional[str], action: str) -> None:
        meta = self.run_meta()
        people = meta.get("participants") or []
        people.append({"id": str(user_id), "name": name, "action": action, "at": _now_iso()})
        meta["participants"] = people[-20:]
        self.set_kv("run_meta", _dumps(meta))

    def set_stop_reason(self, code: str, message: str) -> None:
        self.set_kv("stop_reason", _dumps({"code": code, "message": message, "at": _now_iso()}))

    def get_stop_reason(self) -> Optional[Dict[str, Any]]:
        raw = self.get_kv("stop_reason")
        return json.loads(raw) if raw else None

    def clear_stop_reason(self) -> None:
        self.delete_kv("stop_reason")

    def position(self) -> Optional[Dict[str, Any]]:
        """Where the run is (or stopped): first (job, pass) that still has unfinished rows."""
        targets = self.run_meta().get("jobTargets") or []
        row = self._conn.execute(
            "SELECT job_idx, pass_no FROM row_state WHERE state IN ('pending','in_flight') "
            "ORDER BY job_idx, pass_no LIMIT 1"
        ).fetchone()
        if row:
            j, p = row["job_idx"], row["pass_no"]
            c = {r["state"]: r["n"] for r in self._conn.execute(
                "SELECT state, COUNT(*) AS n FROM row_state WHERE job_idx=? AND pass_no=? GROUP BY state", (j, p)
            ).fetchall()}
            name = self._conn.execute("SELECT pass_name FROM job WHERE job_idx=? AND pass_no=?", (j, p)).fetchone()
            total = sum(c.values())
            return {
                "jobIdx": j, "passNo": p, "passName": name["pass_name"] if name else None,
                "targetObject": targets[j] if j < len(targets) else None, "stage": "sending",
                "processed": total - c.get("pending", 0) - c.get("in_flight", 0), "total": total,
            }
        for idx in range(len(targets)):  # stopped before rows were seeded (extract / id-mapping / files)
            if not self.is_job_done(idx):
                return {"jobIdx": idx, "passNo": None, "passName": None, "targetObject": targets[idx],
                        "stage": "extracting" if not self.is_extracted(idx) else "finishing", "processed": 0, "total": 0}
        return None

    def job_columns(self) -> List[Dict[str, Any]]:
        """Mapped source/target columns per job, for the failed-record editor (no secrets, no sfRules)."""
        out = []
        for idx, job in enumerate(self.load_queue()):
            cols = []
            for m in job.get("mappings", []):
                tgt = m.get("targetField") or m.get("sfField")
                src = m.get("sourceField") or m.get("csvField")
                if tgt and src:
                    cols.append({"sourceField": src, "targetField": tgt, "type": m.get("type")})
            out.append({"jobIdx": idx, "targetObject": job.get("targetObject"),
                        "isPass3Patch": bool(job.get("isPass3Patch")), "columns": cols})
        return out

    # ------------------------------------------------------------------ failed rows: read, edit, requeue
    def failed_rows(self, job_idx: Optional[int], state: str, search: str, offset: int, limit: int) -> Tuple[int, List[Dict[str, Any]]]:
        conds = ["r.state='skipped' AND r.skip_reason LIKE 'parent_failed%'"] if state == "blocked" else ["r.state='error'"]
        params: List[Any] = []
        if job_idx is not None:
            conds.append("r.job_idx=?")
            params.append(job_idx)
        if search:
            like = f"%{search}%"
            conds.append("(r.error LIKE ? OR e.source_json LIKE ?)")
            params += [like, like]
        base = ("FROM row_state r JOIN extracted e ON e.job_idx=r.job_idx AND e.row_idx=r.row_idx WHERE "
                + " AND ".join(conds))
        total = self._conn.execute(f"SELECT COUNT(*) AS n {base}", params).fetchone()["n"]
        rows = self._conn.execute(
            "SELECT r.job_idx, r.pass_no, r.row_idx, r.state, r.error, r.skip_reason, r.attempts, e.source_json "
            f"{base} ORDER BY r.job_idx, r.pass_no, r.row_idx LIMIT ? OFFSET ?", [*params, limit, offset],
        ).fetchall()
        out = []
        for r in rows:
            rec = json.loads(r["source_json"])
            for k in TARGET_RESULT_KEYS:
                rec.pop(k, None)
            out.append({"jobIdx": r["job_idx"], "passNo": r["pass_no"], "rowIdx": r["row_idx"], "state": r["state"],
                        "error": r["error"] or r["skip_reason"], "attempts": r["attempts"], "record": rec})
        return total, out

    def failure_groups(self, job_idx: Optional[int]) -> List[Dict[str, Any]]:
        sql = "SELECT job_idx, substr(error, 1, 240) AS message, COUNT(*) AS n FROM row_state WHERE state='error'"
        params: List[Any] = []
        if job_idx is not None:
            sql += " AND job_idx=?"
            params.append(job_idx)
        sql += " GROUP BY job_idx, message ORDER BY n DESC LIMIT 30"
        return [{"jobIdx": r["job_idx"], "message": r["message"], "count": r["n"]}
                for r in self._conn.execute(sql, params).fetchall()]

    def apply_edits(self, edits: Sequence[Dict[str, Any]], editor: str) -> Dict[str, Any]:
        """
        Persist corrected source values and put those rows (and rows blocked only by them) back to
        'pending', so the next Resume / Retry sends the corrected data. Every change is audited in row_edit.
        """
        queue = self.load_queue()
        allowed: Dict[int, set] = {}
        for idx, job in enumerate(queue):
            names = set()
            for m in job.get("mappings", []):
                names.update(x for x in (m.get("sourceField"), m.get("csvField"), m.get("targetField"), m.get("sfField")) if x)
            allowed[idx] = names

        now, now_iso = time.time(), _now_iso()
        changed_rows = requeued = unblocked = 0
        touched_jobs: set = set()
        keys_by_job: Dict[int, set] = {}

        with self._tx() as c:
            for e in edits:
                job_idx, row_idx = int(e["jobIdx"]), int(e["rowIdx"])
                row = c.execute("SELECT source_json FROM extracted WHERE job_idx=? AND row_idx=?", (job_idx, row_idx)).fetchone()
                if not row:
                    continue
                rec = json.loads(row["source_json"])
                diffs = 0
                for field, value in (e.get("fields") or {}).items():
                    if field in TARGET_RESULT_KEYS or field not in allowed.get(job_idx, set()):
                        continue
                    old = rec.get(field)
                    if str(old if old is not None else "") == str(value if value is not None else ""):
                        continue
                    c.execute(
                        "INSERT INTO row_edit (job_idx, row_idx, field, old_value, new_value, edited_by, edited_at) VALUES (?,?,?,?,?,?,?)",
                        (job_idx, row_idx, field, None if old is None else str(old), None if value is None else str(value), editor, now_iso),
                    )
                    rec[field] = value
                    diffs += 1
                if not diffs:
                    continue
                c.execute("UPDATE extracted SET source_json=? WHERE job_idx=? AND row_idx=?", (_dumps(rec), job_idx, row_idx))
                changed_rows += 1
                cur = c.execute(
                    "UPDATE row_state SET state='pending', error=NULL, skip_reason=NULL, attempts=0, updated_at=? "
                    "WHERE job_idx=? AND row_idx=? AND state IN ('error','skipped')", (now, job_idx, row_idx))
                requeued += cur.rowcount
                for k in c.execute("SELECT DISTINCT source_key FROM row_state WHERE job_idx=? AND row_idx=? AND source_key != ''",
                                   (job_idx, row_idx)).fetchall():
                    keys_by_job.setdefault(job_idx, set()).add(k["source_key"])
                touched_jobs.add(job_idx)

            # a later pass (e.g. self-reference Pass 2) that was blocked only because this record failed
            for job_idx, keys in keys_by_job.items():
                keys = list(keys)
                for start in range(0, len(keys), 500):
                    chunk = keys[start:start + 500]
                    marks = ",".join("?" * len(chunk))
                    cur = c.execute(
                        "UPDATE row_state SET state='pending', skip_reason=NULL, attempts=0, updated_at=? "
                        f"WHERE job_idx=? AND state='skipped' AND skip_reason LIKE 'parent_failed%' AND source_key IN ({marks})",
                        [now, job_idx, *chunk])
                    unblocked += cur.rowcount

            for job_idx in touched_jobs:  # a finished job must be re-entered to send its re-queued rows
                c.execute("DELETE FROM kv WHERE key IN (?, ?)", (f"done:{job_idx}", f"files_done:{job_idx}"))

        return {"rowsChanged": changed_rows, "rowsRequeued": requeued, "rowsUnblocked": unblocked, "jobs": sorted(touched_jobs)}

    def mass_update(self, job_idx: int, field: str, value: Any, error_contains: str, editor: str) -> Dict[str, Any]:
        sql, params = "SELECT row_idx FROM row_state WHERE job_idx=? AND state='error'", [job_idx]
        if error_contains:
            sql += " AND error LIKE ?"
            params.append(f"%{error_contains}%")
        rows = sorted({r["row_idx"] for r in self._conn.execute(sql, params).fetchall()})
        return self.apply_edits([{"jobIdx": job_idx, "rowIdx": i, "fields": {field: value}} for i in rows], editor)

    # ------------------------------------------------------------------ start over (fresh run, same configuration)
    def fork_for_restart(self, acknowledge_duplicates: bool = False) -> "RunStore":
        """
        New run with the same request + queue + CSV payload and NO progress: it re-extracts and re-sends
        everything. In insert mode that duplicates records the old run already created, so that case needs
        an explicit acknowledgement. The old run is marked 'superseded' so it can't be resumed in parallel.
        """
        if self.effective_status() in ("running", "superseded"):
            raise ValueError(f"A {self.effective_status()} run cannot be restarted.")
        created = self.counts().get("success", 0)
        inserts = any((j.get("operationMode") or "insert") == "insert" for j in self.load_queue())
        if created and inserts and not acknowledge_duplicates:
            raise ValueError(f"{created} record(s) were already created by this run; starting over in Insert mode would duplicate them. Confirm to proceed.")

        new_id = f"run_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        child = RunStore(self.actor_id, new_id, _db_path(self.actor_id, new_id))
        now_iso = _now_iso()
        src = self._run_row()
        with child._tx() as c:
            c.execute(
                "INSERT INTO run (run_id, user_id, status, request_json, queue_json, parent_run_id, created_at, updated_at) "
                "VALUES (?, ?, 'interrupted', ?, ?, ?, ?, ?)",
                (new_id, self.actor_id, src["request_json"], src["queue_json"], self.run_id, now_iso, now_iso),
            )
            for kv in self._conn.execute("SELECT key, value FROM kv WHERE key LIKE 'csv:%' OR key='run_meta'").fetchall():
                c.execute("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", (kv["key"], kv["value"]))
        if self.status() != "completed":
            self.set_status("superseded")
        return child