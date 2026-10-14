import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.api.dependencies.auth import get_current_user
from app.services.run_access import RunAccess
from app.services.store import RunForbidden, RunNotFound, RunStore

logger = logging.getLogger(__name__)
router = APIRouter()


def _open_run(user_id: str, run_id: str) -> RunStore:
    """Owner, or a colleague connected to the same org(s). 404 for everything else: don't reveal run ids."""
    try:
        return RunAccess.open(user_id, run_id)
    except (RunNotFound, RunForbidden, ValueError):
        raise HTTPException(status_code=404, detail="Run not found.")


def _editor(user) -> str:
    return getattr(user, "email", None) or str(user.id)


def _require_idle(store: RunStore) -> None:
    if store.effective_status() == "running":
        raise HTTPException(status_code=409, detail="This run is active in another session. Wait for it to stop, then edit.")


@router.get("/api/runs")
def list_runs(current_user=Depends(get_current_user)) -> Dict[str, Any]:
    """The caller's own runs."""
    runs = [
        r for r in RunStore.list_runs(current_user.id)
        # a run that never processed a row (e.g. login expired on start) is noise, not something to resume
        if r["counts"] or r["status"] in ("completed", "running")
    ]
    return {"success": True, "runs": runs}


@router.get("/api/runs/recoverable")
def recoverable_runs(current_user=Depends(get_current_user)) -> Dict[str, Any]:
    """
    Unfinished / failed runs on the org pair this user is connected to RIGHT NOW: their own, and runs another
    user of the same org left behind. Call this after login / when the migration page opens.
    """
    return {"success": True, "runs": RunAccess.recoverable(current_user.id)}


@router.get("/api/runs/{run_id}")
def get_run(run_id: str, current_user=Depends(get_current_user)) -> Dict[str, Any]:
    store = _open_run(current_user.id, run_id)
    try:
        summary = store.summary()
        summary["isMine"] = store.user_id == str(current_user.id)
        return {"success": True, **summary, "jobs": store.job_progress(), "jobColumns": store.job_columns()}
    finally:
        store.close()


@router.get("/api/runs/{run_id}/failures")
def get_failures(
    run_id: str, jobIdx: Optional[int] = None, state: str = "error", search: str = "",
    offset: int = 0, limit: int = 25, current_user=Depends(get_current_user),
) -> Dict[str, Any]:
    """Failed rows (or rows blocked by a failed parent) with their editable source record, plus error groups."""
    if state not in ("error", "blocked"):
        raise HTTPException(status_code=400, detail="state must be 'error' or 'blocked'.")
    offset, limit = max(0, offset), min(max(1, limit), 200)
    store = _open_run(current_user.id, run_id)
    try:
        total, rows = store.failed_rows(jobIdx, state, search.strip(), offset, limit)
        return {"success": True, "total": total, "rows": rows, "groups": store.failure_groups(jobIdx)}
    finally:
        store.close()


class RecordEdit(BaseModel):
    jobIdx: int
    rowIdx: int
    fields: Dict[str, Any]


class EditPayload(BaseModel):
    edits: List[RecordEdit] = Field(..., max_length=500)


class MassUpdatePayload(BaseModel):
    jobIdx: int
    field: str
    value: Any = ""
    errorContains: str = ""


@router.patch("/api/runs/{run_id}/records")
def edit_records(run_id: str, payload: EditPayload, current_user=Depends(get_current_user)) -> Dict[str, Any]:
    """Save corrected source values and re-queue those rows; the next Resume / Retry sends them."""
    store = _open_run(current_user.id, run_id)
    try:
        _require_idle(store)
        result = store.apply_edits([e.model_dump() for e in payload.edits], _editor(current_user))
        return {"success": True, **result}
    finally:
        store.close()


@router.post("/api/runs/{run_id}/records/mass-update")
def mass_update_records(run_id: str, payload: MassUpdatePayload, current_user=Depends(get_current_user)) -> Dict[str, Any]:
    """Set one field on EVERY failed row of a job whose error contains `errorContains` (e.g. a restricted picklist value)."""
    store = _open_run(current_user.id, run_id)
    try:
        _require_idle(store)
        result = store.mass_update(payload.jobIdx, payload.field, payload.value, payload.errorContains.strip(), _editor(current_user))
        return {"success": True, **result}
    finally:
        store.close()


@router.post("/api/runs/{run_id}/cancel")
def cancel_run(run_id: str, current_user=Depends(get_current_user)) -> Dict[str, Any]:
    """
    A live run notices the flag at its next segment boundary and stops cleanly. A run with no live socket
    (crashed / interrupted) is marked cancelled immediately. A cancelled run can still be resumed or restarted.
    """
    store = _open_run(current_user.id, run_id)
    try:
        if store.status() in ("completed", "cancelled", "failed", "superseded"):
            raise HTTPException(status_code=409, detail=f"Run is already {store.status()}.")
        store.set_kv("cancel", "1")
        store.set_stop_reason("cancelled", f"Cancelled by {_editor(current_user)}.")
        if store.effective_status() != "running":
            store.set_status("cancelled")
        return {"success": True, "status": store.status()}
    finally:
        store.close()