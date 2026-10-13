import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException

from app.api.dependencies.auth import get_current_user
from app.services.store import RunForbidden, RunNotFound, RunStore

logger = logging.getLogger(__name__)
router = APIRouter()


def _open_run(user_id: str, run_id: str) -> RunStore:
    try:
        return RunStore.open(user_id, run_id)
    except (RunNotFound, RunForbidden, ValueError):
        raise HTTPException(status_code=404, detail="Run not found.")  # 404 for both: don't reveal other users' run ids


@router.get("/api/runs")
def list_runs(current_user=Depends(get_current_user)) -> Dict[str, Any]:
    runs = [
        r for r in RunStore.list_runs(current_user.id)
        # a run that never processed a row (e.g. login expired on start) is noise, not something to resume
        if r["counts"] or r["status"] in ("completed", "running")
    ]
    return {"success": True, "runs": runs}


@router.get("/api/runs/{run_id}")
def get_run(run_id: str, current_user=Depends(get_current_user)) -> Dict[str, Any]:
    store = _open_run(current_user.id, run_id)
    try:
        return {"success": True, **store.summary(), "jobs": store.job_progress()}
    finally:
        store.close()


@router.post("/api/runs/{run_id}/cancel")
def cancel_run(run_id: str, current_user=Depends(get_current_user)) -> Dict[str, Any]:
    """
    A live run notices the flag at its next segment boundary and stops cleanly. A run with no live
    socket (crashed / interrupted) is marked cancelled immediately so it can no longer be resumed.
    """
    store = _open_run(current_user.id, run_id)
    try:
        if store.status() in ("completed", "cancelled", "failed"):
            raise HTTPException(status_code=409, detail=f"Run is already {store.status()}.")
        store.set_kv("cancel", "1")
        if store.effective_status() != "running":
            store.set_status("cancelled")
        return {"success": True, "status": store.status()}
    finally:
        store.close()