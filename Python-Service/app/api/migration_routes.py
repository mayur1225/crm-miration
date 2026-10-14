import asyncio
import httpx
import traceback
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends
from app.api.dependencies.auth import get_current_user
from app.services.validator_service import process_validation_batch
from app.utils.config import supabase
from app.services.crm_service import CrmService, is_crm_session_expired, crm_session_expired
from app.services.crm_query_service import CrmQueryService
import math
import csv
from fpdf import FPDF
from app.services.migrators.salesforce_migrator import SalesforceMigrator
from app.services.migrators.zoho_migrator import ZohoMigrator
from app.services.migrators.zendesk_migrator import ZendeskMigrator
from app.services.migrators.hubspot_migrator import HubspotMigrator
from app.services.migrators.salesforce_file_migrator import SalesforceFileMigrator
from app.services.file_migration_estimator import FileMigrationEstimator
from app.services.payload_builder import PayloadBuilderService
from app.services.audit_service import AuditService
from app.services.field_access_utils import find_non_writable_mapped_fields
from app.services.id_mapping_service import IdMappingService
from app.services.migrators.cross_crm_file_migrator import CrossCrmFileMigrator
from app.services.crm_picklist_sync_service import MultiCrmPicklistSyncService
from app.services.file_adapters.registry import supported_crms, effective_strategy
from app.services.picklist_sync_service import PicklistSyncService
from app.services.migrators.salesforce_file_migrator import FileMigrationCheckpoint
from app.services.resilience.retry import RetryingClient, ApiQuotaExhausted, CircuitOpen
from app.services.store import RunStore, RunNotFound, RunForbidden
from app.services.run_access import RunAccess
from app.services.org_identity import org_key
from app.services.resumable_runner import ResumableRunner, PassSpec, RunInterrupted

import uuid
import sqlite3
import urllib.parse
import os
import io
import json
import csv
from datetime import datetime
from fastapi.responses import StreamingResponse

import re

from app.services.staging_cleanup_service import cleanup_stale_staging_databases, DEFAULT_MAX_AGE_HOURS

logger = logging.getLogger(__name__)

router = APIRouter()
BASE_STAGING_DIR = os.path.join(os.getcwd(), "SureShift_staging_databases")

MIGRATORS = {
    "salesforce": SalesforceMigrator(),
    "zoho": ZohoMigrator(),
    "zendesk": ZendeskMigrator(),
    "hubspot": HubspotMigrator()
}

# Files/Attachments migration is Salesforce -> Salesforce only
FILE_MIGRATION_CRMS = supported_crms()
FILE_MIGRATOR = CrossCrmFileMigrator()
FILE_MIGRATION_ESTIMATOR = FileMigrationEstimator()
FILE_MIGRATION_DEFAULT_SAFETY_THRESHOLD = 0.90

_SESSION_ID_RE = re.compile(r'^[A-Za-z0-9_-]+$')

def _require_valid_session_id(session_id: str) -> str:
    if not session_id or not _SESSION_ID_RE.fullmatch(session_id):
        raise HTTPException(status_code=400, detail="Invalid session ID.")
    return session_id

# ==========================================
# CRM SESSION EXPIRY (shared by both websockets)
# ==========================================
def _note_stop(store, code: str, message: str) -> None:
    """Remember WHY a run stopped so the recovery panel can explain it to whoever opens it next."""
    try:
        if store is not None:
            store.set_stop_reason(code, str(message)[:400])
    except Exception:
        logger.exception("Could not record stop reason")


def _session_expired_payload(sides, failed_status: str, log: str = None) -> dict:
    """The frontend (isSocketAuthExpired) redirects to the Source/Target connection page on this."""
    return {
        "log": log or "Your CRM session has expired. Please log in again.",
        "status": "SessionExpired",
        "authExpired": True,
        "sessionExpired": True,
        "errorCode": "SESSION_EXPIRED",
        "expiredSides": list(sides),
        "failedStatus": failed_status,
    }


async def _resolve_live_credentials(websocket, user_id, pairs, failed_status: str):
    """
    pairs = [("source", "salesforce"), ("target", "zoho")]. Returns {side: creds} with live tokens,
    or None after telling the client which side(s) must log in again (socket is closed).
    """
    creds, expired_sides = {}, []
    for side, crm in pairs:
        try:
            creds[side] = await CrmService.ensure_valid_credentials(user_id, crm, side)
        except HTTPException as e:
            if is_crm_session_expired(e):
                expired_sides.append(side)
            else:
                raise
    if expired_sides:
        await websocket.send_json(_session_expired_payload(expired_sides, failed_status))
        await websocket.close()
        return None
    return creds


def get_db_path(session_id: str):
    session_id = _require_valid_session_id(session_id)
    crm_folder, obj_folder = _parse_session_id(session_id)
    target_dir = os.path.join(BASE_STAGING_DIR, crm_folder, obj_folder)
    os.makedirs(target_dir, exist_ok=True)
    return os.path.join(target_dir, f"{session_id}.db")

def _parse_session_id(session_id: str):
    """session_id is minted as f"{source_crm}_{safe_obj}_{timestamp}_{uuid}"
    (see the validation route below). Re-validation requests only carry the
    session_id, not the original source_crm/object name, so this recovers
    them from the id itself -- the same convention get_db_path already
    relies on for its staging folder layout."""
    parts = session_id.split('_')
    crm = parts[0] if len(parts) > 0 else "uncategorized"
    obj = parts[1] if len(parts) > 1 else "unknown_object"
    return crm, obj

def chunk_dataset(data: list, chunk_size: int = 5000):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def sort_jobs_by_dependency(jobs):
    sorted_jobs, pass3_jobs = [], []
    visited, visiting = set(), set()

    def visit(job):
        target_obj = job.get("targetObject")
        if target_obj in visited: return
        visiting.add(target_obj)

        dependencies = []
        for m in job.get("mappings", []):
            if m.get("type") == "reference" and m.get("referenceTo"):
                dependencies.extend(m.get("referenceTo"))

        defer_references_to = []
        for dep in dependencies:
            parent_job = next((j for j in jobs if j.get("targetObject") == dep), None)
            if parent_job:
                if dep in visiting:
                    defer_references_to.append(dep)
                else:
                    visit(parent_job)

        visiting.remove(target_obj)
        visited.add(target_obj)

        job["deferReferencesTo"] = defer_references_to
        sorted_jobs.append(job)

        if defer_references_to:
            pass3_patch = dict(job)
            pass3_patch["isPass3Patch"] = True
            pass3_patch["onlyReferencesTo"] = defer_references_to
            pass3_jobs.append(pass3_patch)

    for job in jobs: visit(job)
    return sorted_jobs + pass3_jobs


# ==========================================
# FILE/ATTACHMENT MIGRATION API-BUDGET GATE
# ==========================================
async def resolve_file_migration_scope(
    websocket: WebSocket, client, source_creds, target_creds, user_id: str,
    job_id_map: dict, target_object: str, job_index: int,
    migrate_attachments: bool, migrate_files: bool, send_log,
    source_crm: str = "salesforce", target_crm: str = "salesforce", source_object: str = "",
) -> dict:
    """
    Runs the pre-flight API budget estimate for this job's file/attachment pass and,
    if it doesn't fit the current daily allocation on either org, pauses the websocket
    and waits for the client to acknowledge a decision before any download/upload call
    is made. Returns the (possibly reduced) job_id_map to actually migrate, or an
    empty dict if the user chose to skip.
    """
    await send_log(f"[{target_object}] Calculating file migration API budget...")

    try:
        estimate = await FILE_MIGRATION_ESTIMATOR.estimate(
            client, source_creds, target_creds, user_id, list(job_id_map.keys()),
            migrate_attachments, migrate_files, send_log,
            safety_threshold=FILE_MIGRATION_DEFAULT_SAFETY_THRESHOLD,
            source_crm=source_crm, target_crm=target_crm, source_object=source_object,
            strategy=effective_strategy(source_crm, target_crm, "bulk_zip"),
        )
    except Exception as e:
        await send_log(
            f"[{target_object}] Could not calculate API budget ({str(e)}) -- "
            f"proceeding without a pre-flight estimate."
        )
        return job_id_map

    if estimate.fits_in_budget:
        await send_log(
            f"[{target_object}] Budget check passed: ~{estimate.estimated_total_calls:,} API calls "
            f"needed for {estimate.total_file_count:,} file(s), well within today's allocation."
        )
        return job_id_map

    job_ref = f"{target_object}#{job_index}"
    await websocket.send_json({
        "status": "AwaitingFileMigrationApproval",
        "log": f"[{target_object}] {estimate.message}",
        "fileMigrationBudget": {
            "jobRef": job_ref,
            "targetObject": target_object,
            "message": estimate.message,
            "bindingOrg": estimate.binding_org,
            "estimatedDownloadCalls": estimate.estimated_download_calls,
            "estimatedUploadCalls": estimate.estimated_upload_calls,
            "estimatedTotalCalls": estimate.estimated_total_calls,
            "totalRecordCount": estimate.total_record_count,
            "safeRecordCount": estimate.safe_record_count,
            "totalFileCount": estimate.total_file_count,
            "safeFileCount": estimate.safe_file_count,
            "sourceAvailable": estimate.source_budget.available_calls,
            "targetAvailable": estimate.target_budget.available_calls,
        },
    })

    decision_msg = await websocket.receive_json()
    decision = (decision_msg.get("fileMigrationDecision") or {})
    action = decision.get("action")
    matched_ref = decision.get("jobRef")

    if matched_ref and matched_ref != job_ref:
        await send_log(
            f"[{target_object}] File migration decision did not match the pending job "
            f"(expected '{job_ref}', got '{matched_ref}') -- skipping this pass for safety."
        )
        return {}

    if action == "proceed_limited":
        included_ids, excluded_ids = FILE_MIGRATION_ESTIMATOR.select_batch_within_budget(
            list(job_id_map.keys()), estimate.safe_record_count
        )
        await send_log(
            f"[{target_object}] Proceeding with {len(included_ids):,} of {len(job_id_map):,} record(s) "
            f"({len(excluded_ids):,} deferred to a future run once the daily allocation resets)."
        )
        return {oid: job_id_map[oid] for oid in included_ids}

    elif action == "proceed_full":
        await send_log(
            f"[{target_object}] Proceeding with the FULL scope despite exceeding the estimated safe "
            f"budget, per explicit user override. This may hit Salesforce's daily API limit mid-run."
        )
        return job_id_map

    else:
        await send_log(f"[{target_object}] File migration skipped for this pass by user decision.")
        return {}


# ==========================================
# CORE MIGRATION ROUTE (checkpointed / resumable)
# ==========================================
class _SafeSocket:
    """
    A vanished client flips `gone` instead of raising in the middle of a CRM call. The runner checks
    `gone` at segment boundaries, so every interruption lands on a clean checkpoint.
    """

    def __init__(self, ws: WebSocket):
        self._ws = ws
        self.gone = False

    async def send_json(self, data):
        if self.gone:
            return
        try:
            await self._ws.send_json(data)
        except Exception:
            self.gone = True

    async def receive_json(self):
        try:
            return await self._ws.receive_json()
        except Exception:
            self.gone = True
            raise WebSocketDisconnect()

    async def close(self):
        try:
            await self._ws.close()
        except Exception:
            pass


def _ui_safe(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value) if value is not None else ""


def _format_results_for_ui(success, errors, skipped):
    safe_success = [{k: _ui_safe(v) for k, v in rec.items()} for rec in success]

    formatted_errors = []
    for err in errors:
        if not isinstance(err, dict):
            err = {"Raw_Data": str(err), "Target_Error": "Unknown error format."}
        formatted_errors.append({
            "record": {k: _ui_safe(v) for k, v in err.items() if k != "Target_Error"},
            "error": err.get("Target_Error", "API rejected this record."),
        })

    formatted_skipped = []
    for rec in skipped:
        if not isinstance(rec, dict):
            rec = {"Raw_Data": str(rec), "Target_SkipReason": "Unknown skip reason."}
        formatted_skipped.append({
            "record": {k: _ui_safe(v) for k, v in rec.items() if k != "Target_SkipReason"},
            "reason": rec.get("Target_SkipReason", "Skipped: no matching record found."),
        })
    return safe_success, formatted_errors, formatted_skipped


@router.websocket("/ws/migrate")
async def websocket_migration(ws: WebSocket):
    """
    mode = "start" (default) | "resume" | "retry_failed"
      start        payload: {queue|job fields, authToken}                -> new run, replies {runId} first
      resume       payload: {authToken, runId, mode:"resume"}            -> continues an interrupted run
      retry_failed payload: {authToken, runId, mode:"retry_failed"}      -> child run of a completed run, failed rows only
      restart      payload: {authToken, runId, mode:"restart", acknowledgeDuplicates?} -> fresh run, same configuration
    The run may belong to a colleague connected to the same org(s); RunAccess authorizes by org identity.
    """
    await ws.accept()
    websocket = _SafeSocket(ws)
    store = None
    heartbeat_task = None
    lock_owner = uuid.uuid4().hex
    final_status = "interrupted"
    lock_held = False

    try:
        await websocket.send_json({"log": "System: Multi-Object Engine Initialized.", "status": "Initializing..."})
        payload = await websocket.receive_json()

        mode = str(payload.get("mode") or "start").lower()
        if mode not in ("start", "resume", "retry_failed", "restart"):
            await websocket.send_json({"log": f"FATAL: Unknown mode '{mode}'.", "status": "Failed"})
            await websocket.close()
            return

        raw_queue = payload.get("queue") or [payload]
        auth_token = payload.get("authToken") or raw_queue[0].get("authToken")

        user_res = supabase.auth.get_user(auth_token)
        if not user_res or not user_res.user:
            await websocket.send_json({"log": "FATAL: Invalid or expired session.", "status": "Failed"})
            await websocket.close()
            return

        user_id = user_res.user.id
        user_email = getattr(user_res.user, "email", None)
        user_name = (getattr(user_res.user, "user_metadata", None) or {}).get("full_name")

        # ---------------- run lifecycle ----------------
        if mode == "start":
            execution_queue = sort_jobs_by_dependency(raw_queue)
            store = RunStore.create(user_id, payload, execution_queue)
        else:
            try:
                parent = RunAccess.open(user_id, str(payload.get("runId") or ""))
            except (RunNotFound, RunForbidden, ValueError):
                await websocket.send_json({"log": "FATAL: Run not found.", "status": "Failed"})
                await websocket.close()
                return

            if mode == "resume":
                if parent.status() in ("completed", "failed", "superseded"):
                    await websocket.send_json({"log": f"FATAL: A {parent.status()} run cannot be resumed.", "status": "Failed"})
                    parent.close()
                    await websocket.close()
                    return
                parent.delete_kv("cancel")        # a cancelled run may be resumed from its checkpoint
                parent.clear_stop_reason()
                store = parent
            elif mode == "restart":
                try:
                    store = parent.fork_for_restart(acknowledge_duplicates=bool(payload.get("acknowledgeDuplicates")))
                except ValueError as e:
                    await websocket.send_json({"log": f"FATAL: {e}", "status": "Failed"})
                    parent.close()
                    await websocket.close()
                    return
                finally:
                    parent.close()
            else:
                try:
                    if parent.status() != "completed":
                        raise ValueError("Only a completed run can be retried; resume interrupted runs instead.")
                    store = parent.fork_for_retry()
                except ValueError as e:
                    await websocket.send_json({"log": f"FATAL: {e}", "status": "Failed"})
                    parent.close()
                    await websocket.close()
                    return
                finally:
                    parent.close()
            execution_queue = store.load_queue()

        if not store.acquire_lock(lock_owner):
            await websocket.send_json({"log": "FATAL: This run is already active in another session.", "status": "Failed"})
            await websocket.close()
            return
        lock_held = True

        async def _heartbeat():
            while True:
                await asyncio.sleep(10)
                store.heartbeat(lock_owner)

        heartbeat_task = asyncio.create_task(_heartbeat())
        await websocket.send_json({
            "log": f"System: Run {store.run_id} ({mode}).", "status": "Running", "runId": store.run_id, "mode": mode,
        })

        source_crm = execution_queue[0].get("sourceCrmId", "zendesk").lower()
        target_crm = execution_queue[0].get("targetCrmId", "salesforce").lower()

        pairs = ([("source", source_crm)] if source_crm != "csv" else []) + [("target", target_crm)]
        live = await _resolve_live_credentials(websocket, user_id, pairs, "Failed")
        if live is None:
            return  # client was told to log in again; the run stays resumable

        source_creds = live.get("source")
        source_migrator = MIGRATORS.get(source_crm) if source_crm != "csv" else None
        target_creds = live["target"]
        target_migrator = MIGRATORS.get(target_crm)

        async def send_log(msg: str, status: str = "Running"):
            await websocket.send_json({"log": msg, "status": status})

        def should_stop() -> bool:
            return websocket.gone or store.get_kv("cancel") == "1"

        source_instance = (source_creds or {}).get("instance_url") or (source_creds or {}).get("api_domain") or (source_creds or {}).get("subdomain")
        target_instance = target_creds.get("instance_url") or target_creds.get("api_domain") or target_creds.get("subdomain")

        # ---------------- org identity, ownership, hand-over ----------------
        run_meta = store.run_meta()
        if mode in ("start", "restart") or not run_meta.get("targetOrg"):
            store.set_run_meta(
                startedBy={"id": str(user_id), "name": user_name or user_email, "email": user_email},
                sourceCrm=source_crm, targetCrm=target_crm,
                sourceOrg=org_key(source_crm, source_creds) if source_creds else None,
                targetOrg=org_key(target_crm, target_creds),
                jobTargets=[q.get("targetObject") for q in execution_queue],
            )
        store.add_participant(user_id, user_name or user_email, mode)

        if store.user_id != str(user_id) and mode in ("resume", "retry_failed"):
            # A colleague is continuing someone else's run. Reference remapping reads the ACTING user's id map,
            # so copy over the source->target ids this run already created.
            for j, q in enumerate(execution_queue):
                pairs = [
                    ((r.get("Id") or r.get("id")), r.get("Target_Id"))
                    for r in store.job_success_records(j)
                    if (r.get("Id") or r.get("id")) and r.get("Target_Id")
                ]
                if pairs:
                    IdMappingService.save_mappings(
                        user_id, source_crm, target_crm, q.get("targetObject"), pairs,
                        source_instance=source_instance, target_instance=target_instance,
                    )

        async with RetryingClient(httpx.AsyncClient(timeout=120.0)) as client:
            runner = ResumableRunner(store, client, send_log, should_stop)

            for job_index, job in enumerate(execution_queue):
                target_object = job.get("targetObject")
                source_object = job.get("sourceObject", "")
                extraction_query = job.get("extractionQuery", "").strip()
                mappings = [m for m in job.get("mappings", []) if m.get("targetField") or m.get("sfField")]
                op_mode = job.get("operationMode", "insert")
                batch_size = int(job.get("batchSize", 5000))
                ext_id_field = job.get("externalIdField") or job.get("targetExtIdField", "")
                time_filter = job.get("migrationTimeFilter")
                migrate_attachments = job.get("migrateAttachments", False)
                migrate_files = job.get("migrateFiles", False)

                if not mappings or store.is_job_done(job_index):
                    continue
                if should_stop():
                    raise RunInterrupted()

                if migrate_attachments or migrate_files:
                    await send_log(
                        f"[{target_object}] File/attachment migration requested "
                        f"(Attachments={migrate_attachments}, Files={migrate_files}, "
                        f"source={source_crm}, target={target_crm})."
                    )

                sf_rules = job.get("sfRules", {})
                fls_violations = find_non_writable_mapped_fields(mappings, sf_rules, op_mode)
                if fls_violations:
                    final_status = "failed"
                    await websocket.send_json({
                        "log": f"[{target_object}] FATAL: {len(fls_violations)} mapped field(s) can't be "
                               f"written by the connected {target_crm.capitalize()} user "
                               f"({', '.join(v['label'] for v in fls_violations)}). Remove them from the "
                               f"mapping (or connect a user with field-level write access) and re-validate "
                               f"before running.",
                        "status": "Failed",
                        "fieldAccessErrors": fls_violations,
                    })
                    await websocket.close()
                    return

                # ---------------- extract once, then freeze ----------------
                if store.is_extracted(job_index):
                    source_records = store.load_extracted(job_index)
                    await send_log(f"[{target_object}] Using {len(source_records)} frozen source record(s) from the checkpoint.")
                else:
                    if source_crm == "salesforce" and target_crm == "salesforce":
                        picklist_sync_mappings = [m for m in mappings if m.get("syncPicklistValues")]
                        if picklist_sync_mappings:
                            sync_result = await PicklistSyncService.sync(
                                client, source_creds, target_creds, user_id,
                                source_object, target_object, picklist_sync_mappings, send_log,
                            )
                            if sync_result["failed"]:
                                final_status = "failed"
                                await websocket.send_json({
                                    "log": f"[{target_object}] FATAL: picklist sync failed for "
                                           f"{', '.join(sync_result['failed'])}. Aborting before data load.",
                                    "status": "Failed",
                                })
                                await websocket.close()
                                return

                    source_records, actual_query_used = [], None
                    session_id = job.get("sessionId")
                    if source_crm == "csv":
                        source_records = store.get_csv_rows(job_index)
                        source_mode = "CSV Upload"
                        await send_log(f"[{target_object}] Loaded {len(source_records)} records from CSV payload.")
                    elif session_id:
                        db_path = get_db_path(session_id)
                        conn = sqlite3.connect(db_path)
                        cursor = conn.cursor()
                        cursor.execute("SELECT data FROM records WHERE is_valid = 1")
                        source_records = [json.loads(row[0]) for row in cursor.fetchall()]
                        try:
                            cursor.execute("SELECT value FROM session_meta WHERE key = 'query_used'")
                            meta_row = cursor.fetchone()
                            if meta_row:
                                actual_query_used = meta_row[0]
                        except sqlite3.OperationalError:
                            pass
                        conn.close()
                        source_mode = "Staged (Pre-Validated) Records"
                        await send_log(f"[{target_object}] Loaded {len(source_records)} valid records from staging.")
                    else:
                        source_mode = "Direct API Sync"
                        await send_log(f"[{target_object}] Direct API extraction from {source_crm.capitalize()}...")
                        extract_result = await source_migrator.extract(
                            client, source_creds, source_object, extraction_query, mappings, send_log, time_filter
                        )
                        if isinstance(extract_result, tuple):
                            source_records, actual_query_used = extract_result
                        else:
                            source_records = extract_result

                    await IdMappingService.remap_reference_fields(
                        source_records, mappings, user_id, source_crm, target_crm,
                        source_instance=source_instance, target_instance=target_instance, send_log=send_log,
                    )
                    store.set_job_meta(job_index, sessionId=session_id, sourceMode=source_mode, actualQueryUsed=actual_query_used)
                    store.save_extracted(job_index, source_records)  # the freeze point: resume never re-extracts

                # ---------------- plan passes ----------------
                options_base = {
                    "targetObject": target_object, "targetExtIdField": ext_id_field, "operationMode": op_mode,
                    "token": target_creds.get("access_token"), "instance_url": target_instance,
                    "batchSize": batch_size, "userId": user_id,
                }
                has_self_ref = any(
                    m.get("type") == "reference" and target_object in (m.get("referenceTo") or []) for m in mappings
                )
                base_opts = {"targetObject": target_object, "targetExtIdField": ext_id_field}
                defer = job.get("deferReferencesTo", [])

                if job.get("isPass3Patch", False):
                    plan = [(3, "Pass 3 (Patch)", "upsert", {**base_opts, "onlyReferencesTo": job.get("onlyReferencesTo", []), "operationMode": "upsert"})]
                elif op_mode == "delete":
                    plan = [(1, "Deletion", "delete", {"targetObject": target_object, "operationMode": "delete"})]
                elif has_self_ref:
                    plan = [
                        (1, "Pass 1", op_mode, {**base_opts, "excludeReferencesTo": defer, "skipSelfReferencing": True, "operationMode": op_mode}),
                        (2, "Pass 2", "upsert", {**base_opts, "onlySelfReferencing": True, "operationMode": "upsert"}),
                    ]
                else:
                    plan = [(1, "Standard Sync", op_mode, {**base_opts, "excludeReferencesTo": defer, "operationMode": op_mode})]

                parent_job_idx = None
                if job.get("isPass3Patch", False):
                    parent_job_idx = next(
                        (i for i, q in enumerate(execution_queue) if q.get("targetObject") == target_object and not q.get("isPass3Patch")),
                        None,
                    )

                segment_size = batch_size if target_crm == "salesforce" else min(batch_size, 100)
                segment_size = max(1, segment_size)

                for pass_no, pass_name, pass_op, builder_opts in plan:
                    payload_data = PayloadBuilderService.build_payload(source_records, mappings, builder_opts, target_crm)
                    dedupe_key = ""
                    if pass_op in ("update", "upsert") and ext_id_field:
                        dedupe_key = "external_id" if target_crm == "zendesk" else ext_id_field

                    await runner.run_pass(PassSpec(
                        job_idx=job_index, pass_no=pass_no, pass_name=pass_name, op_mode=pass_op,
                        target_object=target_object, ext_id_field=ext_id_field, segment_size=segment_size,
                        payload=payload_data, source_records=source_records, migrator=target_migrator,
                        options_base=options_base, dedupe_key_field=dedupe_key,
                        correlation_field=(job.get("correlationField") or "") if pass_op == "insert" else "",
                        parent_job_idx=(job_index if pass_no == 2 else parent_job_idx if pass_no == 3 else None),
                    ))

                # ---------------- id mapping ----------------
                job_success_records = store.job_success_records(job_index)
                job_id_map = {
                    (rec.get("Id") or rec.get("id")): rec.get("Target_Id")
                    for rec in job_success_records
                    if (rec.get("Id") or rec.get("id")) and rec.get("Target_Id")
                }
                if job_id_map:
                    saved_count = IdMappingService.save_mappings(
                        user_id, source_crm, target_crm, target_object, list(job_id_map.items()),
                        source_instance=source_instance, target_instance=target_instance,
                    )
                    await send_log(f"[{target_object}] Saved {saved_count} source->target Id mapping(s) for future reference lookups.")

                # ---------------- files & attachments (any CRM pair with file adapters) ----------------
                if (migrate_attachments or migrate_files) and not store.files_done(job_index):
                    if job.get("isPass3Patch", False):
                        await send_log(f"[{target_object}] File migration skipped for this pass (reference patch pass, not the primary sync).")
                    elif source_crm not in FILE_MIGRATION_CRMS or target_crm not in FILE_MIGRATION_CRMS:
                        await send_log(f"[{target_object}] File migration skipped: {source_crm} -> {target_crm} is not supported (supported: {', '.join(sorted(FILE_MIGRATION_CRMS))}).")
                    elif not job_id_map:
                        await send_log(f"[{target_object}] File migration skipped: no source Id was found on synced records.")
                    else:
                        scoped_id_map = await resolve_file_migration_scope(
                            websocket, client, source_creds, target_creds, user_id,
                            job_id_map, target_object, job_index,
                            migrate_attachments, migrate_files, send_log,
                            source_crm=source_crm, target_crm=target_crm, source_object=source_object,
                        )
                        if not scoped_id_map:
                            await send_log(f"[{target_object}] No records in scope for file migration -- skipping this pass.")
                        else:
                            await send_log(f"[{target_object}] Starting file/attachment migration for {len(scoped_id_map)} synced record(s)...")
                            saved = store.get_file_checkpoint(job_index)
                            checkpoint = FileMigrationCheckpoint(
                                migrated_attachment_ids=set(saved["attachments"]),
                                migrated_content_version_ids=set(saved["contentVersions"]),
                            )
                            try:
                                file_results = await FILE_MIGRATOR.migrate_files_for_batch(
                                    client, source_creds, target_creds, str(user_id), scoped_id_map,
                                    migrate_attachments, migrate_files, send_log,
                                    source_crm=source_crm, target_crm=target_crm,
                                    source_object=source_object, target_object=target_object,
                                    strategy=effective_strategy(source_crm, target_crm, "bulk_zip"),
                                    checkpoint=checkpoint,
                                )
                            finally:
                                # also runs when the pass is interrupted, so a resume skips files already copied
                                store.save_file_checkpoint(
                                    job_index, checkpoint.migrated_attachment_ids, checkpoint.migrated_content_version_ids
                                )
                            await send_log(
                                f"[{target_object}] Files complete — "
                                f"Attachments: {file_results['attachments']['success']} ok / {file_results['attachments']['error']} failed, "
                                f"Files: {file_results['files']['success']} ok / {file_results['files']['error']} failed."
                            )
                            await websocket.send_json({
                                "status": "Running",
                                "fileMigrationJobResult": {
                                    "targetObject": target_object,
                                    "attachmentsSuccess": file_results["attachments"]["success"],
                                    "attachmentsError": file_results["attachments"]["error"],
                                    "filesSuccess": file_results["files"]["success"],
                                    "filesError": file_results["files"]["error"],
                                    "deferredRecordCount": max(0, len(job_id_map) - len(scoped_id_map)),
                                },
                            })
                            if file_results.get("apiLimitReached"):
                                raise ApiQuotaExhausted("The daily API limit was reached during file migration.")
                    store.mark_files_done(job_index)

                store.mark_job_done(job_index)

            # ---------------- final report: latest state of every row, across resumes ----------------
            await websocket.send_json({"log": "QUEUE COMPLETE! Building final payload...", "status": "Processing"})
            success, errors, skipped = store.collect_results()
            safe_success_data, formatted_errors, formatted_skipped = _format_results_for_ui(success, errors, skipped)

            last_idx = len(execution_queue) - 1
            last_job = execution_queue[last_idx]
            last_meta = store.get_job_meta(last_idx)
            report_urls = {}
            try:
                await websocket.send_json({"log": "Generating Audit Reports (PDF & CSV)...", "status": "Finalizing"})
                report_urls = AuditService.generate_and_save_reports(
                    user_id=user_id,
                    session_id=last_meta.get("sessionId") or f"direct_{str(uuid.uuid4())[:8]}",
                    source_crm=source_crm,
                    target_crm=target_crm,
                    target_object=last_job.get("targetObject"),
                    op_mode=last_job.get("operationMode", "insert"),
                    auth_token=auth_token,
                    success_data=safe_success_data,
                    error_data=formatted_errors,
                    extraction_query=(last_job.get("extractionQuery") or "").strip(),
                    time_filter=last_job.get("migrationTimeFilter"),
                    user_email=user_email,
                    user_name=user_name,
                    source_mode=last_meta.get("sourceMode", "Direct API Sync"),
                    actual_query_used=last_meta.get("actualQueryUsed"),
                )
            except Exception:
                logger.exception("Failed to generate/persist migration reports for run %s", store.run_id)
                report_urls = {}

            final_status = "completed"
            store.clear_stop_reason()
            store.set_status("completed")
            await websocket.send_json({
                "status": "Finished",
                "log": f"Migration completed! {len(safe_success_data)} records successful, "
                       f"{len(formatted_skipped)} skipped (no match), {len(formatted_errors)} failed.",
                "successData": safe_success_data,
                "errorData": formatted_errors,
                "skippedData": formatted_skipped,
                "reportUrls": report_urls,
                "runId": store.run_id,
                "retryable": len(formatted_errors) > 0,
            })
        await websocket.close()

    except (WebSocketDisconnect, RunInterrupted):
        logger.info("Run %s interrupted (client left or cancelled); checkpoint is intact.", getattr(store, "run_id", None))
        if store is not None and store.get_kv("cancel") != "1":
            _note_stop(store, "client_left", "The browser tab was closed or the connection dropped. Everything finished so far is saved.")
        if store is not None and store.get_kv("cancel") == "1":
            await websocket.send_json({"log": "Run cancelled.", "status": "Cancelled", "runId": store.run_id})
        await websocket.close()
    except (ApiQuotaExhausted, CircuitOpen) as e:
        _note_stop(store, "quota" if isinstance(e, ApiQuotaExhausted) else "crm_unavailable", str(e))
        await websocket.send_json({
            "log": f"Run paused: {e} Everything finished so far is saved -- resume once the limit resets or the CRM recovers.",
            "status": "Interrupted", "runId": store.run_id if store else None, "resumable": True,
        })
        await websocket.close()
    except HTTPException as e:
        _note_stop(store, "session_expired" if is_crm_session_expired(e) else "error",
                   "CRM login expired; log in again to the same org and resume." if is_crm_session_expired(e) else e.detail)
        if is_crm_session_expired(e):
            side = (e.detail or {}).get("side")
            await websocket.send_json(_session_expired_payload([side] if side else ["source", "target"], "Failed"))
        else:
            await websocket.send_json({
                "log": f"SYSTEM CRASH: {e.detail}. Progress is saved; you can resume this run.",
                "status": "Interrupted", "runId": store.run_id if store else None, "resumable": store is not None,
            })
        await websocket.close()
    except Exception as e:
        traceback.print_exc()
        _note_stop(store, "error", str(e))
        await websocket.send_json({
            "log": f"SYSTEM CRASH: {str(e)}. Progress is saved; you can resume this run.",
            "status": "Interrupted", "runId": store.run_id if store else None, "resumable": store is not None,
        })
        await websocket.close()
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
        if store is not None:
            try:
                if lock_held:
                    if final_status == "interrupted" and store.get_kv("cancel") == "1":
                        final_status = "cancelled"
                    store.set_status(final_status)
                    store.release_lock(lock_owner)
            finally:
                store.close()


# ==========================================
# MASSIVE STREAMING VALIDATION (WS)
# ==========================================
@router.websocket("/ws/validate-stream")
async def websocket_validate_stream(websocket: WebSocket):
    await websocket.accept()
    
    try:
        payload = await websocket.receive_json()
        auth_token = payload.get("authToken")
        user_res = supabase.auth.get_user(auth_token)
        if not user_res or not user_res.user:
            await websocket.send_json({"log": "FATAL: Invalid or expired session.", "status": "Validation Failed"})
            await websocket.close()
            return
        user_id = user_res.user.id
        user_email = getattr(user_res.user, "email", None)
        user_name = (getattr(user_res.user, "user_metadata", None) or {}).get("full_name")

        is_revalidation = payload.get("isRevalidation", False)
        session_id = payload.get("sessionId", "")

        # --- RE-VALIDATION ROUTE ---
        if is_revalidation and session_id:
            db_path = get_db_path(session_id) 
            if not os.path.exists(db_path):
                await websocket.send_json({"log": "Error: Staging session expired.", "status": "Validation Failed"})
                await websocket.close()
                return

            fixed_records = payload.get("fixedRecords", [])
            query = payload.get("query", "").strip()
            mappings = payload.get("mappings", [])
            dedupe_key = payload.get("dedupeKey", "")
            sf_rules = payload.get("sfRules", {})
            target_crm = payload.get("targetCrmId", "salesforce").lower()
            time_filter = payload.get("migrationTimeFilter")
            op_mode = payload.get("operationMode", "insert")

            fls_violations = find_non_writable_mapped_fields(mappings, sf_rules, op_mode)
            if fls_violations:
                await websocket.send_json({
                    "log": f"Field-Level Access Denied: {len(fls_violations)} mapped field(s) can't be "
                           f"written by the connected {target_crm.capitalize()} user.",
                    "status": "Validation Failed",
                    "fieldAccessErrors": fls_violations
                })
                await websocket.close()
                return

            await websocket.send_json({"log": "System: Re-validating UI fixes...", "status": "Validating"})

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            db_ids_to_delete = [rec.pop("_db_id") for rec in fixed_records if "_db_id" in rec]

            chunk_result = process_validation_batch(fixed_records, mappings, dedupe_key, sf_rules, "", target_crm)

            if db_ids_to_delete:
                placeholders = ','.join(['?'] * len(db_ids_to_delete))
                cursor.execute(f"DELETE FROM records WHERE id IN ({placeholders})", db_ids_to_delete)

            valid_inserts = [(True, json.dumps(rec), "") for rec in chunk_result.get("validRecords", [])]
            invalid_inserts = [(False, json.dumps(rec["originalRow"]), rec["errors"]) for rec in chunk_result.get("invalidRecords", [])]

            conn.executemany("INSERT INTO records (is_valid, data, errors) VALUES (?, ?, ?)", valid_inserts)
            conn.executemany("INSERT INTO records (is_valid, data, errors) VALUES (?, ?, ?)", invalid_inserts)
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM records")
            total_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM records WHERE is_valid = 1")
            valid_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM records WHERE is_valid = 0")
            invalid_count = cursor.fetchone()[0]

            cursor.execute("SELECT id, data, errors FROM records WHERE is_valid = 0 LIMIT 500")
            all_invalid_records = [{"originalRow": dict(json.loads(row[1]), _db_id=row[0]), "errors": row[2]} for row in cursor.fetchall()]

            # Full (uncapped) invalid set for the permanent report/CSV -- the
            # 500-row query above is just the UI preview.
            cursor.execute("SELECT id, data, errors FROM records WHERE is_valid = 0")
            full_invalid_records = [{"originalRow": dict(json.loads(row[1]), _db_id=row[0]), "errors": row[2]} for row in cursor.fetchall()]
            conn.close()

            validation_stats = {"total": total_count, "valid": valid_count, "invalid": invalid_count, "duplicates": chunk_result["stats"].get("duplicates", 0)}

            try:
                parsed_source_crm, parsed_object = _parse_session_id(session_id)
                AuditService.generate_and_save_validation_report(
                    user_id=user_id,
                    session_id=session_id,
                    source_crm=parsed_source_crm,
                    target_crm=target_crm,
                    target_object=parsed_object,
                    stats=validation_stats,
                    invalid_records=full_invalid_records,
                    auth_token=auth_token,
                    staging_db_path=db_path,
                    user_email=user_email,
                    user_name=user_name,
                )
            except Exception as e:
                logger.exception("Failed to persist re-validation history for session %s", session_id)

            await websocket.send_json({
                "log": f"Re-validation Complete: Fixed {len(valid_inserts)} records.",
                "status": "Validation Passed" if invalid_count == 0 else "Validation Warning",
                "stats": validation_stats,
                "invalidRecords": all_invalid_records,
                "sessionId": session_id
            })
            await websocket.close()
            return

        # --- INITIAL VALIDATION ROUTE (DYNAMIC) ---
        source_crm = payload.get("crmId", "").lower()
        target_crm = payload.get("targetCrmId", "salesforce").lower()
        obj_name = payload.get("objectName", "")
        query = payload.get("query", "").strip()
        mappings = payload.get("mappings", [])
        dedupe_key = payload.get("dedupeKey", "")
        sf_rules = payload.get("sfRules", {})
        time_filter = payload.get("migrationTimeFilter")
        op_mode = payload.get("operationMode", "insert")

        fls_violations = find_non_writable_mapped_fields(mappings, sf_rules, op_mode)
        if fls_violations:
            await websocket.send_json({
                "log": f"Field-Level Access Denied: {len(fls_violations)} mapped field(s) can't be "
                       f"written by the connected {target_crm.capitalize()} user.",
                "status": "Validation Failed",
                "fieldAccessErrors": fls_violations
            })
            await websocket.close()
            return

        live = await _resolve_live_credentials(websocket, user_id, [("source", source_crm)], "Validation Failed")
        if live is None:
            return  # client was told to log in again
        source_creds = live["source"]
        source_migrator = MIGRATORS.get(source_crm)

        async def send_log(msg: str, status: str = "Running"):
            await websocket.send_json({"log": msg, "status": status})

        safe_obj = ''.join(e for e in obj_name if e.isalnum()).lower() or "unknown"
        session_id = f"{source_crm}_{safe_obj}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        
        db_path = get_db_path(session_id)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY AUTOINCREMENT, is_valid BOOLEAN, data TEXT, errors TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS session_meta (key TEXT PRIMARY KEY, value TEXT)")
        
        aggregate_stats = {"total": 0, "valid": 0, "invalid": 0, "duplicates": 0}
        await send_log(f"System: Initializing Streaming Validation...", "Connecting")

        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                # 1. DYNAMIC API EXTRACTION
                extract_result = await source_migrator.extract(
                    client, source_creds, obj_name, query, mappings, send_log, time_filter
                )

                if isinstance(extract_result, tuple):
                    raw_records, validation_query_used = extract_result
                else:
                    raw_records, validation_query_used = extract_result, None

                if validation_query_used:
                    conn.execute(
                        "INSERT OR REPLACE INTO session_meta (key, value) VALUES ('query_used', ?)",
                        (validation_query_used,),
                    )
                    conn.commit()
                
                if not raw_records:
                    await send_log("No records found matching criteria.", "Validation Passed")
                    await websocket.close()
                    return
                
                # 2. BATCH VALIDATE AND DB INSERTION
                chunks = list(chunk_dataset(raw_records, 1000))
                for chunk in chunks:
                    chunk_result = process_validation_batch(chunk, mappings, dedupe_key, sf_rules, "", target_crm)
                    
                    valid_inserts = [(True, json.dumps(rec), "") for rec in chunk_result.get("validRecords", [])]
                    invalid_inserts = [(False, json.dumps(rec["originalRow"]), rec["errors"]) for rec in chunk_result.get("invalidRecords", [])]
        
                    conn.executemany("INSERT INTO records (is_valid, data, errors) VALUES (?, ?, ?)", valid_inserts)
                    conn.executemany("INSERT INTO records (is_valid, data, errors) VALUES (?, ?, ?)", invalid_inserts)
                    conn.commit()
                    
                    aggregate_stats["total"] += chunk_result["stats"]["total"]
                    aggregate_stats["valid"] += chunk_result["stats"]["valid"]
                    aggregate_stats["invalid"] += chunk_result["stats"]["invalid"]
                    aggregate_stats["duplicates"] += chunk_result["stats"]["duplicates"]

                    await send_log(f"Validated {aggregate_stats['total']} records so far...", "Validating")

            except Exception as e:
                await send_log(f"Validation Extraction Failed: {str(e)}", "Validation Failed")
                await websocket.close()
                return

        # 3. INITIAL UI ERRORS
        cursor = conn.cursor()
        cursor.execute("SELECT id, data, errors FROM records WHERE is_valid = 0 LIMIT 500")
        all_invalid_records = [{"originalRow": dict(json.loads(row[1]), _db_id=row[0]), "errors": row[2]} for row in cursor.fetchall()]


        cursor.execute("SELECT id, data, errors FROM records WHERE is_valid = 0")
        full_invalid_records = [{"originalRow": dict(json.loads(row[1]), _db_id=row[0]), "errors": row[2]} for row in cursor.fetchall()]
        conn.close()

        try:
            AuditService.generate_and_save_validation_report(
                user_id=user_id,
                session_id=session_id,
                source_crm=source_crm,
                target_crm=target_crm,
                target_object=obj_name,
                stats=aggregate_stats,
                invalid_records=full_invalid_records,
                auth_token=auth_token,
                staging_db_path=db_path,
                user_email=user_email,
                user_name=user_name,
            )
        except Exception as e:
            logger.exception("Failed to persist validation history for session %s", session_id)

        await websocket.send_json({
            "log": f"Stream Validation Complete: {aggregate_stats['total']} total records.",
            "status": "Validation Passed" if aggregate_stats["invalid"] == 0 else "Validation Warning",
            "stats": aggregate_stats,
            "invalidRecords": all_invalid_records,
            "sessionId": session_id
        })
        await websocket.close()
        
    except WebSocketDisconnect:
        pass
    except HTTPException as e:
        try:
            if is_crm_session_expired(e):
                side = (e.detail or {}).get("side")
                await websocket.send_json(_session_expired_payload([side] if side else ["source"], "Validation Failed"))
            else:
                await websocket.send_json({"log": f"Stream Crash: {e.detail}", "status": "Validation Failed"})
            await websocket.close()
        except Exception:
            pass
    except Exception as e:
        import traceback
        traceback.print_exc()
        try:
            await websocket.send_json({"log": f"Stream Crash: {str(e)}", "status": "Validation Failed"})
            await websocket.close()
        except: pass


# ==========================================
# ROUTE: DOWNLOAD FULL AUDIT REPORT (CSV)
# ==========================================
@router.get("/api/audit/download/{session_id}")
async def download_validation_audit(session_id: str, type: str = 'valid', current_user = Depends(get_current_user)):
    db_path = get_db_path(session_id)
    
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Staging database not found or session expired.")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    is_valid = 1 if type == 'valid' else 0
    cursor.execute("SELECT data, errors FROM records WHERE is_valid = ?", (is_valid,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No {type} records available.")

    all_records = []
    fieldnames = set()
    for row in rows:
        rec = json.loads(row[0])
        if type == 'invalid':
            rec['Validation_Errors'] = row[1]
            
        fieldnames.update(rec.keys())
        all_records.append(rec)
    
    fieldnames = list(fieldnames)
    if 'Validation_Errors' in fieldnames:
        fieldnames.remove('Validation_Errors')
        fieldnames.insert(0, 'Validation_Errors')

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_records)
    
    output.seek(0)
    
    headers = {
        'Content-Disposition': f'attachment; filename="Validation_Audit_{type.capitalize()}_{session_id}.csv"'
    }
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers=headers)


# =========================================================
# FETCH OBJECT RECORD COUNT (With Silent Token Refresh)
# =========================================================
@router.get("/api/metadata/{crm_id}/count/{object_name}")
async def get_crm_object_count(
    crm_id: str,
    object_name: str,
    role: str = "source",
    query: str = "",
    timeFilter: str = "",
    current_user = Depends(get_current_user)
):
    crm_lower = crm_id.lower()
    creds = CrmService.get_active_crm_credentials(current_user.id, crm_lower, role)

    time_filter = None
    if timeFilter:
        try:
            time_filter = json.loads(timeFilter)
        except (json.JSONDecodeError, TypeError):
            raise HTTPException(status_code=400, detail="timeFilter must be valid JSON.")

    async def _fetch(token):
        creds["access_token"] = token
        return await CrmQueryService.get_object_count(creds, object_name, crm_lower, query, time_filter)

    try:
        count = await _fetch(creds["access_token"])
        return {"count": count}
    except HTTPException as e:
        if e.status_code == 401:
            try:
                new_token = await CrmService.refresh_crm_token(current_user.id, crm_lower, role)
            except HTTPException as refresh_err:
                if refresh_err.status_code == 401:
                    raise crm_session_expired(crm_lower, role)
                raise
            count = await _fetch(new_token)
            return {"count": count}
        raise e


@router.post("/api/admin/staging/cleanup")
async def trigger_staging_cleanup(
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    current_user = Depends(get_current_user)
):
    if max_age_hours <= 0:
        raise HTTPException(status_code=400, detail="max_age_hours must be positive.")
    return cleanup_stale_staging_databases(max_age_hours=max_age_hours)