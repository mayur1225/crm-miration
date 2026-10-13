import asyncio
import httpx
import traceback
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends
from app.api.dependencies.auth import get_current_user
from app.services.validator_service import process_validation_batch
from app.utils.config import supabase
from app.services.crm_service import CrmService
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
from app.services.picklist_sync_service import PicklistSyncService

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
FILE_MIGRATOR = SalesforceFileMigrator()
FILE_MIGRATION_ESTIMATOR = FileMigrationEstimator()
FILE_MIGRATION_DEFAULT_SAFETY_THRESHOLD = 0.90

_SESSION_ID_RE = re.compile(r'^[A-Za-z0-9_-]+$')

def _require_valid_session_id(session_id: str) -> str:
    if not session_id or not _SESSION_ID_RE.fullmatch(session_id):
        raise HTTPException(status_code=400, detail="Invalid session ID.")
    return session_id

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
# CORE MIGRATION ROUTE
# ==========================================
@router.websocket("/ws/migrate")
async def websocket_migration(websocket: WebSocket):
    await websocket.accept()
    
    try:
        await websocket.send_json({"log": "System: Multi-Object Engine Initialized.", "status": "Initializing..."})
        payload = await websocket.receive_json()
        
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
        source_crm = raw_queue[0].get("sourceCrmId", "zendesk").lower() 
        target_crm = raw_queue[0].get("targetCrmId", "salesforce").lower() 
        
        if source_crm != "csv":
            source_creds = CrmService.get_active_crm_credentials(user_id, source_crm, "source")
            source_migrator = MIGRATORS.get(source_crm)
        else:
            source_creds = None
            source_migrator = None

        target_creds = CrmService.get_active_crm_credentials(user_id, target_crm, "target")
        target_migrator = MIGRATORS.get(target_crm)

        async def send_log(msg: str, status: str = "Running"):
            await websocket.send_json({"log": msg, "status": status})

        execution_queue = sort_jobs_by_dependency(raw_queue)

        async with httpx.AsyncClient(timeout=120.0) as client:
            all_success_data, all_error_data, all_skipped_data = [], [], []

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
                if migrate_attachments or migrate_files:
                    await send_log(
                        f"[{target_object}] File/attachment migration requested "
                        f"(Attachments={migrate_attachments}, Files={migrate_files}, "
                        f"source={source_crm}, target={target_crm})."
                    )
                
                if not mappings: continue

                sf_rules = job.get("sfRules", {})
                fls_violations = find_non_writable_mapped_fields(mappings, sf_rules, op_mode)
                if fls_violations:
                    await websocket.send_json({
                        "log": f"[{target_object}] FATAL: {len(fls_violations)} mapped field(s) can't be "
                               f"written by the connected {target_crm.capitalize()} user "
                               f"({', '.join(v['label'] for v in fls_violations)}). Remove them from the "
                               f"mapping (or connect a user with field-level write access) and re-validate "
                               f"before running.",
                        "status": "Failed",
                        "fieldAccessErrors": fls_violations
                    })
                    await websocket.close()
                    return

                if source_crm == "salesforce" and target_crm == "salesforce":
                    picklist_sync_mappings = [m for m in mappings if m.get("syncPicklistValues")]
                    if picklist_sync_mappings:
                        sync_result = await PicklistSyncService.sync(
                            client, source_creds, target_creds, user_id,
                            source_object, target_object, picklist_sync_mappings, send_log
                        )
                        if sync_result["failed"]:
                            await websocket.send_json({
                                "log": f"[{target_object}] FATAL: picklist sync failed for "
                                       f"{', '.join(sync_result['failed'])}. Aborting before data load.",
                                "status": "Failed",
                            })
                            await websocket.close()
                            return

                source_records = []
                actual_query_used = None

                session_id = job.get("sessionId")
                if source_crm == "csv":
                    source_records = job.get("sourceRecords", [])
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
                    extract_result = await source_migrator.extract(client, source_creds, source_object, extraction_query, mappings, send_log, time_filter)

                    if isinstance(extract_result, tuple):
                        source_records, actual_query_used = extract_result
                    else:
                        source_records = extract_result

                source_instance = (source_creds or {}).get("instance_url") or (source_creds or {}).get("api_domain") or (source_creds or {}).get("subdomain")
                target_instance = target_creds.get("instance_url") or target_creds.get("api_domain") or target_creds.get("subdomain")

                await IdMappingService.remap_reference_fields(
                    source_records, mappings, user_id, source_crm, target_crm,
                    source_instance=source_instance, target_instance=target_instance, send_log=send_log
                )

                options_base = {
                    "targetObject": target_object, "targetExtIdField": ext_id_field, "operationMode": op_mode,
                    "token": target_creds.get("access_token"), "instance_url": target_creds.get("instance_url") or target_creds.get("api_domain") or target_creds.get("subdomain"),
                    "batchSize": batch_size, "sourceRecords": source_records, "userId": user_id
                }

                async def execute_upload(payload_data, current_op, pass_name):
                    options_pass = dict(options_base)
                    result = await target_migrator.upload(client, payload_data, current_op, pass_name, options_pass, send_log)

                    if len(result) == 6:
                        _, _, _, succ, err, skipped = result
                    else:
                        _, _, succ, err = result
                        skipped = []

                    all_success_data.extend(succ)
                    all_error_data.extend(err)
                    all_skipped_data.extend(skipped)

                async def dedupe_and_execute(payload_data, current_op, pass_name):
                    """CRM-agnostic guard applied before any UPDATE/UPSERT hits the wire.
                    Keeps the last row for any unique-key value that appears more than
                    once in the same batch, and reports the earlier duplicates as
                    skipped instead of letting the CRM API silently drop/overwrite one
                    of them."""
                    if current_op in ("update", "upsert") and ext_id_field:
                        dedupe_key_field = "external_id" if target_crm == "zendesk" else ext_id_field
                        clean_payload, dup_skips = PayloadBuilderService.dedupe_by_unique_key(
                            payload_data, source_records, dedupe_key_field
                        )
                        if dup_skips:
                            await send_log(
                                f"[{target_object}] {pass_name}: {len(dup_skips)} record(s) shared the same "
                                f"'{ext_id_field}' value within this batch -- keeping the last one, skipping the rest."
                            )
                            all_skipped_data.extend(dup_skips)
                        await execute_upload(clean_payload, current_op, pass_name)
                    else:
                        await execute_upload(payload_data, current_op, pass_name)

                job_success_start_idx = len(all_success_data)
                has_self_ref = any(m.get("type") == "reference" and target_object in (m.get("referenceTo") or []) for m in mappings)
                
                if job.get("isPass3Patch", False):
                    p_load = PayloadBuilderService.build_payload(source_records, mappings, {"targetObject": target_object, "targetExtIdField": ext_id_field, "onlyReferencesTo": job.get("onlyReferencesTo", []), "operationMode": "upsert"}, target_crm)
                    await dedupe_and_execute(p_load, "upsert", "Pass 3 (Patch)")
                elif op_mode == "delete":
                    p_load = PayloadBuilderService.build_payload(source_records, mappings, {"targetObject": target_object, "operationMode": "delete"}, target_crm)
                    await execute_upload(p_load, "delete", "Deletion")
                elif has_self_ref:
                    p1_load = PayloadBuilderService.build_payload(source_records, mappings, {"targetObject": target_object, "targetExtIdField": ext_id_field, "excludeReferencesTo": job.get("deferReferencesTo", []), "skipSelfReferencing": True, "operationMode": op_mode}, target_crm)
                    await dedupe_and_execute(p1_load, op_mode, "Pass 1")
                    p2_load = PayloadBuilderService.build_payload(source_records, mappings, {"targetObject": target_object, "targetExtIdField": ext_id_field, "onlySelfReferencing": True, "operationMode": "upsert"}, target_crm)
                    await dedupe_and_execute(p2_load, "upsert", "Pass 2")
                else:
                    p_load = PayloadBuilderService.build_payload(source_records, mappings, {"targetObject": target_object, "targetExtIdField": ext_id_field, "excludeReferencesTo": job.get("deferReferencesTo", []), "operationMode": op_mode}, target_crm)
                    await dedupe_and_execute(p_load, op_mode, "Standard Sync")

    
                job_success_records = all_success_data[job_success_start_idx:]
                job_id_map = {
                    (rec.get("Id") or rec.get("id")): rec.get("Target_Id")
                    for rec in job_success_records
                    if (rec.get("Id") or rec.get("id")) and rec.get("Target_Id")
                }
                if job_id_map:
                    saved_count = IdMappingService.save_mappings(
                        user_id, source_crm, target_crm, target_object, list(job_id_map.items()),
                        source_instance=source_instance, target_instance=target_instance
                    )
                    await send_log(f"[{target_object}] Saved {saved_count} source->target Id mapping(s) for future reference lookups.")

                # ==========================================
                # FILES & ATTACHMENTS PASS (Salesforce -> Salesforce only)
                # ==========================================
                if migrate_attachments or migrate_files:
                    if job.get("isPass3Patch", False):
                        await send_log(f"[{target_object}] File migration skipped for this pass (reference patch pass, not the primary sync).")
                    elif source_crm != "salesforce" or target_crm != "salesforce":
                        await send_log(f"[{target_object}] File migration skipped: only Salesforce -> Salesforce is supported right now (got {source_crm} -> {target_crm}).")
                    elif not job_id_map:
                        await send_log(
                            f"[{target_object}] File migration skipped: no source Id was found on synced records. "
                            f"Sample record keys: {list(job_success_records[0].keys()) if job_success_records else 'N/A'}"
                        )
                    else:
                        scoped_id_map = await resolve_file_migration_scope(
                            websocket, client, source_creds, target_creds, user_id,
                            job_id_map, target_object, job_index,
                            migrate_attachments, migrate_files, send_log,
                        )

                        if not scoped_id_map:
                            await send_log(f"[{target_object}] No records in scope for file migration -- skipping this pass.")
                        else:
                            await send_log(f"[{target_object}] Starting file/attachment migration for {len(scoped_id_map)} synced record(s)...")
                            file_results = await FILE_MIGRATOR.migrate_files_for_batch(
                                client, source_creds, target_creds, user_id, scoped_id_map,
                                migrate_attachments, migrate_files, send_log
                            )
                            await send_log(
                                f"[{target_object}] Files complete — "
                                f"Attachments: {file_results['attachments']['success']} ok / {file_results['attachments']['error']} failed, "
                                f"Files: {file_results['files']['success']} ok / {file_results['files']['error']} failed."
                                + (" (Stopped early: daily API limit reached.)" if file_results.get("apiLimitReached") else "")
                            )
            await websocket.send_json({"log": f"QUEUE COMPLETE! Building final payload...", "status": "Processing"})
            
            safe_success_data = []
            for record in all_success_data:
                safe_record = {}
                for key, value in record.items():
                    if isinstance(value, float) and math.isnan(value):
                        safe_record[key] = None
                    elif hasattr(value, "isoformat"):
                        safe_record[key] = value.isoformat()
                    else:
                        safe_record[key] = str(value) if value is not None else ""
                safe_success_data.append(safe_record)
            
            formatted_errors = []
            for err in all_error_data:
                if not isinstance(err, dict):
                    err = {"Raw_Data": str(err), "Target_Error": "Unknown error format."}
                
                error_msg = err.get("Target_Error", "API rejected this record.")
                
                safe_record = {}
                for k, v in err.items():
                    if k == "Target_Error":
                        continue
                    
                    if isinstance(v, float) and math.isnan(v):
                        safe_record[k] = None
                    elif hasattr(v, "isoformat"):
                        safe_record[k] = v.isoformat()
                    else:
                        safe_record[k] = str(v) if v is not None else ""
                
                formatted_errors.append({
                    "record": safe_record,
                    "error": error_msg
                })


            formatted_skipped = []
            for rec in all_skipped_data:
                if not isinstance(rec, dict):
                    rec = {"Raw_Data": str(rec), "Target_SkipReason": "Unknown skip reason."}

                skip_reason = rec.get("Target_SkipReason", "Skipped: no matching record found.")

                safe_record = {}
                for k, v in rec.items():
                    if k == "Target_SkipReason":
                        continue

                    if isinstance(v, float) and math.isnan(v):
                        safe_record[k] = None
                    elif hasattr(v, "isoformat"):
                        safe_record[k] = v.isoformat()
                    else:
                        safe_record[k] = str(v) if v is not None else ""

                formatted_skipped.append({
                    "record": safe_record,
                    "reason": skip_reason
                })

        report_urls = {}
        try:
            await websocket.send_json({"log": "Generating Audit Reports (PDF & CSV)...", "status": "Finalizing"})
            report_urls = AuditService.generate_and_save_reports(
                user_id=user_id,
                session_id=session_id or f"direct_{str(uuid.uuid4())[:8]}",
                source_crm=source_crm,
                target_crm=target_crm,
                target_object=target_object,
                op_mode=op_mode,
                auth_token=auth_token,
                success_data=safe_success_data, 
                error_data=formatted_errors,
                extraction_query=extraction_query,
                time_filter=time_filter,
                user_email=user_email,
                user_name=user_name,
                source_mode=source_mode,
                actual_query_used=actual_query_used,
            )
        except Exception as e:
            logger.exception("Failed to generate/persist migration reports for session %s", session_id)
            report_urls = {}

        
        await websocket.send_json({
            "status": "Finished",
            "log": f"Migration completed! {len(safe_success_data)} records successful, "
                   f"{len(formatted_skipped)} skipped (no match), {len(formatted_errors)} failed.",
            "successData": safe_success_data,
            "errorData": formatted_errors,
            "skippedData": formatted_skipped,
            "reportUrls": report_urls

        })
        
        await websocket.close()
        
    except WebSocketDisconnect:
        pass
    except Exception as e:
        traceback.print_exc()
        try:
            await websocket.send_json({"log": f"SYSTEM CRASH: {str(e)}", "status": "Failed"})
            await websocket.close()
        except: pass


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

        source_creds = CrmService.get_active_crm_credentials(user_id, source_crm, "source")
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
            new_token = await CrmService.refresh_crm_token(current_user.id, crm_lower, role)
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