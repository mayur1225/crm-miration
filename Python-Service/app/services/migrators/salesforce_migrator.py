import urllib.parse
import asyncio
import re
from app.services.crm_service import CrmService
from app.services.time_filter_service import (
    merge_time_clause,
    build_salesforce_time_clause,
    TimeFilterError,
)
from app.services.query_field_utils import ensure_fields_selected

def chunk_dataset(data: list, chunk_size: int = 5000):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

class SalesforceMigrator:
    
    # ==========================================
    # EXTRACT (Pull from Salesforce)
    # ==========================================
    async def extract(self, client, creds, obj_name, query, mappings, send_log, time_filter=None):
        sf_token = creds.get("access_token")
        sf_instance = creds.get("instance_url", "").rstrip('/')
        
        headers_list = [m["sourceField"] if "sourceField" in m else m["csvField"] for m in mappings if m.get("sourceField") or m.get("csvField")]
        if "Id" not in headers_list:
            headers_list.append("Id")
        fields_str = ", ".join(headers_list) if headers_list else "Id"
        
        clean_query = (query or "").strip()

        # APPLY DYNAMIC TIME FILTER LOGIC (date-range only; see time_filter_service.py)
        try:
            time_clause = build_salesforce_time_clause(time_filter)
        except TimeFilterError as e:
            await send_log(f"[{obj_name}] Invalid migration filter: {e}")
            raise

        if clean_query.lower().startswith("select "):
            soql = clean_query
            
            if time_clause:
                soql = merge_time_clause(soql, time_clause, where_kw="WHERE", and_kw="AND")
                    
            if " * " in soql.lower() or soql.lower().startswith("select *"):
                soql = re.sub(r'(?i)select\s+\*\s+from', f'SELECT {fields_str} FROM', soql)
            else:
                # A hand-written/auto-generated SELECT list is a floor, not
                # a ceiling -- every field mapped in the UI must actually be
                # queried, or it comes back silently empty in the extracted
                # records even though the mapping table says it's mapped.
                # headers_list already guarantees "Id" is included, so this
                # single union replaces the old id-only injection below it.
                soql = ensure_fields_selected(soql, headers_list)
        else:
            where_parts = []
            if clean_query: where_parts.append(f"({clean_query})")
            if time_clause: where_parts.append(time_clause)
            
            where_combined = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
            soql = f"SELECT {fields_str} FROM {obj_name}{where_combined}"
        
        headers = {"Authorization": f"Bearer {sf_token}", "Content-Type": "application/json"}
        safe_soql = urllib.parse.quote(soql)
        url = f"{sf_instance}/services/data/v60.0/query?q={safe_soql}"
        
        await send_log(f"[{obj_name}] Executing SOQL: {soql}")
        
        source_records = []
        while url:
            res = await client.get(url, headers=headers)
            
            if res.status_code != 200:
                await send_log(f"Salesforce Extraction Failed: {res.text}")
                res.raise_for_status()
                
            data = res.json()
            
            for r in data.get("records", []):
                r.pop("attributes", None)
                flat_rec = {}
                for k, v in r.items():
                    if isinstance(v, dict):
                        flat_rec[k] = v.get("Name", v.get("Id", str(v)))
                    elif isinstance(v, list):
                        flat_rec[k] = str(v)
                    else:
                        flat_rec[k] = v
                source_records.append(flat_rec)
                
            if len(source_records) % 5000 == 0:
                await send_log(f"[{obj_name}] Extracted {len(source_records)} records...")
                
            url = f"{sf_instance}{data.get('nextRecordsUrl')}" if not data.get("done") else None

        await send_log(f"[{obj_name}] Extraction Complete! Total: {len(source_records)}")
        return source_records, soql

    # ==========================================
    # UPLOAD (Push to Salesforce)
    # ==========================================
    async def upload(self, client, payload, op_mode, pass_name, options, send_log):
        if not payload: return 0, 0, 0, [], [], []
        
        target_object = options["targetObject"]
        target_ext_id_field = options["targetExtIdField"]
        sf_token = options["token"]
        sf_instance = options["instance_url"]
        batch_size = options.get("batchSize", 5000)
        source_records = options["sourceRecords"]
        user_id = options["userId"]

        total_success, total_error, total_skipped = 0, 0, 0
        all_success_data, all_error_data, all_skipped_data = [], [], []

        await send_log(f"[{target_object}] {pass_name}: Initializing {op_mode.upper()} to Salesforce...")
        sf_headers = {"X-SFDC-Session": sf_token, "Content-Type": "application/json; charset=UTF-8", "Accept": "application/json"}
        bulk_base_url = f"{sf_instance.rstrip('/')}/services/async/60.0"

        is_update_only = (op_mode == "update")
        wire_op_mode = "upsert" if is_update_only else op_mode
        if wire_op_mode == "upsert" and not target_ext_id_field:
            await send_log(
                f"[{target_object}] {pass_name}: No unique/external ID field configured -- "
                f"cannot match existing records for {op_mode.upper()}."
            )
            return 0, len(payload), 0, [], [source_records[item["originalIndex"]] for item in payload], []

        job_config = {"operation": wire_op_mode, "object": target_object, "contentType": "JSON"}
        if wire_op_mode == "upsert": job_config["externalIdFieldName"] = target_ext_id_field

        chunks = list(chunk_dataset(payload, batch_size))

        # RESUME: a previous run already opened a Bulk job for exactly this segment (one job == one batch).
        # Ingest its results instead of submitting the rows again.
        job_id, batch_ids = None, []
        resume_job_id = options.get("resumeBulkJobId")
        if resume_job_id and len(chunks) == 1:
            list_res = await client.get(f"{bulk_base_url}/job/{resume_job_id}/batch", headers=sf_headers)
            existing = [b.get("id") for b in list_res.json().get("batchInfo", [])] if list_res.status_code == 200 else []
            if len(existing) == 1:
                job_id, batch_ids = resume_job_id, existing
                await send_log(f"[{target_object}] {pass_name}: resuming Salesforce Bulk job {job_id} from the interrupted run...")
            else:
                await client.post(f"{bulk_base_url}/job/{resume_job_id}", json={"state": "Closed"}, headers=sf_headers)

        if job_id is None:
            job_res = await client.post(f"{bulk_base_url}/job", json=job_config, headers=sf_headers)

            if job_res.status_code == 401:
                await send_log(f"[{target_object}] Session Expired. Silently refreshing SF Token...")
                sf_token = await CrmService.refresh_crm_token(user_id, "salesforce", "target")
                sf_headers["X-SFDC-Session"] = sf_token
                job_res = await client.post(f"{bulk_base_url}/job", json=job_config, headers=sf_headers)

            if job_res.status_code != 201:
                error_text = job_res.text
                if wire_op_mode == "upsert" and "does not match an External ID" in error_text:
                    await send_log(
                        f"[{target_object}] Salesforce Job Failed: '{target_ext_id_field}' is not marked as an "
                        f"External ID, Salesforce Id, or indexed/lookup field on {target_object} in Salesforce. "
                        f"Mark it as an External ID field in Salesforce Setup, or choose a different field for matching."
                    )
                else:
                    await send_log(f"[{target_object}] Salesforce Job Failed: {error_text}")
                return 0, len(payload), 0, [], [source_records[item["originalIndex"]] for item in payload], []

            job_id = job_res.json().get("id")
            on_job_created = options.get("onBulkJobCreated")
            if on_job_created:
                on_job_created(job_id)  # persisted BEFORE any batch is submitted
            await send_log(f"[{target_object}] {pass_name}: Executing {len(chunks)} batches...")

            semaphore = asyncio.Semaphore(6)

            async def upload_chunk(chunk_data):
                async with semaphore:
                    just_records = [c["targetRecord"] for c in chunk_data]
                    b_res = await client.post(f"{bulk_base_url}/job/{job_id}/batch", json=just_records, headers=sf_headers)
                    b_res.raise_for_status()
                    return b_res.json().get("id")

            batch_ids = await asyncio.gather(*[upload_chunk(c) for c in chunks])

        await client.post(f"{bulk_base_url}/job/{job_id}", json={"state": "Closed"}, headers=sf_headers)

        poll_delay = 1.0
        while True:
            await asyncio.sleep(poll_delay)
            status_res = await asyncio.gather(*[client.get(f"{bulk_base_url}/job/{job_id}/batch/{b_id}", headers=sf_headers) for b_id in batch_ids])
            states = [r.json().get("state") for r in status_res]
            if all(s == "Completed" for s in states) or any(s in ["Failed", "NotProcessed"] for s in states):
                break
            poll_delay = min(poll_delay * 1.5, 4.0)

        ids_to_revert = []  

        for i, b_id in enumerate(batch_ids):
            res = await client.get(f"{bulk_base_url}/job/{job_id}/batch/{b_id}/result", headers=sf_headers)
            results = res.json()
            original_chunk = chunks[i]

            for row_data, sf_result in zip(original_chunk, results):
                orig_record = source_records[row_data["originalIndex"]]
                
                if sf_result.get("success"):
     
                    if is_update_only and sf_result.get("created"):
                        orig_record["Target_SkipReason"] = (
                            f"[{target_ext_id_field}] No matching record found in Salesforce. "
                            f"Skipped because Update mode does not create new records."
                        )
                        all_skipped_data.append(orig_record)
                        total_skipped += 1
                        ids_to_revert.append(sf_result.get("id"))
                        continue

                    orig_record["Target_Id"] = sf_result.get("id")
                    all_success_data.append(orig_record)
                    total_success += 1
                else:
                    err_obj = sf_result.get("errors", [{}])[0]
                    err_msg = err_obj.get("message", "Unknown Error")
                    fields = err_obj.get("fields", [])
                    
                    if fields and isinstance(fields, list) and len(fields) > 0:
                        err_msg = f"[{', '.join(fields)}] {err_msg}"
                        
                    orig_record["Target_Error"] = err_msg
                    all_error_data.append(orig_record)
                    total_error += 1

        if ids_to_revert:
            await send_log(
                f"[{target_object}] {pass_name}: Update mode found {len(ids_to_revert)} record(s) with no "
                f"match — reverting the records Salesforce auto-created for them..."
            )
            on_revert_pending = options.get("onRevertPending")
            if on_revert_pending:
                on_revert_pending(ids_to_revert)  # persisted BEFORE the delete job so a crash can finish the revert
            await self._delete_records(client, sf_instance, sf_headers, bulk_base_url, target_object, ids_to_revert, send_log)

        return total_success, total_error, total_skipped, all_success_data, all_error_data, all_skipped_data

    async def revert_created(self, client, options, record_ids, send_log):
        """Public entry used by the resume path to finish a revert that was interrupted."""
        sf_instance = options["instance_url"]
        sf_headers = {"X-SFDC-Session": options["token"], "Content-Type": "application/json; charset=UTF-8", "Accept": "application/json"}
        bulk_base_url = f"{sf_instance.rstrip('/')}/services/async/60.0"
        await self._delete_records(client, sf_instance, sf_headers, bulk_base_url, options["targetObject"], record_ids, send_log)

    async def find_existing(self, client, options, key_field, values):
        """{key_value: Salesforce Id} for rows that already exist -- makes insert resume duplicate-free."""
        base = options["instance_url"].rstrip("/")
        headers = {"Authorization": f"Bearer {options['token']}"}
        found = {}
        for chunk in chunk_dataset(list(values), 200):
            quoted = ",".join("'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'" for v in chunk)
            soql = f"SELECT Id, {key_field} FROM {options['targetObject']} WHERE {key_field} IN ({quoted})"
            url, params = f"{base}/services/data/v60.0/query", {"q": soql}
            while url:
                res = await client.get(url, params=params, headers=headers)
                res.raise_for_status()
                body = res.json()
                for rec in body.get("records", []):
                    if rec.get(key_field) is not None:
                        found[str(rec[key_field])] = rec["Id"]
                url = f"{base}{body['nextRecordsUrl']}" if not body.get("done") else None
                params = None
        return found

    async def _delete_records(self, client, sf_instance, sf_headers, bulk_base_url, target_object, record_ids, send_log):
        """Runs a small Bulk API delete job to undo records that 'update' mode
        should never have created in the first place."""
        try:
            job_res = await client.post(
                f"{bulk_base_url}/job",
                json={"operation": "delete", "object": target_object, "contentType": "JSON"},
                headers=sf_headers
            )
            if job_res.status_code != 201:
                await send_log(f"[{target_object}] Revert Failed: could not open delete job: {job_res.text}")
                return

            job_id = job_res.json().get("id")
            delete_rows = [{"Id": rid} for rid in record_ids if rid]

            for chunk in chunk_dataset(delete_rows, 5000):
                b_res = await client.post(f"{bulk_base_url}/job/{job_id}/batch", json=chunk, headers=sf_headers)
                if b_res.status_code != 201:
                    await send_log(f"[{target_object}] Revert batch failed: {b_res.text}")

            await client.post(f"{bulk_base_url}/job/{job_id}", json={"state": "Closed"}, headers=sf_headers)
        except Exception as e:
            await send_log(f"[{target_object}] Revert Failed: {str(e)}")