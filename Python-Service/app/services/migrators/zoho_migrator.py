import re
import asyncio
from app.services.crm_service import CrmService
from app.services.resilience.retry import ApiQuotaExhausted, CircuitOpen
from app.services.time_filter_service import (
    merge_time_clause,
    build_zoho_time_clause,
    TimeFilterError,
)
from app.services.query_field_utils import ensure_fields_selected
from app.services.coql_query_builder import build_coql, extract_user_limit, COQL_PAGE_SIZE

MAX_RATE_LIMIT_PAUSES = 10  # x 30s = ~5 minutes before the run is paused for a later resume


def chunk_dataset(data: list, chunk_size: int = 100):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

class ZohoMigrator:

    async def extract(self, client, creds, obj_name, query, mappings, send_log, time_filter=None):
        zoho_token = creds.get("access_token")
        domain = (creds.get("api_domain") or "https://www.zohoapis.com").rstrip('/')
        if not domain.startswith("http"):
            domain = f"https://{domain}"

        headers = {"Authorization": f"Zoho-oauthtoken {zoho_token}"}
        target_fields = [m.get("sourceField") or m.get("csvField") for m in mappings if m.get("sourceField") or m.get("csvField")]
        safe_fields = target_fields[:40] if target_fields else ["id"]

        try:
            time_clause = build_zoho_time_clause(time_filter)
        except TimeFilterError as e:
            await send_log(f"[{obj_name}] Invalid migration filter: {e}")
            raise

        raw_query = (query or "").strip()

        try:
            if raw_query or time_clause:
                await send_log("Extracting data from Zoho using COQL...")
                user_limit = extract_user_limit(raw_query)
                data, offset, actual_query_used = [], 0, ""

                while True:
                    page_size = COQL_PAGE_SIZE if user_limit is None else min(COQL_PAGE_SIZE, user_limit - len(data))
                    if page_size <= 0:
                        break

                    coql_query = build_coql(raw_query, obj_name, safe_fields, time_clause, limit=page_size, offset=offset)
                    actual_query_used = actual_query_used or coql_query

                    res = await client.post(f"{domain}/crm/v6/coql", headers=headers, json={"select_query": coql_query})
                    if res.status_code == 204:  # COQL returns 204 when nothing matches
                        break
                    if res.status_code != 200:
                        raise Exception(f"Zoho COQL Error: {res.text} | Query: {coql_query}")

                    body = res.json()
                    batch = body.get("data", [])
                    data.extend(batch)
                    offset += len(batch)

                    if len(data) % 1000 < COQL_PAGE_SIZE:
                        await send_log(f"[{obj_name}] Extracted {len(data)} records...")
                    if not batch or not body.get("info", {}).get("more_records"):
                        break
            else:
                await send_log("Extracting data from Zoho (Standard API)...")
                data, page = [], 1
                while True:
                    res = await client.get(
                        f"{domain}/crm/v6/{obj_name}?page={page}&per_page=200&fields={','.join(safe_fields)}",
                        headers=headers,
                    )
                    if res.status_code == 204:
                        break
                    if res.status_code != 200:
                        raise Exception(f"Zoho API Error: {res.text}")

                    batch = res.json().get("data", [])
                    if not batch:
                        break
                    data.extend(batch)
                    if not res.json().get("info", {}).get("more_records"):
                        break
                    page += 1

                actual_query_used = f"GET {obj_name} (Zoho Standard API, no filter -- fields: {', '.join(safe_fields)})"

            processed_data = []
            for r in data:
                flat_rec = {}
                for k, v in r.items():
                    if isinstance(v, dict) and "id" in v:
                        flat_rec[k] = v.get("name", v["id"])
                    else:
                        flat_rec[k] = v
                processed_data.append(flat_rec)

            return processed_data, actual_query_used

        except Exception as e:
            raise Exception(f"Failed to extract from Zoho: {str(e)}")

    async def upload(self, client, payload, op_mode, pass_name, options, send_log):
        if not payload: return 0, 0, 0, [], [], []

        target_object = options["targetObject"]
        token = options["token"]
        user_id = options.get("userId") 
        domain = options["instance_url"].rstrip('/')
        source_records = options["sourceRecords"]

        if not domain.startswith("http"): domain = f"https://{domain}"
        
        total_success, total_error, total_skipped = 0, 0, 0
        all_success_data, all_error_data, all_skipped_data = [], [], []
        ids_to_revert = []  

        normalized_obj = target_object.strip()
        if not normalized_obj.endswith('s') and normalized_obj.lower() != 'data':
            plurals = {'lead': 'Leads', 'contact': 'Contacts', 'account': 'Accounts', 'deal': 'Deals'}
            normalized_obj = plurals.get(normalized_obj.lower(), normalized_obj.capitalize() + 's')

        await send_log(f"[{normalized_obj}] {pass_name}: Injecting data stream into Zoho CRM...")
        headers = {"Authorization": f"Zoho-oauthtoken {token}", "Content-Type": "application/json"}
        
        chunks = list(chunk_dataset(payload, 100))

        is_update_only = (op_mode == "update")
        wire_op_mode = "upsert" if is_update_only else op_mode

        if not options.get("targetExtIdField") and op_mode in ("update", "upsert"):
            await send_log(f"[{normalized_obj}] {pass_name}: No External ID field configured -- cannot match existing records for {op_mode.upper()}.")
            return 0, len(payload), 0, [], [source_records[item["originalIndex"]] for item in payload], []

        if wire_op_mode == "upsert":
            api_path = f"{domain}/crm/v6/{normalized_obj}/upsert"
            http_method = "POST"
        else: # insert
            api_path = f"{domain}/crm/v6/{normalized_obj}"
            http_method = "POST"

        for chunk in chunks:
            zoho_data_rows = [c["targetRecord"] for c in chunk]
            
            try:
                req_payload = {"data": zoho_data_rows}
                if wire_op_mode == "upsert" and options.get("targetExtIdField"):
                   req_payload["duplicate_check_fields"] = [options["targetExtIdField"]]

                rate_limit_hits = 0
                while True:
                    if http_method == "PUT":
                        res = await client.put(api_path, json=req_payload, headers=headers)
                    else:
                        res = await client.post(api_path, json=req_payload, headers=headers)
                    
                    if res.status_code == 401:
                        await send_log("  Zoho token expired mid-migration. Silently refreshing...")
                        token = await CrmService.refresh_crm_token(user_id, "zoho", "target")
                        headers["Authorization"] = f"Zoho-oauthtoken {token}"
                        continue 
                        
                    if res.status_code == 429:
                        rate_limit_hits += 1
                        if rate_limit_hits > MAX_RATE_LIMIT_PAUSES:
                            # Stop pausing forever: checkpoint the run so it can be resumed once Zoho's limit resets.
                            raise ApiQuotaExhausted(f"Zoho API rate limit still active after {MAX_RATE_LIMIT_PAUSES} pauses.")
                        await send_log(f" ⏳ Zoho API Rate Limit reached during upload. Pausing 30s ({rate_limit_hits}/{MAX_RATE_LIMIT_PAUSES})...")
                        await asyncio.sleep(30)
                        continue
                        
                    break 
                
                if res.status_code in [200, 201, 202, 207]:
                    for item, z_res in zip(chunk, res.json().get("data", [])):
                        orig_record = source_records[item["originalIndex"]]
                        
                        if z_res.get("status") == "success":
                            raw_id = z_res.get("details", {}).get("id")


                            if is_update_only and z_res.get("action") == "insert":
                                orig_record["Target_SkipReason"] = (
                                    f"[{options.get('targetExtIdField')}] No matching record found in Zoho. "
                                    f"Skipped because Update mode does not create new records."
                                )
                                all_skipped_data.append(orig_record)
                                total_skipped += 1
                                if raw_id:
                                    ids_to_revert.append(str(raw_id))
                                    on_revert_pending = options.get("onRevertPending")
                                    if on_revert_pending:
                                        on_revert_pending([str(raw_id)])  # persisted before the revert so a crash can finish it
                                continue

                            orig_record["Target_Id"] = str(raw_id) if raw_id else "Success"
                            
                            all_success_data.append(orig_record)
                            total_success += 1
                        else:
                            err_msg = z_res.get("message") or z_res.get("code", "Unknown Error")
                            details = z_res.get("details", {})
                            
                            if isinstance(details, dict):
                                api_name = details.get("api_name")
                                if api_name:
                                    err_msg = f"[{api_name}] {err_msg}"
                                elif details:
                                    err_msg = f"{err_msg} | Details: {str(details)}"
                                    
                            orig_record["Target_Error"] = f"Zoho Error: {err_msg}"
                            all_error_data.append(orig_record)
                            total_error += 1
                else:
                    error_text = res.text
                    try: error_text = res.json().get("message", res.json().get("errors", [{}])[0].get("message", res.text))
                    except: pass
                    
                    await send_log(f" Zoho API Rejected Batch ({res.status_code}): {error_text}")
                    for item in chunk:
                        orig_record = source_records[item["originalIndex"]]
                        orig_record["Target_Error"] = f"Zoho API Error: {error_text}"
                        all_error_data.append(orig_record)
                        total_error += 1
                        
            except (ApiQuotaExhausted, CircuitOpen):
                raise  # a stop signal for the run, not a per-row failure: let the runner checkpoint and pause
            except Exception as exc:
                for item in chunk:
                    orig_record = source_records[item["originalIndex"]]
                    orig_record["Target_Error"] = str(exc)
                    all_error_data.append(orig_record)
                    total_error += 1

        if ids_to_revert:
            await send_log(
                f"[{normalized_obj}] {pass_name}: Update mode found {len(ids_to_revert)} record(s) with no "
                f"match — reverting the records Zoho auto-created for them..."
            )
            failed = set(await self._delete_records(client, domain, headers, normalized_obj, ids_to_revert, user_id, send_log))
            on_revert_done = options.get("onRevertDone")
            if on_revert_done:  # only confirmed deletes leave the pending-revert queue
                on_revert_done([i for i in ids_to_revert if i and i not in failed])
            if failed:
                await send_log(
                    f"[{normalized_obj}] {pass_name}: WARNING {len(failed)} auto-created record(s) could not be deleted "
                    f"and stay queued; they are retried on the next resume."
                )

        return total_success, total_error, total_skipped, all_success_data, all_error_data, all_skipped_data

    @staticmethod
    def _normalize_object(target_object):
        normalized_obj = target_object.strip()
        if not normalized_obj.endswith('s') and normalized_obj.lower() != 'data':
            plurals = {'lead': 'Leads', 'contact': 'Contacts', 'account': 'Accounts', 'deal': 'Deals'}
            normalized_obj = plurals.get(normalized_obj.lower(), normalized_obj.capitalize() + 's')
        return normalized_obj

    @staticmethod
    def _domain(options):
        domain = options["instance_url"].rstrip('/')
        return domain if domain.startswith("http") else f"https://{domain}"

    async def revert_created(self, client, options, record_ids, send_log):
        """Public entry used by the resume path to finish a revert that was interrupted."""
        headers = {"Authorization": f"Zoho-oauthtoken {options['token']}", "Content-Type": "application/json"}
        return await self._delete_records(
            client, self._domain(options), headers, self._normalize_object(options["targetObject"]),
            record_ids, options.get("userId"), send_log,
        )

    async def find_existing(self, client, options, key_field, values):
        """{key_value: Zoho id} for rows that already exist -- makes insert resume duplicate-free."""
        domain, module = self._domain(options), self._normalize_object(options["targetObject"])
        headers = {"Authorization": f"Zoho-oauthtoken {options['token']}", "Content-Type": "application/json"}
        found = {}
        for chunk in chunk_dataset(list(values), 50):  # COQL IN() accepts at most 50 values
            quoted = ",".join("'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'" for v in chunk)
            query = f"select id, {key_field} from {module} where {key_field} in ({quoted}) limit 200"
            res = await client.post(f"{domain}/crm/v6/coql", headers=headers, json={"select_query": query})
            if res.status_code == 204:
                continue
            res.raise_for_status()
            for rec in res.json().get("data", []):
                if rec.get(key_field) is not None:
                    found[str(rec[key_field])] = str(rec["id"])
        return found

    async def _delete_records(self, client, domain, headers, normalized_obj, record_ids, user_id, send_log):
        """Deletes records Zoho auto-created that 'update' mode should never
        have created in the first place (Zoho's bulk delete takes <=100 ids/call).

        Returns the ids that could NOT be confirmed deleted ([] = everything reverted). Zoho answers per id, so
        each entry of the response is checked; an id that is already gone counts as done (safe to replay).
        """
        failed = []
        ids = [str(r) for r in dict.fromkeys(record_ids) if r]
        for chunk in chunk_dataset(ids, 100):
            try:
                ids_param = ",".join(chunk)
                res = await client.delete(f"{domain}/crm/v6/{normalized_obj}?ids={ids_param}", headers=headers)

                if res.status_code == 401:
                    token = await CrmService.refresh_crm_token(user_id, "zoho", "target")
                    headers["Authorization"] = f"Zoho-oauthtoken {token}"
                    res = await client.delete(f"{domain}/crm/v6/{normalized_obj}?ids={ids_param}", headers=headers)

                if res.status_code not in [200, 202]:
                    await send_log(f"[{normalized_obj}] Revert batch failed: {res.text}")
                    failed.extend(chunk)
                    continue

                try:
                    items = res.json().get("data") or []
                except ValueError:
                    items = []
                confirmed = set()
                for item in items:
                    rid = str((item.get("details") or {}).get("id") or "")
                    message = str(item.get("message") or "").lower()
                    gone = any(w in message for w in ("not exist", "not found", "already", "deleted", "invalid"))
                    if item.get("status") == "success" or gone:
                        confirmed.add(rid)
                failed.extend(i for i in chunk if i not in confirmed)
            except Exception as e:
                await send_log(f"[{normalized_obj}] Revert Failed: {str(e)}")
                failed.extend(chunk)
        return failed