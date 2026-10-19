import urllib.parse
import re
import asyncio
import json
from app.services.crm_service import CrmService, crm_session_expired
from app.services.time_filter_service import (
    build_zendesk_time_clause,
    build_zendesk_custom_object_time_filter,
    TimeFilterError,
)

def chunk_dataset(data: list, chunk_size: int = 100):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def _merge_zendesk_custom_filter(payload: dict, extra_clauses: list) -> dict:
    """ANDs extra_clauses into payload['filter']['and'], creating that
    structure if the caller's JSON didn't already have one, and wrapping
    a bare single-condition filter dict into an 'and' list if needed.
    Mirrors CrmQueryService._merge_zendesk_custom_filter so live preview
    and actual extraction build the identical filter shape."""
    payload = dict(payload)
    existing_filter = payload.get("filter")

    if isinstance(existing_filter, dict) and isinstance(existing_filter.get("and"), list):
        and_list = list(existing_filter["and"]) + extra_clauses
    elif isinstance(existing_filter, dict) and existing_filter:
        and_list = [existing_filter] + extra_clauses
    else:
        and_list = list(extra_clauses)

    payload["filter"] = {"and": and_list}
    return payload


class ZendeskMigrator:

    async def extract(self, client, creds, obj_name, query, mappings, send_log, time_filter=None):
        token = creds.get("access_token")
        subdomain = creds.get("subdomain")
        user_id = creds.get("user_id")
        if not subdomain and creds.get("api_domain"):
            subdomain = creds.get("api_domain").replace(".zendesk.com", "").replace("https://", "")

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        safe_obj = obj_name.strip().lower()
        source_records = []

        standard_objects = ["tickets", "users", "organizations", "groups", "macros", "triggers", "views"]
        is_standard = safe_obj in standard_objects or f"{safe_obj}s" in standard_objects

        try:
            time_clause = build_zendesk_time_clause(time_filter)
            custom_time_filters = build_zendesk_custom_object_time_filter(time_filter)
        except TimeFilterError as e:
            await send_log(f"[{obj_name}] Invalid migration filter: {e}")
            raise

        try:
            if is_standard:
                # ==========================================
                # STANDARD OBJECT EXTRACTION
                # ==========================================
                safe_obj_singular = safe_obj[:-1] if safe_obj.endswith('s') else safe_obj

                clean_query = re.sub(r'type:[a-zA-Z0-9_]+', '', query, flags=re.IGNORECASE).strip() if query else ""
                query_parts = [p for p in [clean_query, time_clause] if p]
                final_query = f"{' '.join(query_parts)} type:{safe_obj_singular}".strip()
                safe_query = urllib.parse.quote(final_query)

                url = f"https://{subdomain}.zendesk.com/api/v2/search/export.json?filter[type]={safe_obj_singular}&query={safe_query}&page[size]=1000"

                token_refreshed = False
                while url:
                    while True:
                        res = await client.get(url, headers=headers)
                        if res.status_code == 401:
                            if token_refreshed:  # a freshly refreshed token was refused too: do not loop forever
                                raise crm_session_expired("zendesk", "source")
                            await send_log(" Zendesk token expired. Silently refreshing...")
                            token = await CrmService.refresh_crm_token(user_id, "zendesk", "source")
                            headers["Authorization"] = f"Bearer {token}"
                            token_refreshed = True
                            continue
                        if res.status_code == 429:
                            await send_log(" ⏳ Zendesk Rate Limit hit. Pausing 30s...")
                            await asyncio.sleep(30)
                            continue
                        break

                    res.raise_for_status()
                    token_refreshed = False
                    data = res.json()

                    results = data.get("results", [])
                    if not results: break

                    for r in results:
                        flat_rec = {}
                        for k, v in r.items():
                            if isinstance(v, dict):
                                flat_rec[k] = v.get("id", v.get("name", str(v)))
                            elif isinstance(v, list):
                                flat_rec[k] = ";".join([str(i.get("id", i.get("name", i))) if isinstance(i, dict) else str(i) for i in v])
                            else:
                                flat_rec[k] = v
                        source_records.append(flat_rec)

                    if len(source_records) % 500 == 0:
                        await send_log(f"[{safe_obj}] Extracted {len(source_records)} records...")

                    url = data.get("links", {}).get("next")
                    if not data.get("meta", {}).get("has_more"):
                        break

            else:
                # ==========================================
                # CUSTOM OBJECT EXTRACTION
                # ==========================================
                had_explicit_query = bool(query and query.strip())
                json_payload = {}

                if had_explicit_query:
                    try:
                        json_payload = json.loads(query)
                    except json.JSONDecodeError:
                        raise Exception("Invalid JSON payload in Zendesk query for custom objects.")

                if custom_time_filters:
                    json_payload = _merge_zendesk_custom_filter(json_payload, custom_time_filters)

                is_search = had_explicit_query or bool(custom_time_filters)

                if is_search:
                    url = f"https://{subdomain}.zendesk.com/api/v2/custom_objects/{safe_obj}/records/search?page[size]=100"
                else:
                    url = f"https://{subdomain}.zendesk.com/api/v2/custom_objects/{safe_obj}/records?page[size]=100"

                token_refreshed = False
                while url:
                    while True:
                        if is_search:
                            res = await client.post(url, headers=headers, json=json_payload)
                        else:
                            res = await client.get(url, headers=headers)

                        if res.status_code == 401:
                            if token_refreshed:  # a freshly refreshed token was refused too: do not loop forever
                                raise crm_session_expired("zendesk", "source")
                            await send_log(" Zendesk token expired. Silently refreshing...")
                            token = await CrmService.refresh_crm_token(user_id, "zendesk", "source")
                            headers["Authorization"] = f"Bearer {token}"
                            token_refreshed = True
                            continue
                        if res.status_code == 429:
                            await send_log("Zendesk Rate Limit hit. Pausing 30s...")
                            await asyncio.sleep(30)
                            continue
                        break

                    res.raise_for_status()
                    token_refreshed = False
                    data = res.json()

                    results = data.get("custom_object_records", [])
                    if not results: break

                    for rec in results:
                        flat_rec = {}
                        for k, v in rec.items():
                            if k == "custom_fields" and isinstance(v, list):
                                for cf in v: flat_rec[f"custom_field_{cf['id']}"] = cf.get("value")
                            elif k == "custom_object_fields" and isinstance(v, dict):
                                for cf_key, cf_val in v.items(): flat_rec[cf_key] = cf_val
                            elif not isinstance(v, (dict, list)):
                                flat_rec[k] = v
                        source_records.append(flat_rec)

                    if len(source_records) % 100 == 0:
                        await send_log(f"[{safe_obj}] Extracted {len(source_records)} records...")

                    url = data.get("links", {}).get("next")
                    if not data.get("meta", {}).get("has_more"):
                        break

        except Exception as e:
            await send_log(f" Zendesk Extraction Error: {str(e)}")
            raise

        await send_log(f"[{safe_obj}] Extraction Complete! Total: {len(source_records)}")
        return source_records