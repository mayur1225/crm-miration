import math
import logging

logger = logging.getLogger(__name__)

# Types whose values legitimately arrive as "123.0" (float-ified by pandas/Excel) and must be "123".
# Plain text/picklist columns are NOT in this set: "3.0" in a version field must stay "3.0".
_DOT_ZERO_TYPES = {"phone", "reference", "number", "integer", "double", "currency", "percent", "float"}
# Only pandas/NumPy artifacts count as "no value". Real words such as "Nan", "Null" or "None" (they are
# surnames and product names) are kept. A job can add its own list with options["nullStrings"].
_DEFAULT_NULL_STRINGS = ("", "nan", "NaN", "NaT", "<NA>")
_TRUE_VALUES = {"true", "1", "1.0", "yes", "y", "t", "on"}
_FALSE_VALUES = {"false", "0", "0.0", "no", "n", "f", "off"}

class PayloadBuilderService:
    @staticmethod
    def build_payload(raw_records, mappings, options, target_crm="salesforce", errors=None):
        """
        Returns [{originalIndex, targetRecord}].

        errors: optional list. When given, rows that cannot be built safely (a value that does not convert to the
        mapped type, or a patch-pass row with no external-id value to match on) are NOT sent; one
        {"originalIndex", "message"} entry per row is appended so the caller can record them as failed rows
        instead of silently dropping data. When omitted (legacy callers) a bad value is omitted from the record
        and a warning is logged.
        """
        mappings = PayloadBuilderService._dedupe_target_fields(mappings)

        skip_self_ref = options.get("skipSelfReferencing", False)
        only_self_ref = options.get("onlySelfReferencing", False)
        exclude_refs = options.get("excludeReferencesTo", [])
        only_refs = options.get("onlyReferencesTo", [])
        target_object = options.get("targetObject", "")
        target_ext_id_field = options.get("targetExtIdField", "")
        op_mode = options.get("operationMode", "insert")
        null_strings = set(options.get("nullStrings") or _DEFAULT_NULL_STRINGS)

        payload = []
        is_patch_mode = only_self_ref or len(only_refs) > 0

        for idx, raw_row in enumerate(raw_records):
            target_record = {}
            has_patch_data = False
            row_errors = []
            skip_row = False

            for mapping in mappings:
                # ---  Support BOTH API Mapping and CSV Mapping formats ---
                target_field = mapping.get("targetField") or mapping.get("sfField")
                if not target_field: continue

                source_field = mapping.get("sourceField") or mapping.get("csvField")
                
                # 1. Try fetching by the original CSV Header
                csv_val = raw_row.get(source_field)

                # 2. FALLBACK: If data was already transformed by the Validator
                if csv_val is None and target_field in raw_row:
                    csv_val = raw_row.get(target_field)

                expected_type = (mapping.get("type") or "string").lower()
                original_val = csv_val
                coerce_failed = False

                # --- SANITIZER (only real "no value" markers are nulled) ---
                if csv_val is not None:
                    if isinstance(csv_val, float) and math.isnan(csv_val):
                        csv_val = None
                    elif isinstance(csv_val, str):
                        csv_val = csv_val.strip()
                        if csv_val in null_strings:
                            csv_val = None
                        elif expected_type in _DOT_ZERO_TYPES and csv_val.endswith(".0") and csv_val[:-2].isdigit():
                            csv_val = csv_val[:-2]

                # --- TYPE ENFORCEMENT: exact JSON types required by the CRM API ---
                if csv_val is not None:
                    # Force strings for Phone, Email, Picklists, etc.
                    if expected_type in ["string", "phone", "email", "text", "picklist", "textarea", "reference"]:
                        if isinstance(csv_val, float) and csv_val.is_integer():
                            csv_val = str(int(csv_val))
                        else:
                            csv_val = str(csv_val)

                    # Force numbers (integer vs float matters for Zoho)
                    elif expected_type in ["number", "integer", "double", "currency", "percent", "float"]:
                        try:
                            num_val = float(csv_val)
                            if not math.isfinite(num_val):
                                raise ValueError("not finite")
                            if num_val.is_integer() or expected_type == "integer":
                                csv_val = int(num_val)
                            else:
                                csv_val = num_val
                        except (ValueError, TypeError, OverflowError):
                            csv_val = None
                            coerce_failed = True

                    # Force true booleans for Checkboxes. Unknown words are an error, never "True".
                    elif expected_type == "boolean":
                        if not isinstance(csv_val, bool):
                            flag = str(csv_val).strip().lower()
                            if flag in _TRUE_VALUES:
                                csv_val = True
                            elif flag in _FALSE_VALUES:
                                csv_val = False
                            else:
                                csv_val = None
                                coerce_failed = True

                    elif expected_type in ["date", "datetime"]:
                        try:
                            import pandas as pd
                            parsed_date = pd.to_datetime(csv_val, errors='coerce')
                            if pd.notna(parsed_date):
                                if expected_type == "date":
                                    csv_val = parsed_date.strftime('%Y-%m-%d')
                                else:
                                    csv_val = parsed_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                        except Exception:
                            pass

                if coerce_failed:
                    row_errors.append(f"[{target_field}] Cannot convert {str(original_val)[:80]!r} to {expected_type}.")
                    continue

                if is_patch_mode and target_field in ['CreatedDate', 'CreatedById', 'LastModifiedDate', 'LastModifiedById', 'created_at', 'updated_at']:
                    continue

                # Prevent empty strings/None from wiping out data (unless it's the external ID)
                if csv_val is None and target_field != target_ext_id_field: 
                    continue

                is_self_ref = mapping.get("type") == "reference" and target_object in (mapping.get("referenceTo") or [])
                refs_other = (mapping.get("referenceTo") or []) if mapping.get("type") == "reference" else []

                is_excluded_cross = any(obj in refs_other for obj in exclude_refs)
                is_only_target_cross = len(only_refs) > 0 and any(obj in refs_other for obj in only_refs)

                is_ext_id_mapping = bool(target_ext_id_field) and target_field == target_ext_id_field
                if skip_self_ref and is_self_ref: continue
                if only_self_ref and not is_self_ref and not is_ext_id_mapping: continue
                if is_excluded_cross: continue
                if len(only_refs) > 0 and not is_only_target_cross and not is_ext_id_mapping: continue

                # ==========================================
                # CRM-SPECIFIC PAYLOAD FORMATTING
                # ==========================================
                if target_crm == "salesforce":
                    rel_name = mapping.get("relationshipName")
                    if not rel_name and target_field:
                        if target_field.endswith('Id'): rel_name = target_field[:-2]
                        elif target_field.endswith('__c'): rel_name = target_field.replace('__c', '__r')

                    rel_ext_id = mapping.get("relationalExtIdField")
                    
                    if rel_ext_id:
                        if rel_ext_id.lower() == "id":
                            target_record[target_field] = csv_val
                        else:
                            target_record[rel_name] = {rel_ext_id: csv_val}
                            
                        if is_patch_mode: has_patch_data = True
                    else:
                        target_record[target_field] = csv_val
                        if is_patch_mode and mapping.get("type") == "reference": has_patch_data = True
                
                elif target_crm == "zoho":
                    rel_ext_id = mapping.get("relationalExtIdField")
                    
                    #  If rel_ext_id exists, always treat it as a lookup dictionary
                    if rel_ext_id:
                        if rel_ext_id.lower() == "id":
                            target_record[target_field] = csv_val
                        else:
                            # Safely wrap it in the Zoho External ID dictionary
                            target_record[target_field] = {rel_ext_id: csv_val}
                            
                        if is_patch_mode: has_patch_data = True
                    else:
                        target_record[target_field] = csv_val
                        
                    if is_patch_mode and mapping.get("type") == "reference": has_patch_data = True
                
                elif target_crm == "zendesk":
                    zd_standard_objects = {"tickets", "users", "organizations", "groups", "macros", "triggers", "views"}
                    zd_obj_lower = (target_object or "").strip().lower()
                    zd_is_standard = zd_obj_lower in zd_standard_objects or f"{zd_obj_lower}s" in zd_standard_objects

                    if target_ext_id_field and target_field == target_ext_id_field:
                        target_record["external_id"] = csv_val
                    elif target_field.startswith("custom_field_"):
                        if "custom_fields" not in target_record:
                            target_record["custom_fields"] = []
                        
                        cf_id = int(target_field.replace("custom_field_", ""))
                        target_record["custom_fields"].append({"id": cf_id, "value": csv_val})
                    elif not zd_is_standard and target_field not in ("name", "external_id", "id"):
                        if "custom_object_fields" not in target_record:
                            target_record["custom_object_fields"] = {}
                        target_record["custom_object_fields"][target_field] = csv_val
                    else:
                        target_record[target_field] = csv_val
                        
                    if is_patch_mode and mapping.get("type") == "reference": has_patch_data = True

                elif target_crm == "hubspot":
                    if mapping.get("type") == "reference":
                        pass 
                    else:
                        target_record[target_field] = csv_val
                    
                    if is_patch_mode and mapping.get("type") == "reference": 
                        has_patch_data = True

            if is_patch_mode and has_patch_data and target_ext_id_field:
                key_name = "external_id" if target_crm == "zendesk" else target_ext_id_field
                if target_record.get(key_name) in (None, ""):
                    row_errors.append(
                        f"[{target_ext_id_field}] Missing value: a patch pass cannot match the record created by the first pass."
                    )
                    skip_row = True

            if row_errors:
                if errors is not None:
                    errors.append({"originalIndex": idx, "message": "; ".join(row_errors)})
                    continue
                logger.warning("[PAYLOAD BUILDER] row %d: %s", idx, "; ".join(row_errors))
                if skip_row:
                    continue

            # Ensure External ID is present for Upserts
            if target_ext_id_field and target_ext_id_field not in target_record:
                target_record[target_ext_id_field] = None

            # Handle Delete mode payloads
            if op_mode == "delete":
                if "Id" in target_record: target_record = {"Id": target_record["Id"]}
                elif "id" in target_record: target_record = {"id": target_record["id"]}
                else: target_record = {}

            if target_record:
                if not is_patch_mode or (is_patch_mode and has_patch_data):
                    payload.append({"originalIndex": idx, "targetRecord": target_record})

        return payload

    @staticmethod
    def _dedupe_target_fields(mappings):
        """
        Two mapping rows pointed at the same target field silently overwrite each
        other inside the per-record loop below -- only the LAST one in list order
        ever actually reaches the target CRM, and the earlier source column's data
        is dropped with no error anywhere. The mapping UI now blocks this at
        selection time, but this is a backstop for mapping arrays assembled
        outside that flow (recovered sessions, direct API calls, older saved
        mappings). Keeps the LAST occurrence per target field, mirroring the
        "last row wins" convention used by dedupe_by_unique_key below.
        """
        seen: dict = {}
        no_target: list = []

        for m in mappings:
            target_field = m.get("targetField") or m.get("sfField")
            if not target_field:
                no_target.append(m)
                continue
            seen[target_field] = m

        dropped = len([m for m in mappings if (m.get("targetField") or m.get("sfField"))]) - len(seen)
        if dropped > 0:
            logger.warning(
                "[PAYLOAD BUILDER] %d duplicate target-field mapping(s) collapsed to their last occurrence.",
                dropped
            )

        return list(seen.values()) + no_target

    @staticmethod
    def dedupe_by_unique_key(payload, source_records, key_field):
        """
        Guards UPDATE/UPSERT batches against sending two rows with the same
        unique-key value in a single request. Bulk/batch upsert APIs (Salesforce
        Bulk API, HubSpot batch upsert, Zoho bulk upsert, Zendesk
        create_or_update_many / custom object jobs) don't guarantee deterministic
        behavior when two rows in the *same* call match the same key -- results
        range from a hard API rejection to one row silently overwriting the
        other with no error.

        Keeps the LAST occurrence of each key value (mirrors "last row in the
        source file wins") and reports every earlier occurrence as skipped so
        it shows up in the audit trail instead of disappearing.

        Returns (deduped_payload, skipped_records). skipped_records are ready to
        append directly to all_skipped_data.
        """
        if not key_field:
            return payload, []

        last_index_for_key = {}
        for item in payload:
            key_val = item["targetRecord"].get(key_field)
            if key_val in (None, ""):
                continue
            last_index_for_key[key_val] = item["originalIndex"]

        winning_indices = set(last_index_for_key.values())

        deduped_payload = []
        skipped_records = []

        for item in payload:
            key_val = item["targetRecord"].get(key_field)
            if key_val in (None, "") or item["originalIndex"] in winning_indices:
                deduped_payload.append(item)
            else:
                orig_record = dict(source_records[item["originalIndex"]])
                orig_record["Target_SkipReason"] = (
                    f"[{key_field}] Duplicate value '{key_val}' appeared more than once in this "
                    f"batch. Only the last occurrence was sent; this row was skipped to avoid an "
                    f"ambiguous match."
                )
                skipped_records.append(orig_record)

        return deduped_payload, skipped_records