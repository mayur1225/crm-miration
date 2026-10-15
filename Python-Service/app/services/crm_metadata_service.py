import httpx
import urllib.parse 
from fastapi import HTTPException

client = httpx.AsyncClient(timeout=30.0)


def _session_expired(crm: str) -> HTTPException:
    return HTTPException(
        status_code=401,
        detail={
            "code": "CRM_SESSION_EXPIRED",
            "crm": crm,
            "message": f"Your {crm} session has expired. Please log in again.",
        },
    )


def _upstream_error(e: "httpx.HTTPStatusError", crm: str, message: str) -> HTTPException:
    """Upstream 401 == dead CRM token -> coded error the frontend turns into a re-login redirect."""
    if e.response.status_code == 401:
        return _session_expired(crm)
    return HTTPException(status_code=e.response.status_code, detail=message)

class CrmMetadataService:
    
    # =========================================================
    # SALESFORCE METADATA EXTRACTION ENGINE 
    # =========================================================
    @staticmethod
    async def fetch_salesforce_objects(sf_token: str, instance_url: str):
        if not sf_token or not instance_url:
            raise _session_expired("salesforce")
        
        headers = {"Authorization": f"Bearer {sf_token}", "Content-Type": "application/json"}
        url = f"{instance_url.rstrip('/')}/services/data/v60.0/sobjects"
        
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                
                data = response.json()
                
                objects = [
                    {
                        "name": obj["name"], 
                        "label": obj["label"],
                        "keyPrefix": obj.get("keyPrefix"),
                        "isCustomMetadata": str(obj["name"]).endswith("__mdt"),
                        "isCustomObject": str(obj["name"]).endswith("__c")
                    }
                    for obj in data.get("sobjects", [])
                ]
                
                return sorted(objects, key=lambda x: x["label"])
                
        except httpx.HTTPStatusError as e:
            raise _upstream_error(e, "salesforce", f"Salesforce rejected object request: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Network error connecting to Salesforce: {str(e)}")

    @staticmethod
    async def fetch_salesforce_fields(sf_token: str, instance_url: str, object_name: str):
        if not sf_token or not instance_url:
            raise _session_expired("salesforce")
            
        headers = {"Authorization": f"Bearer {sf_token}", "Content-Type": "application/json"}
        base_url = instance_url.rstrip('/')
        
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                # 1. Fetch Schema
                describe_url = f"{base_url}/services/data/v60.0/sobjects/{object_name}/describe"
                desc_res = await client.get(describe_url, headers=headers)
                desc_res.raise_for_status()
                
                fields_raw = desc_res.json().get("fields", [])
                
                parsed_fields = []
                select_fields_list = []
                
                for f in fields_raw:
                    if f["type"] in ["address", "location"]:
                        continue

                    if f.get("createable") or f.get("updateable") or f.get("name") == "Id":
                        select_fields_list.append(f["name"])
                        
                    is_required = (not f.get("nillable", True)) and f.get("createable", False) and (not f.get("defaultedOnCreate", False))
                    
                    parsed_fields.append({
                        "name": f["name"],
                        "label": f["label"],
                        "type": f["type"],
                        "length": f.get("length"),
                        "custom": f.get("custom"),
                        "isRequired": is_required,
                        "referenceTo": f.get("referenceTo") if f.get("referenceTo") else None,
                        "externalId": f.get("externalId", False),
                        "unique": f.get("unique", False),
                        "idLookup": f.get("idLookup", False),
                        "createable": f.get("createable", False),
                        "updateable": f.get("updateable", False)
                    })

                sample_fields = select_fields_list[:15]
                sample_records = []
                
                if sample_fields:
                    soql = f"SELECT {', '.join(sample_fields)} FROM {object_name} LIMIT 5"
                    query_url = f"{base_url}/services/data/v60.0/query/?q={soql}"
                    
                    query_res = await client.get(query_url, headers=headers)
                    if query_res.status_code == 200:
                        raw_records = query_res.json().get("records", [])
                        for r in raw_records:
                            r.pop("attributes", None)
                            sample_records.append(r)

                return {
                    "headers": sample_fields[:12],
                    "sampleRecords": sample_records,
                    "fields": sorted(parsed_fields, key=lambda x: x["isRequired"], reverse=True)
                }
                
        except httpx.HTTPStatusError as e:
            raise _upstream_error(e, "salesforce", f"Salesforce schema error: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Network error connecting to Salesforce: {str(e)}")


   # =========================================================
    # ZENDESK METADATA EXTRACTION ENGINE
    # =========================================================
    @staticmethod
    async def fetch_zendesk_objects(zd_token: str = None, subdomain: str = None):
        
        # 1. Standard Objects
        objects = [
            {"name": "tickets", "label": "Tickets"},
            {"name": "users", "label": "Users"},
            {"name": "organizations", "label": "Organizations"},
            {"name": "groups", "label": "Groups"},
            {"name": "macros", "label": "Macros"},
            {"name": "triggers", "label": "Triggers"},
            {"name": "views", "label": "Views"}
        ]
        
        if not zd_token or not subdomain:
            print("Notice: Missing Zendesk credentials. Returning standard objects only.")
            return sorted(objects, key=lambda x: x["label"])
        
        headers = {"Authorization": f"Bearer {zd_token}", "Content-Type": "application/json"}
        
        url = f"https://{subdomain}.zendesk.com/api/v2/custom_objects"
        
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                res = await client.get(url, headers=headers)
                if res.status_code == 200:
                    for custom_obj in res.json().get("custom_objects", []):
                        objects.append({
                            "name": custom_obj["key"], 
                            "label": custom_obj.get("title", custom_obj["key"])
                        })
        except Exception as e:
            print(f"Notice: Network error fetching Zendesk custom objects: {str(e)}")
            
        return sorted(objects, key=lambda x: x["label"])

    @staticmethod
    async def fetch_zendesk_fields(zd_token: str, subdomain: str, object_name: str):
        if not zd_token or not subdomain:
            raise _session_expired("zendesk")
            
        headers = {"Authorization": f"Bearer {zd_token}", "Content-Type": "application/json"}
        base_url = f"https://{subdomain}.zendesk.com/api/v2"
        safe_object_name = object_name.lower()
        
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                schema_fields_map = {}
                sample_records = []
                
                standard_objects = ["tickets", "users", "organizations", "groups", "macros", "triggers", "views"]
                is_standard = safe_object_name in standard_objects
                
                if is_standard:
                    # ==========================================
                    # STANDARD ZENDESK OBJECTS
                    # ==========================================
                    singular_name = safe_object_name[:-1] if safe_object_name.endswith('s') else safe_object_name
                    
                    # 1. Fetch Schema
                    if safe_object_name in ["tickets", "users", "organizations"]:
                        meta_url = f"{base_url}/{singular_name}_fields.json"
                        meta_res = await client.get(meta_url, headers=headers)
                        
                        if meta_res.status_code == 200:
                            for f in meta_res.json().get(f"{singular_name}_fields", []):
                                field_id = f.get("id")
                                is_custom = isinstance(field_id, int)
                                api_name = f"custom_field_{field_id}" if is_custom else str(field_id)
                                
                                schema_fields_map[api_name] = {
                                    "name": api_name,
                                    "label": f.get("title", api_name),
                                    "type": f.get("type", "string"),
                                    "isRequired": f.get("required", False) or f.get("required_in_portal", False),
                                    "custom": is_custom,
                                    "referenceTo": None,
                                    "externalId": api_name in ("id", "external_id") or (api_name == "email" and singular_name == "user"),
                                    "createable": True,
                                    "updateable": True
                                }
                                
                    # 2. Fetch Sample Data
                    data_url = f"{base_url}/{safe_object_name}.json?per_page=5"
                    data_res = await client.get(data_url, headers=headers)
                    data_res.raise_for_status()
                    
                    raw_records = data_res.json().get(safe_object_name, [])
                    for rec in raw_records:
                        flat_rec = {}
                        for k, v in rec.items():
                            if k == "custom_fields" and isinstance(v, list):
                                for cf in v:
                                    flat_rec[f"custom_field_{cf['id']}"] = cf.get("value")
                            elif not isinstance(v, (dict, list)): 
                                flat_rec[k] = v
                                # Discover missing system fields dynamically
                                if k not in schema_fields_map:
                                    field_type = "boolean" if isinstance(v, bool) else "number" if isinstance(v, (int, float)) else "string"
                                    schema_fields_map[k] = {
                                        "name": k, "label": k.replace("_", " ").title(), "type": field_type,
                                        "isRequired": False,
                                        "custom": False, "referenceTo": None,
                                        "externalId": k in ("id", "external_id"),
                                        "createable": k != "id",
                                        "updateable": k != "id"
                                    }
                        sample_records.append(flat_rec)

                else:
                    # ==========================================
                    # NATIVE CUSTOM OBJECTS (Admin Center)
                    # ==========================================
                    # 1. Fetch Schema using the modern endpoint
                    meta_url = f"{base_url}/custom_objects/{safe_object_name}/fields"
                    meta_res = await client.get(meta_url, headers=headers)
                    
                    if meta_res.status_code == 200:
                        for f in meta_res.json().get("custom_object_fields", []):
                            api_name = f.get("key")
                            schema_fields_map[api_name] = {
                                "name": api_name,
                                "label": f.get("title", api_name),
                                "type": f.get("type", "string"),
                                "isRequired": False,
                                "custom": True,
                                "referenceTo": None,
                                "externalId": api_name in ("id", "external_id"),
                                "unique": False,
                                "createable": True,
                                "updateable": True
                            }
                            
                    # 2. Fetch Sample Data using the modern records endpoint
                    data_url = f"{base_url}/custom_objects/{safe_object_name}/records?per_page=5"
                    data_res = await client.get(data_url, headers=headers)
                    data_res.raise_for_status()
                    
                    raw_records = data_res.json().get("custom_object_records", [])
                    for rec in raw_records:
                        # Extract standard native system fields
                        flat_rec = {
                            "id": rec.get("id"), 
                            "name": rec.get("name"), 
                            "created_at": rec.get("created_at"), 
                            "updated_at": rec.get("updated_at")
                        }
                        
                        # Flatten custom_object_fields payload
                        c_fields = rec.get("custom_object_fields", {})
                        for k, v in c_fields.items():
                            flat_rec[k] = v
                            
                        # Discover missing system fields dynamically
                        for k, v in flat_rec.items():
                            if k not in schema_fields_map:
                                field_type = "boolean" if isinstance(v, bool) else "number" if isinstance(v, (int, float)) else "string"
                                schema_fields_map[k] = {
                                    "name": k, "label": k.replace("_", " ").title(), "type": field_type,
                                    "isRequired": k == "id", "custom": False, "referenceTo": None,
                                    "externalId": k in ("id", "external_id"),
                                    "createable": k not in ("id", "created_at", "updated_at"),
                                    "updateable": k not in ("id", "created_at", "updated_at")
                                }
                        sample_records.append(flat_rec)

                # Format and Sort Output to match Salesforce structure
                fields_list = list(schema_fields_map.values())
                fields_list = sorted(fields_list, key=lambda x: (not x["isRequired"], x["name"]))
                headers_preview = [f["name"] for f in fields_list[:12]] 
                
            return {
                "headers": headers_preview,
                "sampleRecords": sample_records,
                "fields": fields_list
            }
            
        except httpx.HTTPStatusError as e:
            raise _upstream_error(e, "zendesk", f"Zendesk data fetch error for '{object_name}': {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Zendesk network connection failed: {str(e)}")



   # =========================================================
    # ZOHO CRM METADATA EXTRACTION ENGINE
    # =========================================================
    @staticmethod
    async def fetch_zoho_objects(zoho_token: str, api_domain: str):
        if not zoho_token or not api_domain:
            raise _session_expired("zoho")

        headers = {"Authorization": f"Zoho-oauthtoken {zoho_token}"}
        
        
        url = f"{api_domain.rstrip('/')}/crm/v6/settings/modules"

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()

                data = response.json()
                objects = []
                
                for mod in data.get("modules", []):
                    if mod.get("api_supported", False) or mod.get("generated_type") == "custom":
                        objects.append({
                            "name": mod.get("api_name"),
                            "label": mod.get("plural_label") or mod.get("module_name") or mod.get("api_name")
                        })
                        
                return sorted(objects, key=lambda x: x["label"])

        except httpx.HTTPStatusError as e:
            raise _upstream_error(e, "zoho", f"Zoho rejected module request: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Network error connecting to Zoho: {str(e)}")

    @staticmethod
    async def fetch_zoho_fields(zoho_token: str, api_domain: str, module_name: str):
        if not zoho_token or not api_domain:
            raise _session_expired("zoho")

        headers = {"Authorization": f"Zoho-oauthtoken {zoho_token}"}
        
       
        base_url = f"{api_domain.rstrip('/')}/crm/v6"

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                # 1. Fetch Field Metadata Schema
                fields_url = f"{base_url}/settings/fields?module={module_name}"
                fields_res = await client.get(fields_url, headers=headers)
                fields_res.raise_for_status()

                fields_raw = fields_res.json().get("fields", [])

                type_mapping = {
                    "text": "string", "textarea": "string", "email": "string", "phone": "string", "website": "string",
                    "integer": "number", "double": "number", "currency": "number", "percent": "number",
                    "date": "date", "datetime": "date",
                    "boolean": "boolean",
                    "picklist": "picklist", "multiselectpicklist": "picklist",
                    "lookup": "reference", "ownerlookup": "reference"
                }

                parsed_fields = []
                select_fields_list = []

                dangerous_zoho_types = [
                    "subform", "multiselectlookup", "imageupload", 
                    "fileupload", "line_tax", "pricing_details", "userlookup"
                ]

                for f in fields_raw:
                    api_name = f.get("api_name")
                    if not api_name:
                        continue

                    # Safe COQL check
                    if not api_name.startswith("$") and f.get("data_type") not in dangerous_zoho_types:
                        select_fields_list.append(api_name)

                    zoho_view_type = f.get("view_type") or {}
                    zoho_is_read_only = f.get("read_only", False)

                    parsed_fields.append({
                        "name": api_name,
                        "label": f["field_label"],
                        "type": type_mapping.get(f["data_type"], "string"),
                        "isRequired": f.get("system_mandatory", False) or f.get("required", False),
                        "unique": f.get("unique") is not None,
                        "externalId": api_name == "id",
                        "createable": zoho_view_type.get("create", True) and not zoho_is_read_only,
                        "updateable": zoho_view_type.get("edit", True) and not zoho_is_read_only
                    })

                # 2. Fetch Sample Data
                records_url = f"{base_url}/{module_name}?page=1&per_page=5"
                records_res = await client.get(records_url, headers=headers)
                sample_records = []

                if records_res.status_code == 200:
                    raw_records = records_res.json().get("data", [])
                    for r in raw_records:
                        flat_rec = {}
                        for k, v in r.items():
                            if isinstance(v, dict) and "id" in v:
                                flat_rec[k] = v.get("name", v["id"]) 
                            else:
                                flat_rec[k] = v
                        sample_records.append(flat_rec)

                return {
                    "headers": select_fields_list[:12],
                    "sampleRecords": sample_records,
                    "fields": sorted(parsed_fields, key=lambda x: x["isRequired"], reverse=True)
                }

        except httpx.HTTPStatusError as e:
            raise _upstream_error(e, "zoho", f"Zoho schema error: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Network error connecting to Zoho: {str(e)}")


    # =========================================================
    # HUBSPOT METADATA EXTRACTION ENGINE
    # =========================================================
    @staticmethod
    async def fetch_hubspot_objects(hs_token: str, api_domain: str = "https://api.hubapi.com"):
        if not hs_token:
            raise _session_expired("hubspot")

        headers = {
            "Authorization": f"Bearer {hs_token}",
            "Content-Type": "application/json"
        }
        
        objects = []
        
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                standard_objects = [
                    {"name": "contacts", "label": "Contacts"},
                    {"name": "companies", "label": "Companies"},
                    {"name": "deals", "label": "Deals"},
                    {"name": "tickets", "label": "Tickets"},
                    {"name": "products", "label": "Products"},
                    {"name": "line_items", "label": "Line Items"},
                    {"name": "quotes", "label": "Quotes"},
                    {"name": "calls", "label": "Calls"},
                    {"name": "emails", "label": "Emails"},
                    {"name": "meetings", "label": "Meetings"},
                    {"name": "notes", "label": "Notes"},
                    {"name": "tasks", "label": "Tasks"}
                ]
                objects.extend(standard_objects)

                custom_url = f"{api_domain.rstrip('/')}/crm/v3/schemas"
                res = await client.get(custom_url, headers=headers)
                if res.status_code == 200:
                    for schema in res.json().get("results", []):
                        objects.append({
                            "name": schema.get("objectTypeId"),
                            "label": schema.get("labels", {}).get("plural", schema.get("name")),
                            "isCustomObject": True
                        })
                
                return sorted(objects, key=lambda x: x["label"])

        except httpx.HTTPStatusError as e:
            raise _upstream_error(e, "hubspot", f"HubSpot rejected object request: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Network error connecting to HubSpot: {str(e)}")

    @staticmethod
    async def fetch_hubspot_fields(hs_token: str, api_domain: str, object_name: str):
        if not hs_token:
            raise _session_expired("hubspot")

        headers = {
            "Authorization": f"Bearer {hs_token}",
            "Content-Type": "application/json"
        }
        base_url = f"{api_domain.rstrip('/')}/crm/v3/properties/{object_name}"

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                # 1. Fetch Schema Properties
                props_res = await client.get(base_url, headers=headers)
                props_res.raise_for_status()

                fields_raw = props_res.json().get("results", [])
                
                type_mapping = {
                    "string": "string",
                    "number": "number",
                    "date": "date",
                    "datetime": "date",
                    "enumeration": "picklist",
                    "bool": "boolean",
                    "phone_number": "string"
                }

                parsed_fields = []
                select_fields_list = []

                for f in fields_raw:
                    if f.get("hidden"):
                        continue
                        
                    api_name = f.get("name")
                    select_fields_list.append(api_name)
                    hs_read_only = (f.get("modificationMetadata") or {}).get("readOnlyValue", False)

                    parsed_fields.append({
                        "name": api_name,
                        "label": f.get("label"),
                        "type": type_mapping.get(f.get("type"), "string"),
                        "isRequired": False, 
                        "custom": not f.get("hubspotDefined", True),
                        "referenceTo": f.get("referencedObjectType"),
                        "unique": f.get("hasUniqueValue", False),
                        "externalId": api_name in ("hs_object_id", "id"),
                        "createable": not hs_read_only,
                        "updateable": not hs_read_only
                    })


                sample_fields = select_fields_list[:50] 
                properties_query = "&".join([f"properties={urllib.parse.quote(p)}" for p in sample_fields])
                
                records_url = f"{api_domain.rstrip('/')}/crm/v3/objects/{object_name}?limit=5&{properties_query}"
                records_res = await client.get(records_url, headers=headers)
                
                sample_records = []
                if records_res.status_code == 200:
                    raw_records = records_res.json().get("results", [])
                    for r in raw_records:
                        flat_rec = {"id": r.get("id")}
                        
                        props = r.get("properties", {})
                        if props:
                            for k, v in props.items():
                                flat_rec[k] = v
                                
                        sample_records.append(flat_rec)

                return {
                    "headers": sample_fields[:12],
                    "sampleRecords": sample_records,
                    "fields": sorted(parsed_fields, key=lambda x: x["label"])
                }

        except httpx.HTTPStatusError as e:
            raise _upstream_error(e, "hubspot", f"HubSpot schema error for '{object_name}': {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Network error connecting to HubSpot: {str(e)}")