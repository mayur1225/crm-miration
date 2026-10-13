# app/api/routes/picklist_diagnostics_routes.py
import logging
from typing import Any, Awaitable, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.dependencies.auth import get_current_user
from app.services.crm_service import CrmService
from app.services.picklist_sync_service import (
    PicklistSyncError,
    PicklistSyncService,
    _SalesforceSession,
    _soql_escape,
)

logger = logging.getLogger(__name__)
router = APIRouter()


async def _safe(coro: Awaitable[Any]) -> Dict[str, Any]:
    """Each diagnostic section reports its own failure instead of aborting the whole report."""
    try:
        return {"ok": True, "data": await coro}
    except (PicklistSyncError, httpx.HTTPError, KeyError, IndexError, ValueError) as e:
        return {"ok": False, "error": str(e)[:600]}


async def _org(s: _SalesforceSession) -> Dict[str, Any]:
    res = await s.request(
        "GET", "/query/", params={"q": "SELECT Id, Name, IsSandbox, InstanceName FROM Organization"}
    )
    rec = res.json()["records"][0]
    rec.pop("attributes", None)
    rec["instance_url"] = s.instance_url
    return rec


async def _permissions(s: _SalesforceSession) -> Dict[str, Any]:
    info = await s.client.get(
        f"{s.instance_url}/services/oauth2/userinfo", headers={"Authorization": f"Bearer {s.token}"}
    )
    info.raise_for_status()
    uid = _soql_escape(info.json()["user_id"])
    res = await s.request(
        "GET",
        "/query/",
        params={
            "q": "SELECT PermissionsCustomizeApplication, PermissionsModifyMetadata, PermissionsModifyAllData "
                 "FROM PermissionSet WHERE Id IN "
                 f"(SELECT PermissionSetId FROM PermissionSetAssignment WHERE AssigneeId = '{uid}')"
        },
    )
    rows = res.json()["records"]
    return {
        "userId": uid,
        "customizeApplication": any(r["PermissionsCustomizeApplication"] for r in rows),
        "modifyMetadata": any(r["PermissionsModifyMetadata"] for r in rows),
        "modifyAllData": any(r["PermissionsModifyAllData"] for r in rows),
    }


def _matches(entry_value: Any, wanted: Optional[str]) -> bool:
    return not wanted or str(entry_value or "").strip().lower() == wanted.strip().lower()


async def _field_view(meta: Dict[str, Any], value: Optional[str]) -> Dict[str, Any]:
    values = meta.get("picklistValues") or []
    return {
        "type": meta.get("type"),
        "custom": meta.get("custom"),
        "restrictedPicklist": meta.get("restrictedPicklist"),
        "controllerName": meta.get("controllerName"),
        "totalValues": len(values),
        "describeEntries": [
            {"value": v.get("value"), "active": v.get("active")}
            for v in values
            if _matches(v.get("value"), value)
        ],
    }


async def _value_set(
    s: _SalesforceSession, obj: str, field: str, meta: Dict[str, Any], value: Optional[str]
) -> Dict[str, Any]:
    if meta.get("custom") or field.endswith("__c"):
        ns, dev = PicklistSyncService._split_api_name(field)
        where = (
            f"EntityDefinition.QualifiedApiName = '{_soql_escape(obj)}' "
            f"AND DeveloperName = '{_soql_escape(dev)}'"
        )
        if ns:
            where += f" AND NamespacePrefix = '{_soql_escape(ns)}'"
        recs = await s.tooling_query(f"SELECT Id, Metadata FROM CustomField WHERE {where}")
        if not recs:
            raise PicklistSyncError(f"CustomField {obj}.{field} not found via Tooling API.")
        vs = recs[0]["Metadata"].get("valueSet") or {}
        entries = vs.get("valueSetName") and [] or (vs.get("valueSetDefinition") or {}).get("value") or []
        return {
            "kind": "custom",
            "globalValueSet": vs.get("valueSetName"),
            "entries": [
                {"valueName": e.get("valueName"), "isActive": e.get("isActive")}
                for e in entries
                if _matches(e.get("valueName"), value)
            ],
        }

    name = await PicklistSyncService._resolve_standard_value_set(s, obj, field, meta)
    recs = await s.tooling_query(
        f"SELECT Id, Metadata FROM StandardValueSet WHERE MasterLabel = '{_soql_escape(name)}'"
    )
    entries = (recs[0]["Metadata"].get("standardValue") or []) if recs else []
    return {
        "kind": "standard",
        "valueSetName": name,
        "totalEntries": len(entries),
        "entries": [
            {"valueName": e.get("valueName"), "isActive": e.get("isActive")}
            for e in entries
            if _matches(e.get("valueName"), value)
        ],
    }


async def _record_types(s: _SalesforceSession, obj: str, field: str, value: Optional[str]) -> Any:
    res = await s.request("GET", f"/sobjects/{obj}/describe")
    rts = [
        r for r in res.json().get("recordTypeInfos") or []
        if r.get("active") and not r.get("master")
    ]
    out = []
    for rt in rts:
        r = await s.request("GET", f"/ui-api/object-info/{obj}/picklist-values/{rt['recordTypeId']}/{field}")
        available = {str(v.get("value", "")).strip().lower() for v in r.json().get("values") or []}
        out.append({
            "recordType": rt.get("name"),
            "valueCount": len(available),
            "valueAvailable": (value.strip().lower() in available) if value else None,
        })
    return out or "no active non-master record types"


@router.get("/api/metadata/salesforce/picklist-diagnose")
async def picklist_diagnose(
    field: str,
    object_name: str = Query(..., alias="object"),
    value: Optional[str] = None,
    role: str = "target",
    current_user=Depends(get_current_user),
) -> Dict[str, Any]:
    creds = CrmService.get_active_crm_credentials(current_user.id, "salesforce", role)
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            s = _SalesforceSession(client, creds, current_user.id, role)
            meta = (await s.describe_fields(object_name, refresh=True)).get(field)
        except (PicklistSyncError, httpx.HTTPError) as e:
            raise HTTPException(status_code=502, detail=str(e)[:500])
        if not meta:
            raise HTTPException(status_code=404, detail=f"{object_name}.{field} not found in {role} org.")

        return {
            "org": await _safe(_org(s)),
            "userPermissions": await _safe(_permissions(s)),
            "describe": await _safe(_field_view(meta, value)),
            "valueSet": await _safe(_value_set(s, object_name, field, meta, value)),
            "recordTypes": await _safe(_record_types(s, object_name, field, value)),
        }