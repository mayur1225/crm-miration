import logging
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple
from xml.sax.saxutils import escape as xml_escape

import httpx

from app.services.crm_service import CrmService

logger = logging.getLogger(__name__)

PICKLIST_TYPES = {"picklist", "multipicklist"}
_SVS_ENTRY_FIELDS = [
    "fullName", "allowEmail", "closed", "color", "converted", "cssExposed", "default",
    "description", "forecastCategory", "groupingString", "highPriority", "isActive",
    "label", "probability", "reverseRole", "reviewed", "won",
]


_SVS_NEW_ENTRY_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "opportunitystage": {"closed": False, "won": False, "probability": 10, "forecastCategory": "Pipeline"},
    "casestatus": {"closed": False},
    "leadstatus": {"converted": False},
    "taskstatus": {"closed": False},
    "taskpriority": {"highPriority": False},
}

_GLOBAL_VALUE_SET_ID = re.compile(r"^0Nt[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$")
_MAX_OVERLAP_CANDIDATES = 5


class PicklistSyncError(Exception):
    pass


def _soql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _active_values(field_meta: Dict[str, Any]) -> set:
    return {
        str(v.get("value", "")).strip().lower()
        for v in (field_meta.get("picklistValues") or [])
        if v.get("active", True) and str(v.get("value", "")).strip()
    }


class _SalesforceSession:
    API_VERSION = "v60.0"

    def __init__(self, client: httpx.AsyncClient, creds: Dict[str, Any], user_id: str, role: str):
        self.client = client
        self.token: str = creds.get("access_token") or ""
        self.instance_url: str = (creds.get("instance_url") or "").rstrip("/")
        self.user_id = user_id
        self.role = role
        self._describe_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.svs_names: Optional[List[str]] = None
        self.svs_resolution_cache: Dict[Tuple[str, str], str] = {}

        if not self.token or not self.instance_url:
            raise PicklistSyncError(f"Missing Salesforce credentials for {role} org.")

    async def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        url = f"{self.instance_url}/services/data/{self.API_VERSION}{path}"
        for attempt in range(2):
            res = await self.client.request(
                method,
                url,
                headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
                **kwargs,
            )
            if res.status_code == 401 and attempt == 0:
                self.token = await CrmService.refresh_crm_token(self.user_id, "salesforce", self.role)
                continue
            if res.status_code >= 400:
                raise PicklistSyncError(f"{method} {path} -> {res.status_code}: {res.text[:500]}")
            return res
        raise PicklistSyncError(f"{method} {path} failed after token refresh.")

    async def describe_fields(self, object_name: str, refresh: bool = False) -> Dict[str, Dict[str, Any]]:
        if refresh or object_name not in self._describe_cache:
            res = await self.request("GET", f"/sobjects/{object_name}/describe")
            self._describe_cache[object_name] = {f["name"]: f for f in res.json().get("fields", [])}
        return self._describe_cache[object_name]

    async def tooling_query(self, soql: str) -> List[Dict[str, Any]]:
        res = await self.request("GET", "/tooling/query/", params={"q": soql})
        return res.json().get("records", [])

    async def tooling_patch_metadata(self, sobject: str, record_id: str, metadata: Dict[str, Any]) -> None:
        await self.request("PATCH", f"/tooling/sobjects/{sobject}/{record_id}", json={"Metadata": metadata})

    # ------------------------------------------------------------------
    # Metadata SOAP API
    # ------------------------------------------------------------------
    async def _soap(self, body_xml: str) -> ET.Element:
        url = f"{self.instance_url}/services/Soap/m/{self.API_VERSION.lstrip('v')}"

        res: Optional[httpx.Response] = None
        for attempt in range(2):
            envelope = (
                '<?xml version="1.0" encoding="utf-8"?>'
                '<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/" '
                'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                'xmlns:meta="http://soap.sforce.com/2006/04/metadata">'
                f"<env:Header><meta:SessionHeader><meta:sessionId>{xml_escape(self.token)}</meta:sessionId>"
                "</meta:SessionHeader></env:Header>"
                f"<env:Body>{body_xml}</env:Body></env:Envelope>"
            )
            res = await self.client.post(
                url,
                content=envelope.encode("utf-8"),
                headers={"Content-Type": "text/xml; charset=UTF-8", "SOAPAction": '""'},
            )
            if "INVALID_SESSION_ID" in res.text and attempt == 0:
                self.token = await CrmService.refresh_crm_token(self.user_id, "salesforce", self.role)
                continue
            break

        if res is None:
            raise PicklistSyncError("Metadata API request was never sent.")

        try:
            root = ET.fromstring(res.text)
        except ET.ParseError:
            raise PicklistSyncError(f"Metadata API returned an unreadable response ({res.status_code}): {res.text[:300]}")

        faults = self._texts(root, "faultstring")
        if faults:
            raise PicklistSyncError(f"Metadata API fault: {faults[0]}")
        return root

    @staticmethod
    def _texts(root: ET.Element, tag: str) -> List[str]:
        return [(el.text or "").strip() for el in root.iter() if el.tag.split("}")[-1] == tag]

    async def metadata_list_names(self, metadata_type: str) -> List[str]:
        """listMetadata is the only supported way to enumerate StandardValueSet names
        (the Tooling StandardValueSet object cannot be queried without a filter)."""
        root = await self._soap(
            "<meta:listMetadata><meta:queries>"
            f"<meta:type>{xml_escape(metadata_type)}</meta:type>"
            f"</meta:queries><meta:asOfVersion>{self.API_VERSION.lstrip('v')}</meta:asOfVersion>"
            "</meta:listMetadata>"
        )
        return sorted({name for name in self._texts(root, "fullName") if name})

    @staticmethod
    def _svs_entry_xml(entry: Dict[str, Any]) -> str:
        normalized = dict(entry)
        if "valueName" in normalized:
            normalized["fullName"] = normalized.pop("valueName")

        parts = []
        for key in _SVS_ENTRY_FIELDS:
            value = normalized.get(key)
            if value is None:
                continue
            text = str(value).lower() if isinstance(value, bool) else xml_escape(str(value))
            parts.append(f"<meta:{key}>{text}</meta:{key}>")
        return f"<meta:standardValue>{''.join(parts)}</meta:standardValue>"

    async def metadata_update_standard_value_set(
        self, full_name: str, sorted_flag: bool, entries: List[Dict[str, Any]]
    ) -> None:
        root = await self._soap(
            "<meta:updateMetadata>"
            '<meta:metadata xsi:type="meta:StandardValueSet">'
            f"<meta:fullName>{xml_escape(full_name)}</meta:fullName>"
            f"<meta:sorted>{'true' if sorted_flag else 'false'}</meta:sorted>"
            f"{''.join(self._svs_entry_xml(e) for e in entries)}"
            "</meta:metadata></meta:updateMetadata>"
        )
        if "true" not in [s.lower() for s in self._texts(root, "success")]:
            raise PicklistSyncError(
                f"updateMetadata failed: {'; '.join(self._texts(root, 'message')) or 'no success flag returned'}"
            )


class PicklistSyncService:
    @staticmethod
    def _split_api_name(field_name: str) -> Tuple[Optional[str], str]:
        base = field_name[:-3] if field_name.endswith("__c") else field_name
        if "__" in base:
            ns, dev = base.split("__", 1)
            return ns, dev
        return None, base

    @staticmethod
    def _missing_values(source_field: Dict[str, Any], target_field: Dict[str, Any]) -> List[Dict[str, str]]:
        existing = {
            str(v.get("value", "")).strip().lower()
            for v in (target_field.get("picklistValues") or [])
            if v.get("active", True)
        }
        missing, seen = [], set()
        for v in source_field.get("picklistValues") or []:
            value = v.get("value")
            if value is None or not v.get("active", True):
                continue
            key = str(value).strip().lower()
            if not key or key in existing or key in seen:
                continue
            seen.add(key)
            missing.append({"value": str(value), "label": str(v.get("label") or value)})
        return missing

    @staticmethod
    def _append_entries(
        entries: List[Dict[str, Any]],
        missing: List[Dict[str, str]],
        extras: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
 
        for e in entries:
            if e.get("isActive") is None:
                e["isActive"] = True

        index = {str(e.get("valueName", "")).strip().lower(): e for e in entries}
        added = []
        for m in missing:
            key = m["value"].strip().lower()
            existing = index.get(key)
            if existing is not None:
                if existing.get("isActive") is False:
                    existing["isActive"] = True
                    added.append(m["value"])
                continue
            entries.append({
                "label": m["label"],
                "valueName": m["value"],
                "default": False,
                "isActive": True,
                "color": None,
                "description": None,
                **(extras or {}),
            })
            added.append(m["value"])
        return added

    # ------------------------------------------------------------------
    # Dynamic StandardValueSet resolution
    # ------------------------------------------------------------------
    @classmethod
    async def _standard_value_set_exists(cls, session: _SalesforceSession, name: str) -> bool:
        recs = await session.tooling_query(
            f"SELECT Id FROM StandardValueSet WHERE MasterLabel = '{_soql_escape(name)}'"
        )
        return bool(recs)

    @classmethod
    async def _list_standard_value_set_names(cls, session: _SalesforceSession) -> List[str]:
        if session.svs_names is None:
            try:
                session.svs_names = await session.metadata_list_names("StandardValueSet")
            except PicklistSyncError as e:
                logger.warning("listMetadata(StandardValueSet) failed: %s", e)
                session.svs_names = []
        return session.svs_names

    @classmethod
    async def _resolve_via_entity_particle(
        cls, session: _SalesforceSession, obj: str, field_name: str
    ) -> Optional[str]:
        """EntityParticle.ValueTypeId carries the value-set name for standard picklists.
        The candidate is verified with a filtered StandardValueSet query before use."""
        try:
            recs = await session.tooling_query(
                "SELECT ValueTypeId FROM EntityParticle "
                f"WHERE EntityDefinition.QualifiedApiName = '{_soql_escape(obj)}' "
                f"AND QualifiedApiName = '{_soql_escape(field_name)}'"
            )
        except PicklistSyncError as e:
            logger.debug("EntityParticle lookup failed for %s.%s: %s", obj, field_name, e)
            return None

        value_type = ((recs[0].get("ValueTypeId") if recs else None) or "").strip()
        if not value_type or _GLOBAL_VALUE_SET_ID.match(value_type):
            return None
        try:
            return value_type if await cls._standard_value_set_exists(session, value_type) else None
        except PicklistSyncError as e:
            logger.debug("StandardValueSet verification failed for %s: %s", value_type, e)
            return None

    @classmethod
    async def _resolve_by_value_overlap(
        cls,
        session: _SalesforceSession,
        obj: str,
        field_name: str,
        field_meta: Dict[str, Any],
    ) -> Optional[str]:
        """Probes likely value-set names directly with filtered Tooling queries (no dependency on
        listMetadata), then widens with listMetadata names. A candidate is accepted only if the
        destination field's live active values overlap its entries."""
        obj_l, field_l = obj.lower(), field_name.lower()

        guesses: List[str] = []
        for g in (
            f"{obj}{field_name}",
            f"{obj}{field_name[:-4]}" if field_name.endswith("Name") else "",
            field_name,
            f"{obj}{field_name}s",
        ):
            if g and g not in guesses:
                guesses.append(g)

        names = await cls._list_standard_value_set_names(session)
        lookup = {n.lower(): n for n in names}
        candidates: List[str] = [lookup.get(g.lower(), g) for g in guesses]

        fuzzy = [
            n for n in names
            if n not in candidates and (field_l in n.lower() or (n.lower() in field_l and len(n) > 3))
        ]
        fuzzy.sort(key=lambda n: (not n.lower().startswith(obj_l), len(n)))
        candidates = (candidates + fuzzy)[: _MAX_OVERLAP_CANDIDATES + len(guesses)]

        target_values = _active_values(field_meta)
        tried: List[str] = []
        for name in candidates:
            try:
                recs = await session.tooling_query(
                    f"SELECT Id, Metadata FROM StandardValueSet WHERE MasterLabel = '{_soql_escape(name)}'"
                )
            except PicklistSyncError as e:
                tried.append(f"{name} (query error: {str(e)[:120]})")
                continue
            if not recs:
                tried.append(f"{name} (not found)")
                continue

            set_values = {
                str(e.get("valueName", "")).strip().lower()
                for e in (recs[0].get("Metadata") or {}).get("standardValue") or []
            }
            if not target_values:
                return name
            overlap = len(target_values & set_values) / len(target_values)
            if overlap >= 0.5:
                return name
            tried.append(f"{name} (overlap {overlap:.0%})")

        logger.warning("Value-set resolution for %s.%s tried: %s", obj, field_name, "; ".join(tried) or "nothing")
        return None

    @classmethod
    async def _resolve_standard_value_set(
        cls, session: _SalesforceSession, obj: str, field_name: str, field_meta: Dict[str, Any]
    ) -> str:
        cache_key = (obj.lower(), field_name)
        if cache_key in session.svs_resolution_cache:
            return session.svs_resolution_cache[cache_key]

        resolved = (
            await cls._resolve_via_entity_particle(session, obj, field_name)
            or await cls._resolve_by_value_overlap(session, obj, field_name, field_meta)
        )
        if not resolved:
            raise PicklistSyncError(
                f"'{obj}.{field_name}' is a standard picklist but no matching StandardValueSet could be "
                f"resolved in the destination org. Check the server log line "
                f"'Value-set resolution for {obj}.{field_name} tried: ...' for the candidates and why each failed."
            )
        session.svs_resolution_cache[cache_key] = resolved
        return resolved

    # ------------------------------------------------------------------
    # Value-set writers
    # ------------------------------------------------------------------
    @classmethod
    async def _add_to_global_value_set(cls, session: _SalesforceSession, name: str, missing) -> List[str]:
        recs = await session.tooling_query(
            f"SELECT Id, Metadata FROM GlobalValueSet WHERE DeveloperName = '{_soql_escape(name)}'"
        )
        if not recs:
            raise PicklistSyncError(f"Global value set '{name}' not found in destination org.")
        metadata = recs[0]["Metadata"]
        metadata["customValue"] = metadata.get("customValue") or []
        added = cls._append_entries(metadata["customValue"], missing)
        if added:
            await session.tooling_patch_metadata("GlobalValueSet", recs[0]["Id"], metadata)
        return added

    @classmethod
    async def _add_to_standard_value_set(cls, session: _SalesforceSession, name: str, missing) -> List[str]:
        recs = await session.tooling_query(
            f"SELECT Id, Metadata FROM StandardValueSet WHERE MasterLabel = '{_soql_escape(name)}'"
        )
        if not recs:
            raise PicklistSyncError(f"Standard value set '{name}' not found in destination org.")

        metadata = recs[0]["Metadata"]
        entries = metadata.get("standardValue") or []
        extras = _SVS_NEW_ENTRY_DEFAULTS.get(name.lower())
        added = cls._append_entries(entries, missing, extras)
        if added:
            await session.metadata_update_standard_value_set(
                name, bool(metadata.get("sorted", False)), entries
            )
        return added

    @classmethod
    async def _add_to_custom_field(cls, session: _SalesforceSession, obj: str, field_name: str, missing) -> List[str]:
        ns, dev = cls._split_api_name(field_name)
        where = (
            f"EntityDefinition.QualifiedApiName = '{_soql_escape(obj)}' "
            f"AND DeveloperName = '{_soql_escape(dev)}'"
        )
        if ns:
            where += f" AND NamespacePrefix = '{_soql_escape(ns)}'"
        recs = await session.tooling_query(f"SELECT Id, Metadata FROM CustomField WHERE {where}")
        if not recs:
            raise PicklistSyncError(f"Custom field '{obj}.{field_name}' not found in destination org.")

        metadata = recs[0]["Metadata"]
        value_set = metadata.get("valueSet") or {}

        if value_set.get("valueSetName"):
            return await cls._add_to_global_value_set(session, value_set["valueSetName"], missing)

        definition = value_set.get("valueSetDefinition") or {"sorted": False, "value": []}
        definition["value"] = definition.get("value") or []
        added = cls._append_entries(definition["value"], missing)
        if added:
            value_set["valueSetDefinition"] = definition
            metadata["valueSet"] = value_set
            await session.tooling_patch_metadata("CustomField", recs[0]["Id"], metadata)
        return added

    @classmethod
    async def _add_values(
        cls,
        session: _SalesforceSession,
        obj: str,
        field_name: str,
        missing,
        field_meta: Dict[str, Any],
    ) -> List[str]:
        if field_meta.get("custom") or field_name.endswith("__c"):
            return await cls._add_to_custom_field(session, obj, field_name, missing)
        std_set = await cls._resolve_standard_value_set(session, obj, field_name, field_meta)
        return await cls._add_to_standard_value_set(session, std_set, missing)

    @classmethod
    async def sync(
        cls,
        client: httpx.AsyncClient,
        source_creds: Dict[str, Any],
        target_creds: Dict[str, Any],
        user_id: str,
        source_object: str,
        target_object: str,
        mappings: List[Dict[str, Any]],
        send_log,
    ) -> Dict[str, Dict[str, Any]]:
        created: Dict[str, List[str]] = {}
        failed: Dict[str, str] = {}
        try:
            source = _SalesforceSession(client, source_creds, user_id, "source")
            target = _SalesforceSession(client, target_creds, user_id, "target")
            source_fields = await source.describe_fields(source_object)
            target_fields = await target.describe_fields(target_object)
        except PicklistSyncError as e:
            await send_log(f"[{target_object}] Picklist Sync skipped: {e}")
            return {"created": created, "failed": {"*": str(e)}}

        for m in mappings:
            src_name = m.get("sourceField") or m.get("csvField")
            tgt_name = m.get("targetField") or m.get("sfField")
            src_meta, tgt_meta = source_fields.get(src_name), target_fields.get(tgt_name)

            if not src_meta or not tgt_meta:
                await send_log(f"[{target_object}] Picklist Sync: field metadata not found for {src_name} -> {tgt_name}.")
                continue
            if src_meta.get("type") not in PICKLIST_TYPES or tgt_meta.get("type") not in PICKLIST_TYPES:
                await send_log(f"[{target_object}] Picklist Sync: {src_name} -> {tgt_name} is not a picklist pair; skipped.")
                continue

            missing = cls._missing_values(src_meta, tgt_meta)
            if not missing:
                await send_log(f"[{target_object}] Picklist Sync: {tgt_name} already contains all source values.")
                continue

            await send_log(
                f"[{target_object}] Picklist Sync: creating {len(missing)} missing value(s) in {tgt_name}: "
                f"{', '.join(v['value'] for v in missing)}"
            )
            try:
                added = await cls._add_values(target, target_object, tgt_name, missing, tgt_meta)
                created[tgt_name] = added
                await send_log(f"[{target_object}] Picklist Sync: {tgt_name} updated ({len(added)} value(s) added).")

                fresh = (await target.describe_fields(target_object, refresh=True)).get(tgt_name) or {}
                live = {
                    str(v.get("value", "")).strip().lower(): v.get("active", True)
                    for v in fresh.get("picklistValues") or []
                }
                not_active = [v for v in added if live.get(v.strip().lower()) is not True]
                if not_active:
                    failed[tgt_name] = f"not active after write: {', '.join(not_active)}"
                    await send_log(
                        f"[{target_object}] Picklist Sync WARNING: {tgt_name} values not active after write: "
                        f"{', '.join(not_active)}"
                    )
                else:
                    await send_log(f"[{target_object}] Picklist Sync: verified {tgt_name} values are active.")
            except (PicklistSyncError, httpx.HTTPError) as e:
                failed[tgt_name] = str(e)
                logger.warning("Picklist sync failed for %s.%s: %s", target_object, tgt_name, e)
                await send_log(f"[{target_object}] Picklist Sync failed for {tgt_name}: {e}")

        return {"created": created, "failed": failed}