"""
Stable "which org is this?" key for a saved CRM connection.

Two users connected to the same org must produce the SAME key, so this uses the org/portal/account id
when the connection row has one, and falls back to the instance URL / API domain / subdomain.

NOTE: Zoho `api_domain` (e.g. https://www.zohoapis.com) and HubSpot's API host are shared by every
customer in a region, so the fallback is NOT unique for those two. Store an org id on the connection
at OAuth time (Zoho: org id from /crm/v6/org, HubSpot: portalId from the token info) under one of the
_ID_KEYS columns and this module will pick it up automatically.
"""
from typing import Any, Dict, Optional

_ID_KEYS = ("org_id", "organization_id", "portal_id", "hub_id", "account_id")
_URL_KEYS = ("instance_url", "api_domain", "subdomain")


def org_key(crm_type: Optional[str], creds: Optional[Dict[str, Any]]) -> Optional[str]:
    if not creds:
        return None
    crm = (crm_type or creds.get("crm_type") or "").strip().lower()
    for k in _ID_KEYS:
        v = creds.get(k)
        if v:
            return f"{crm}:id:{str(v).strip().lower()}"
    for k in _URL_KEYS:
        v = creds.get(k)
        if v:
            v = str(v).strip().lower().rstrip("/")
            for prefix in ("https://", "http://"):
                if v.startswith(prefix):
                    v = v[len(prefix):]
            return f"{crm}:url:{v}"
    return None