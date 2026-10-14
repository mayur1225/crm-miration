"""
Stable "which org is this?" key for a saved CRM connection.

Two users connected to the SAME org must produce the SAME key, and two DIFFERENT orgs must never collide.

  1. org/portal/account id stored on the connection at OAuth time (strong):
       Salesforce -> 18-char org id from the token `id` URL, Zoho -> /crm/v6/org id, HubSpot -> hub_id
  2. else instance_url / subdomain (strong for Salesforce and Zendesk: unique per org)
  3. Zoho `api_domain` and HubSpot's API host are shared by EVERY customer of a region, so they are never
     used as an identity. Without a stored org id the key is None and the run stays owner-only
     (fail closed: no cross-user sharing, no org-mismatch guard).
"""
from typing import Any, Dict, Optional

_ID_KEYS = ("org_id", "organization_id", "portal_id", "hub_id", "account_id")
_URL_KEYS = ("instance_url", "subdomain")
_SHARED_HOST_CRMS = {"zoho", "hubspot"}


def org_key(crm_type: Optional[str], creds: Optional[Dict[str, Any]]) -> Optional[str]:
    if not creds:
        return None
    crm = (crm_type or creds.get("crm_type") or "").strip().lower()
    for k in _ID_KEYS:
        v = creds.get(k)
        if v:
            return f"{crm}:id:{str(v).strip().lower()}"
    if crm in _SHARED_HOST_CRMS:
        return None
    for k in _URL_KEYS:
        v = creds.get(k)
        if v:
            v = str(v).strip().lower().rstrip("/")
            for prefix in ("https://", "http://"):
                if v.startswith(prefix):
                    v = v[len(prefix):]
            return f"{crm}:url:{v}"
    return None


def org_label(key: Optional[str]) -> str:
    """Human-readable part of a key for error messages."""
    return key.split(":", 2)[-1] if key else "unknown"