"""
Stable "which org is this?" identity for a saved CRM connection.

Two users connected to the SAME org must share at least one alias, and two DIFFERENT orgs must never share one.
An identity is a set of aliases (strongest first), so a run started with one alias is still found by a colleague
whose connection only yields another:

  1. org/portal/account id stored on the connection at OAuth time
  2. else the id read LIVE from the CRM with the connection's token (cached):
       Salesforce -> /services/oauth2/userinfo organization_id, Zoho -> /crm/v6/org id (needs ZohoCRM.org.READ),
       HubSpot -> /account-info/v3/details portalId
  3. instance_url / subdomain for CRMs where it is unique per org (Salesforce, Zendesk)

Zoho `api_domain` and HubSpot's API host are shared by EVERY customer of a region, so they are never an identity.
With no alias at all the run stays owner-only (fail closed).
"""
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

_ID_KEYS = ("org_id", "organization_id", "portal_id", "hub_id", "account_id")
_URL_KEYS = ("instance_url", "subdomain")
_SHARED_HOST_CRMS = {"zoho", "hubspot"}

_IDENTITY_TTL = 24 * 3600.0
_NEGATIVE_TTL = 60.0
_IDENTITY_TIMEOUT = 5.0
_identity_cache: Dict[Tuple[str, str], Tuple[Optional[str], float]] = {}


def _live_identity(crm: str, creds: Dict[str, Any]) -> Optional[str]:
    """Org id read from the CRM itself. Blocking (short timeout) and cached; call via a thread from async code."""
    token = creds.get("access_token")
    if not token or crm not in ("salesforce", "zoho", "hubspot"):
        return None

    cache_key = (crm, hashlib.sha256(str(token).encode()).hexdigest()[:16])  # new token / reconnect -> fresh lookup
    now = time.monotonic()
    hit = _identity_cache.get(cache_key)
    if hit and hit[1] > now:
        return hit[0]

    value: Optional[str] = None
    try:
        with httpx.Client(timeout=_IDENTITY_TIMEOUT) as client:
            if crm == "salesforce":
                base = (creds.get("instance_url") or "").rstrip("/")
                if base:
                    res = client.get(f"{base}/services/oauth2/userinfo", headers={"Authorization": f"Bearer {token}"})
                    if res.status_code == 200:
                        value = res.json().get("organization_id")
            elif crm == "zoho":
                base = (creds.get("api_domain") or "https://www.zohoapis.com").rstrip("/")
                res = client.get(f"{base}/crm/v6/org", headers={"Authorization": f"Zoho-oauthtoken {token}"})
                if res.status_code == 200:
                    value = ((res.json().get("org") or [{}])[0]).get("id")
            elif crm == "hubspot":
                res = client.get("https://api.hubapi.com/account-info/v3/details", headers={"Authorization": f"Bearer {token}"})
                if res.status_code == 200:
                    value = res.json().get("portalId")
    except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError, AttributeError) as exc:
        logger.warning("[ORG] could not read the %s org id: %s", crm, exc)
        value = None

    value = str(value).strip().lower() if value else None
    _identity_cache[cache_key] = (value, now + (_IDENTITY_TTL if value else _NEGATIVE_TTL))
    if not value:
        logger.info("[ORG] no live org id for %s (token rejected, scope missing or CRM unreachable).", crm)
    return value


def org_aliases(crm_type: Optional[str], creds: Optional[Dict[str, Any]], live: bool = True) -> List[str]:
    """Every stable key this connection can be known by, strongest first. Empty = identity unknown (owner-only)."""
    if not creds:
        return []
    crm = (crm_type or creds.get("crm_type") or "").strip().lower()
    out: List[str] = []

    for k in _ID_KEYS:
        v = creds.get(k)
        if v:
            out.append(f"{crm}:id:{str(v).strip().lower()}")

    if live and not out:
        live_id = _live_identity(crm, creds)
        if live_id:
            out.append(f"{crm}:id:{live_id}")

    if crm not in _SHARED_HOST_CRMS:
        for k in _URL_KEYS:
            v = creds.get(k)
            if v:
                v = str(v).strip().lower().rstrip("/")
                for prefix in ("https://", "http://"):
                    if v.startswith(prefix):
                        v = v[len(prefix):]
                out.append(f"{crm}:url:{v}")
                break

    return list(dict.fromkeys(out))


def org_key(crm_type: Optional[str], creds: Optional[Dict[str, Any]]) -> Optional[str]:
    """Strongest single alias (what runs store as targetOrg / sourceOrg)."""
    aliases = org_aliases(crm_type, creds)
    return aliases[0] if aliases else None


def org_label(key: Optional[str]) -> str:
    """Human-readable part of a key for error messages."""
    return key.split(":", 2)[-1] if key else "unknown"