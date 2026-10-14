import os
import base64
import hashlib
import urllib
import urllib.parse
import secrets
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
import httpx
from app.api.dependencies.auth import get_current_user
from app.services.crm_service import CrmService
from app.utils.config import supabase

router = APIRouter()

FASTAPI_BACKEND_URL = os.getenv("FASTAPI_BACKEND_URL", "http://localhost:8000").rstrip("/")
ANGULAR_FRONTEND_URL = os.getenv("ANGULAR_FRONTEND_URL", "http://localhost:4200").rstrip("/")

SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")
SF_REDIRECT_URI = f"{FASTAPI_BACKEND_URL}/api/crm/auth/salesforce/callback"

ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID", "").strip()
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET", "").strip()
ZOHO_REDIRECT_URI = f"{FASTAPI_BACKEND_URL}/api/crm/auth/zoho/callback"

ZD_CLIENT_ID = os.getenv("ZD_CLIENT_ID", "").strip()
ZD_CLIENT_SECRET = os.getenv("ZD_CLIENT_SECRET", "").strip()
ZD_REDIRECT_URI = f"{FASTAPI_BACKEND_URL}/api/crm/auth/zendesk/callback"

# ---- org identity captured at OAuth time (needs: alter table crm_connections add column org_id text;) ----
def _sf_org_id(token_data: dict):
    """Salesforce token `id` = https://<login>/id/<18-char org id>/<user id>"""
    parts = [p for p in (token_data.get("id") or "").rstrip("/").split("/") if p]
    return parts[-2] if len(parts) >= 2 else None


async def _zoho_org_id(api_domain: str, access_token: str):
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(f"{api_domain.rstrip('/')}/crm/v6/org", headers={"Authorization": f"Zoho-oauthtoken {access_token}"})
        if res.status_code == 200:
            orgs = res.json().get("org") or []
            return str(orgs[0].get("id")) if orgs and orgs[0].get("id") else None
    except Exception:
        pass
    return None  # without it Zoho runs stay owner-only (the API host is shared by all customers)


async def _hubspot_hub_id(access_token: str):
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(f"https://api.hubapi.com/oauth/v1/access-tokens/{access_token}")
        if res.status_code == 200 and res.json().get("hub_id"):
            return str(res.json()["hub_id"])
    except Exception:
        pass
    return None


def _insert_connection(row: dict) -> None:
    """Insert the connection; if the org_id column has not been added yet, save it without so login keeps working."""
    try:
        supabase.table("crm_connections").insert(row).execute()
    except Exception:
        if row.get("org_id") is None:
            raise
        supabase.table("crm_connections").insert({k: v for k, v in row.items() if k != "org_id"}).execute()


ZOHO_REGIONS = {
    "us": "https://accounts.zoho.com",
    "in": "https://accounts.zoho.in",
    "eu": "https://accounts.zoho.eu",
    "au": "https://accounts.zoho.com.au",
    "jp": "https://accounts.zoho.jp",
    "ca": "https://accounts.zohocloud.ca",
    "sa": "https://accounts.zoho.sa",
    "uk": "https://accounts.zoho.uk",
    "cn": "https://accounts.zoho.com.cn"
}

# =========================================================
# 1. SALESFORCE ROUTING
# =========================================================
@router.get("/auth/salesforce/login")
def get_salesforce_url(side: str, environment: str = "production", current_user = Depends(get_current_user)):
    domain = "test.salesforce.com" if environment.lower() == "sandbox" else "login.salesforce.com"
    
    code_verifier = secrets.token_urlsafe(64)
    sha256_hash = hashlib.sha256(code_verifier.encode('ascii')).digest()
    code_challenge = base64.urlsafe_b64encode(sha256_hash).decode('ascii').rstrip('=')
    
    raw_state = f"{side}::{current_user.id}::{environment}::{code_verifier}"
    encoded_state = base64.urlsafe_b64encode(raw_state.encode()).decode()
    
    params = {
        "response_type": "code",
        "client_id": SF_CLIENT_ID,
        "redirect_uri": SF_REDIRECT_URI,
        "scope": "api refresh_token offline_access",
        "state": encoded_state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "prompt": "login consent"
    }
    
    auth_url = f"https://{domain}/services/oauth2/authorize?{urllib.parse.urlencode(params)}"
    return {"url": auth_url}

@router.get("/auth/salesforce/callback")
async def salesforce_callback(code: str = None, state: str = None, error: str = None):
    if error or not code:
        return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

    try:
        decoded_bytes = base64.urlsafe_b64decode(state.encode())
        side, user_id, environment, code_verifier = decoded_bytes.decode().split("::")
    except Exception:
        return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

    environment = (environment or "production").lower()
    domain = "test.salesforce.com" if environment == "sandbox" else "login.salesforce.com"
    token_url = f"https://{domain}/services/oauth2/token"

    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": SF_CLIENT_ID,
        "client_secret": SF_CLIENT_SECRET,
        "redirect_uri": SF_REDIRECT_URI,
        "code_verifier": code_verifier
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(token_url, data=payload)
        if response.status_code != 200:
            return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")
            
        token_data = response.json()

    try:
        supabase.table("crm_connections").delete().eq("user_id", user_id).eq("connection_role", side).execute()
        
        _insert_connection({
            "org_id": _sf_org_id(token_data),
            "user_id": user_id,
            "crm_type": "salesforce",
            "connection_role": side,
            "access_token": token_data.get("access_token"),
            "refresh_token": token_data.get("refresh_token", ""),
            "instance_url": token_data.get("instance_url", ""),
            "environment": environment
        })
    except Exception:
        return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

    return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=success&side={side}&crm=salesforce")

# =========================================================
# 2. ZOHO ROUTING 
# =========================================================
@router.get("/auth/zoho/login")
def get_zoho_url(side: str, region: str = "IN", current_user = Depends(get_current_user)):
    custom_state = f"{side}::{current_user.id}::{region}"
    accounts_url = ZOHO_REGIONS.get(region.lower(), ZOHO_REGIONS["us"])

    scopes = [
        "ZohoCRM.modules.ALL",       # records + attachments
        "ZohoCRM.bulk.READ",
        "ZohoCRM.files.CREATE",
        "ZohoCRM.files.READ",
        "ZohoCRM.settings.ALL",      # fields/modules metadata + picklist value updates
        "ZohoCRM.coql.READ",
        "ZohoCRM.org.READ",          # org id, so two users of the same org can recover each other's runs
    ]

    params = {
        "scope": ",".join(scopes),
        "client_id": ZOHO_CLIENT_ID,
        "response_type": "code",
        "access_type": "offline",
        "redirect_uri": ZOHO_REDIRECT_URI,
        "prompt": "consent",
        "state": custom_state,
    }

    auth_url = f"{accounts_url}/oauth/v2/auth?{urllib.parse.urlencode(params)}"
    return {"url": auth_url}

@router.get("/auth/zoho/callback")
async def zoho_callback(code: str, state: str, request: Request):
    try:
        side, user_id, reg_key = state.split("::") 
    except ValueError:
        return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

    base_accounts_url = ZOHO_REGIONS.get(reg_key.lower(), ZOHO_REGIONS["us"])
    accounts_server = request.query_params.get("accounts-server", base_accounts_url)
    
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{accounts_server}/oauth/v2/token", data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "redirect_uri": ZOHO_REDIRECT_URI
        })
        if response.status_code != 200:
            return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")
            
        token_data = response.json()

    try:
        supabase.table("crm_connections").delete().eq("user_id", user_id).eq("connection_role", side).execute()
        
        zoho_api_domain = token_data.get("api_domain", "https://www.zohoapis.com")
        _insert_connection({
            "org_id": await _zoho_org_id(zoho_api_domain, token_data.get("access_token")),
            "user_id": user_id,
            "crm_type": "zoho",
            "connection_role": side,
            "access_token": token_data.get("access_token"),
            "refresh_token": token_data.get("refresh_token", ""),
            "api_domain": token_data.get("api_domain", "https://www.zohoapis.com"),
            "accounts_server": accounts_server,
            "region": reg_key
        })
    except Exception:
        return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

    return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=success&side={side}&crm=zoho")

# =========================================================
# 3. ZENDESK ROUTING
# =========================================================
@router.get("/auth/zendesk/login")
def get_zendesk_url(side: str, subdomain: str, current_user = Depends(get_current_user)):
    if not subdomain:
        raise HTTPException(status_code=400, detail="Subdomain parameter is missing.")
        
    custom_state = f"{side}::{current_user.id}::{subdomain}"
    
    params = {
        "response_type": "code",
        "client_id": ZD_CLIENT_ID,
        "redirect_uri": ZD_REDIRECT_URI,
        "scope": "read write",
        "state": custom_state
    }
    
    auth_url = f"https://{subdomain}.zendesk.com/oauth/authorizations/new?{urllib.parse.urlencode(params)}"
    
    return {"url": auth_url}

@router.get("/auth/zendesk/callback")
async def zendesk_callback(code: str, state: str):
    try:
        side, user_id, subdomain = state.split("::")
    except ValueError:
        return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

    async with httpx.AsyncClient() as client:
        response = await client.post(f"https://{subdomain}.zendesk.com/oauth/tokens", json={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": ZD_CLIENT_ID,
            "client_secret": ZD_CLIENT_SECRET,
            "redirect_uri": ZD_REDIRECT_URI,
            "scope": "read write"
        })
        if response.status_code != 200:
            return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")
            
        token_data = response.json()

    try:
        supabase.table("crm_connections").delete().eq("user_id", user_id).eq("connection_role", side).execute()
        
        supabase.table("crm_connections").insert({
            "user_id": user_id,
            "crm_type": "zendesk",
            "connection_role": side,
            "access_token": token_data.get("access_token"),
            "subdomain": subdomain
        }).execute()
    except Exception:
        return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

    return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=success&side={side}&crm=zendesk")
# =========================================================
# HUBSPOT OAUTH FLOW
# =========================================================

HUBSPOT_SCOPES = [
    "oauth",
    "crm.objects.contacts.read",
    "crm.objects.contacts.write",
    "crm.objects.companies.read",
    "crm.objects.companies.write",
    "crm.objects.deals.read",
    "crm.objects.deals.write",
    "tickets",
    "files.read",
    "files.write",
    "files.ui_hidden.read",
]

HUBSPOT_OPTIONAL_SCOPES = [
    "crm.schemas.contacts.write",
    "crm.schemas.companies.write",
    "crm.schemas.deals.write",
    "crm.schemas.custom.write",
]
@router.get("/auth/hubspot/login")
def get_hubspot_url(side: str, current_user = Depends(get_current_user)):
    import urllib.parse

    HS_CLIENT_ID = os.getenv("HS_CLIENT_ID", "").strip()
    HS_REDIRECT_URI = os.getenv("HS_REDIRECT_URI", f"{FASTAPI_BACKEND_URL}/api/crm/auth/hubspot/callback")

    params = {
        "client_id": HS_CLIENT_ID,
        "redirect_uri": HS_REDIRECT_URI,
        "scope": " ".join(HUBSPOT_SCOPES),
        "optional_scope": " ".join(HUBSPOT_OPTIONAL_SCOPES),
        "state": f"{side}::{current_user.id}",
    }
    auth_url = f"https://app.hubspot.com/oauth/authorize?{urllib.parse.urlencode(params)}"
    return {"url": auth_url}

@router.get("/auth/hubspot/callback")
async def hubspot_callback(code: str, state: str):
    try:
        side, user_id = state.split("::")
    except ValueError:
        return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

    HS_CLIENT_ID = os.getenv("HS_CLIENT_ID", "").strip()
    HS_CLIENT_SECRET = os.getenv("HS_CLIENT_SECRET", "").strip()
    HS_REDIRECT_URI = os.getenv("HS_REDIRECT_URI", f"{FASTAPI_BACKEND_URL}/api/crm/auth/hubspot/callback")

    async with httpx.AsyncClient() as client:
        response = await client.post("https://api.hubapi.com/oauth/v1/token", data={
            "grant_type": "authorization_code",
            "client_id": HS_CLIENT_ID,
            "client_secret": HS_CLIENT_SECRET,
            "redirect_uri": HS_REDIRECT_URI,
            "code": code
        })
        if response.status_code != 200:
            errorBody = response.text
            print(f"HubSpot token error ({response.status_code}): {errorBody}")
            return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

        token_data = response.json()

    try:
        supabase.table("crm_connections").delete().eq("user_id", user_id).eq("connection_role", side).execute()
        
        _insert_connection({
            "org_id": await _hubspot_hub_id(token_data.get("access_token")),
            "user_id": user_id,
            "crm_type": "hubspot",
            "connection_role": side,
            "access_token": token_data.get("access_token"),
            "refresh_token": token_data.get("refresh_token", ""),
            "api_domain": "https://api.hubapi.com" 
        })
    except Exception as e:
        print(f"Hubspot DB Insert Error: {e}")
        return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=error")

    return RedirectResponse(url=f"{ANGULAR_FRONTEND_URL}/connection?status=success&side={side}&crm=hubspot")

# =========================================================
# CORE CONNECTIONS MANAGEMENT
# =========================================================
@router.get("/connections")
async def get_connections(current_user = Depends(get_current_user)):
    return await CrmService.get_connections_with_status(current_user.id)

@router.delete("/connections/{side}")
def disconnect_crm(side: str, current_user = Depends(get_current_user)):
    if side not in ("source", "target"):
        raise HTTPException(status_code=400, detail="side must be 'source' or 'target'.")
    CrmService.delete_connection(current_user.id, side)
    return {"success": True}