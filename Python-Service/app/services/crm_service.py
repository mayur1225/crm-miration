import asyncio
import os
from typing import Optional

import httpx
from fastapi import HTTPException

from app.utils.config import supabase

CRM_SESSION_EXPIRED = "CRM_SESSION_EXPIRED"
SENSITIVE_CONNECTION_FIELDS = {"access_token", "refresh_token"}
PROBE_TIMEOUT = 10.0


def crm_session_expired(crm_type: str, role: str, message: Optional[str] = None) -> HTTPException:
    """
    One consistent, machine-readable error for "this CRM token is dead, user must log in again".
    The frontend (crm-session.util.ts) keys off detail.code, and detail.side tells it which
    connection (source/target) to clear.
    """
    return HTTPException(
        status_code=401,
        detail={
            "code": CRM_SESSION_EXPIRED,
            "crm": crm_type,
            "side": role,
            "message": message or f"Your {crm_type} session has expired. Please log in again.",
        },
    )


def is_crm_session_expired(exc: HTTPException) -> bool:
    detail = getattr(exc, "detail", None)
    return isinstance(detail, dict) and detail.get("code") == CRM_SESSION_EXPIRED


class CrmService:
    # ------------------------------------------------------------------
    # Raw DB access
    # ------------------------------------------------------------------
    @staticmethod
    def get_user_connections(user_id: str):
        try:
            response = supabase.table("crm_connections").select("*").eq("user_id", user_id).execute()
            return response.data
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @staticmethod
    def delete_connection(user_id: str, side: str):
        try:
            response = (
                supabase.table("crm_connections").delete().eq("user_id", user_id).eq("connection_role", side).execute()
            )
            return response.data
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    @staticmethod
    def get_active_crm_credentials(user_id: str, crm_type: str, role: str):
        """Fetches the secure CRM credentials for a user directly from the database."""
        response = (
            supabase.table("crm_connections")
            .select("*")
            .eq("user_id", user_id)
            .eq("crm_type", crm_type.lower())
            .eq("connection_role", role.lower())
            .execute()
        )

        if not response.data:
            raise HTTPException(status_code=404, detail=f"No active database connection found for {crm_type} ({role}).")

        return response.data[0]

    # ------------------------------------------------------------------
    # Token health
    # ------------------------------------------------------------------
    @staticmethod
    async def _token_is_valid(client: httpx.AsyncClient, conn: dict) -> Optional[bool]:
        """
        Cheap authenticated call against the CRM.
          True  -> token works
          False -> CRM answered 401 (expired / revoked)
          None  -> could not tell (network error, unknown CRM) -- never treat as expired
        """
        crm = (conn.get("crm_type") or "").lower()
        token = conn.get("access_token")
        if not token:
            return False

        try:
            if crm == "salesforce":
                base = (conn.get("instance_url") or "").rstrip("/")
                if not base:
                    return False
                res = await client.get(f"{base}/services/data/v60.0/limits", headers={"Authorization": f"Bearer {token}"})
            elif crm == "zoho":
                base = (conn.get("api_domain") or "https://www.zohoapis.com").rstrip("/")
                res = await client.get(
                    f"{base}/crm/v2/settings/modules", headers={"Authorization": f"Zoho-oauthtoken {token}"}
                )
            elif crm == "zendesk":
                sub = conn.get("subdomain")
                if not sub:
                    return False
                res = await client.get(
                    f"https://{sub}.zendesk.com/api/v2/users/me.json", headers={"Authorization": f"Bearer {token}"}
                )
            elif crm == "hubspot":
                res = await client.get(
                    "https://api.hubapi.com/crm/v3/objects/contacts",
                    params={"limit": 1},
                    headers={"Authorization": f"Bearer {token}"},
                )
            else:
                return None
        except httpx.HTTPError:
            return None

        return False if res.status_code == 401 else True

    @staticmethod
    async def _check_row(client: httpx.AsyncClient, row: dict) -> dict:
        """Probe one saved connection; try a silent refresh before declaring it expired."""
        expired = False
        if await CrmService._token_is_valid(client, row) is False:
            try:
                await CrmService.refresh_crm_token(row["user_id"], row["crm_type"], row["connection_role"])
            except HTTPException as e:
                # 401 = refresh token dead too. Anything else (503 etc.) is a hiccup, not an expiry.
                expired = e.status_code == 401

        # NEVER send OAuth tokens to the browser.
        safe = {k: v for k, v in row.items() if k not in SENSITIVE_CONNECTION_FIELDS}
        safe["token_expired"] = expired
        safe["status"] = "expired" if expired else "connected"
        return safe

    @staticmethod
    async def get_connections_with_status(user_id: str):
        rows = CrmService.get_user_connections(user_id)
        if not rows:
            return []
        async with httpx.AsyncClient(timeout=PROBE_TIMEOUT) as client:
            return list(await asyncio.gather(*(CrmService._check_row(client, r) for r in rows)))

    @staticmethod
    async def ensure_valid_credentials(user_id: str, crm_type: str, role: str):
        """
        Credentials guaranteed to have a live access token (refreshed silently if needed).
        Raises the coded CRM_SESSION_EXPIRED error when the user has to log in again.
        """
        try:
            creds = CrmService.get_active_crm_credentials(user_id, crm_type, role)
        except HTTPException as e:
            if e.status_code == 404:
                raise crm_session_expired(crm_type, role, f"{crm_type} ({role}) is not connected. Please log in again.")
            raise

        async with httpx.AsyncClient(timeout=PROBE_TIMEOUT) as client:
            valid = await CrmService._token_is_valid(client, creds)

        if valid is False:
            try:
                creds["access_token"] = await CrmService.refresh_crm_token(user_id, crm_type, role)
            except HTTPException as e:
                if e.status_code == 401:
                    raise crm_session_expired(crm_type, role)
                raise
        return creds

    @staticmethod
    async def refresh_crm_token(user_id: str, crm_type: str, role: str):
        """Silently exchanges an expired access_token for a new one using the refresh_token."""
        creds = CrmService.get_active_crm_credentials(user_id, crm_type, role)
        refresh_token = creds.get("refresh_token")

        if not refresh_token:
            raise HTTPException(status_code=401, detail="No refresh token available. Reconnect UI.")

        crm = crm_type.lower()
        res = None

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                if crm == "salesforce":
                    domain = "test.salesforce.com" if (creds.get("environment") or "").lower() == "sandbox" else "login.salesforce.com"
                    res = await client.post(
                        f"https://{domain}/services/oauth2/token",
                        data={
                            "grant_type": "refresh_token",
                            "client_id": os.getenv("SF_CLIENT_ID"),
                            "client_secret": os.getenv("SF_CLIENT_SECRET"),
                            "refresh_token": refresh_token,
                        },
                    )
                elif crm == "zoho":
                    accounts_server = creds.get("accounts_server") or "https://accounts.zoho.com"
                    res = await client.post(
                        f"{accounts_server}/oauth/v2/token",
                        params={
                            "grant_type": "refresh_token",
                            "client_id": os.getenv("ZOHO_CLIENT_ID"),
                            "client_secret": os.getenv("ZOHO_CLIENT_SECRET"),
                            "refresh_token": refresh_token,
                        },
                    )
                elif crm == "zendesk":
                    res = await client.post(
                        f"https://{creds.get('subdomain')}.zendesk.com/oauth/tokens",
                        json={
                            "grant_type": "refresh_token",
                            "client_id": os.getenv("ZD_CLIENT_ID"),
                            "client_secret": os.getenv("ZD_CLIENT_SECRET"),
                            "refresh_token": refresh_token,
                            "scope": "read write",
                        },
                    )
                elif crm == "hubspot":
                    res = await client.post(
                        "https://api.hubapi.com/oauth/v1/token",
                        data={
                            "grant_type": "refresh_token",
                            "client_id": os.getenv("HS_CLIENT_ID"),
                            "client_secret": os.getenv("HS_CLIENT_SECRET"),
                            "refresh_token": refresh_token,
                        },
                    )
        except httpx.HTTPError as e:
            # Network trouble is NOT the same as a revoked token.
            raise HTTPException(status_code=503, detail=f"Could not reach {crm_type} to refresh the session: {e}")

        if res is not None and res.status_code == 200:
            data = res.json()
            new_access = data.get("access_token")
            if new_access:
                update = {"access_token": new_access}
                # HubSpot / Zendesk may rotate the refresh token -- keep it or the next refresh fails.
                if data.get("refresh_token"):
                    update["refresh_token"] = data["refresh_token"]
                supabase.table("crm_connections").update(update).eq("id", creds["id"]).execute()
                return new_access

        raise HTTPException(status_code=401, detail="Refresh token expired. Reconnect UI.")