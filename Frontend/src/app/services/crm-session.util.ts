// Put this file at: src/app/services/crm-session.util.ts
import { CrmConnection } from './CrmAuthService.service';

export type CrmSide = 'source' | 'target';

/** Code the backend puts in detail.code when a CRM token is dead (see crm_service.crm_session_expired). */
export const CRM_SESSION_EXPIRED_CODE = 'CRM_SESSION_EXPIRED';

const EXPIRED_STATUSES = [
  'expired', 'token_expired', 'revoked', 'invalid', 'reauth_required', 'needs_reauth', 'unauthorized'
];

/**
 * True when a saved connection row must NOT be shown as "Connected".
 * GET /api/crm/connections now returns `token_expired` + `status` ("connected" | "expired") per row.
 */
export function isConnectionExpired(conn: CrmConnection | any): boolean {
  if (!conn) return true;
  if (conn.token_expired === true || conn.is_expired === true || conn.expired === true || conn.needs_reauth === true) {
    return true;
  }
  const status = String(conn.status ?? conn.token_status ?? '').toLowerCase();
  return EXPIRED_STATUSES.includes(status);
}

const AUTH_ERROR_PATTERN =
  /invalid_session_id|session (has )?expired|token (has )?(expired|been revoked)|token used to make this call expired|expired access\/refresh token|invalid_grant|invalid_token|expired_authentication|re-?authenticat|refresh token expired/i;

const AUTH_ERROR_CODES = [
  CRM_SESSION_EXPIRED_CODE, 'SESSION_EXPIRED', 'TOKEN_EXPIRED', 'INVALID_SESSION_ID', 'REAUTH_REQUIRED', 'INVALID_GRANT'
];

/** FastAPI sends `{detail: "text"}` or `{detail: {code, crm, side, message}}`. Handle both. */
function detailObject(err: any): any {
  const d = err?.error?.detail;
  return d && typeof d === 'object' ? d : null;
}

function errorCode(err: any): string {
  const d = detailObject(err);
  return String(d?.code ?? err?.error?.code ?? err?.error?.errorCode ?? err?.code ?? '').toUpperCase();
}

function errorText(err: any): string {
  const d = detailObject(err);
  return [
    typeof err?.error?.detail === 'string' ? err.error.detail : '',
    d?.message,
    err?.error?.message,
    typeof err?.error === 'string' ? err.error : '',
    err?.message
  ]
    .filter((s) => typeof s === 'string')
    .join(' ');
}

/**
 * True when an HttpErrorResponse / thrown error means the CRM token is expired or revoked.
 *
 * `strict` = ignore a bare HTTP 401. Use it for raw fetch() calls, which bypass the auth interceptor,
 * so a 401 there may just as well be the *app* (Supabase) token -- only trust the code / message.
 */
export function isAuthExpiredError(err: any, strict = false): boolean {
  if (!err) return false;
  if (AUTH_ERROR_CODES.includes(errorCode(err))) return true;
  if (AUTH_ERROR_PATTERN.test(errorText(err))) return true;
  return !strict && err.status === 401;
}

/** Which connection the backend says is dead (detail.side). Empty array = unknown -> caller clears both. */
export function getExpiredSidesFromError(err: any): CrmSide[] {
  const side = detailObject(err)?.side;
  return side === 'source' || side === 'target' ? [side] : [];
}

/** True when a WebSocket message from the backend reports an expired CRM session. */
export function isSocketAuthExpired(data: any): boolean {
  if (!data) return false;
  if (data.authExpired === true || data.sessionExpired === true) return true;
  const status = String(data.status ?? '').toLowerCase();
  if (status === 'sessionexpired' || status === 'authexpired') return true;
  return isAuthExpiredError({ error: { code: data.errorCode, detail: data.detail || data.error } }, true);
}

export function getSocketExpiredSides(data: any): CrmSide[] {
  const sides = Array.isArray(data?.expiredSides) ? data.expiredSides : [];
  return sides.filter((s: any) => s === 'source' || s === 'target');
}