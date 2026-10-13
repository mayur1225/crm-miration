// Put this file at: src/app/services/crm-session.util.ts
import { CrmConnection } from './CrmAuthService.service';

const EXPIRED_STATUSES = [
  'expired', 'token_expired', 'revoked', 'invalid', 'reauth_required', 'needs_reauth', 'unauthorized'
];

/**
 * True when a saved connection row must NOT be shown as "Connected".
 * ADJUST the field names below to whatever your backend returns for an expired token.
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
  /invalid_session_id|session (has )?expired|token (has )?(expired|been revoked)|expired access\/refresh token|invalid_grant|invalid_token|re-?authenticat/i;

const AUTH_ERROR_CODES = ['SESSION_EXPIRED', 'TOKEN_EXPIRED', 'INVALID_SESSION_ID', 'REAUTH_REQUIRED', 'INVALID_GRANT'];

/** True when an HttpErrorResponse / thrown error means the CRM token is expired or revoked. */
export function isAuthExpiredError(err: any): boolean {
  if (!err) return false;
  if (err.status === 401) return true;

  const code = String(err?.error?.code ?? err?.error?.errorCode ?? err?.code ?? '').toUpperCase();
  if (AUTH_ERROR_CODES.includes(code)) return true;

  const text = [err?.error?.detail, err?.error?.message, typeof err?.error === 'string' ? err.error : '', err?.message]
    .filter((s) => typeof s === 'string')
    .join(' ');
  return AUTH_ERROR_PATTERN.test(text);
}

/** True when a WebSocket message from the backend reports an expired CRM session. */
export function isSocketAuthExpired(data: any): boolean {
  if (!data) return false;
  if (data.authExpired === true || data.sessionExpired === true) return true;
  const status = String(data.status ?? '').toLowerCase();
  if (status === 'sessionexpired' || status === 'authexpired') return true;
  return isAuthExpiredError({ error: { code: data.errorCode, detail: data.detail || data.error } });
}