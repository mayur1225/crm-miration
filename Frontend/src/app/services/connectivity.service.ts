import { Injectable, computed, inject, signal } from '@angular/core';
import { Subject } from 'rxjs';
import { ActiveToast, ToastrService } from 'ngx-toastr';
import { environment } from 'src/environments/environment';

export type ConnectivityStatus = 'online' | 'offline' | 'server-unavailable';

export const TRANSIENT_HTTP_STATUSES: ReadonlySet<number> = new Set([0, 502, 503, 504]);

const PROBE_URL = `${environment.apiUrl}/health`;
const PROBE_TIMEOUT_MS = 5_000;
const BACKOFF_BASE_MS = 2_000;
const BACKOFF_MAX_MS = 30_000;
const OVERLAY_GRACE_MS = 5_000;

@Injectable({ providedIn: 'root' })
export class ConnectivityService {
  private readonly toastr = inject(ToastrService);

  private readonly _status = signal<ConnectivityStatus>('online');
  private readonly _attempts = signal(0);
  private readonly _isChecking = signal(false);
  private readonly _overlayVisible = signal(false);
  private readonly _outageStartedAt = signal<number | null>(null);
  private readonly _nextProbeAt = signal<number | null>(null);
  private readonly _nowMs = signal(Date.now());

  readonly status = this._status.asReadonly();
  readonly attempts = this._attempts.asReadonly();
  readonly isChecking = this._isChecking.asReadonly();
  readonly overlayVisible = this._overlayVisible.asReadonly();
  readonly isDegraded = computed(() => this._status() !== 'online');
  readonly elapsedSeconds = computed(() => {
    const started = this._outageStartedAt();
    return started === null ? 0 : Math.max(0, Math.floor((this._nowMs() - started) / 1000));
  });
  readonly retryInSeconds = computed(() => {
    const next = this._nextProbeAt();
    return next === null ? 0 : Math.max(0, Math.ceil((next - this._nowMs()) / 1000));
  });

  readonly reconnected$ = new Subject<void>();

  private probeTimer?: ReturnType<typeof setTimeout>;
  private overlayTimer?: ReturnType<typeof setTimeout>;
  private tickTimer?: ReturnType<typeof setInterval>;
  private verification?: Promise<boolean>;
  private overlayDismissed = false;
  private activeToast?: ActiveToast<unknown>;

  constructor() {
    window.addEventListener('online', () => {
      if (this.isDegraded()) void this.runProbe();
    });
    window.addEventListener('offline', () => void this.reportFailure());
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible' && this.isDegraded()) void this.runProbe();
    });

    if (!navigator.onLine) this.enterDegraded();
  }


  reportFailure(): Promise<boolean> {
    if (this.isDegraded()) return Promise.resolve(true);
    if (!navigator.onLine) {
      this.enterDegraded();
      return Promise.resolve(true);
    }
    this.verification ??= this.probe().then((ok) => {
      this.verification = undefined;
      if (!ok) this.enterDegraded();
      return !ok;
    });
    return this.verification;
  }

  reportSuccess(): void {
    if (this.isDegraded()) this.recover();
  }

  async guardedFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    try {
      const response = await fetch(input, init);
      if (TRANSIENT_HTTP_STATUSES.has(response.status)) {
        void this.reportFailure();
      } else {
        this.reportSuccess();
      }
      return response;
    } catch (error) {
      const aborted = error instanceof DOMException && error.name === 'AbortError';
      if (!aborted) void this.reportFailure();
      throw error;
    }
  }

  retryNow(): void {
    void this.runProbe();
  }

  dismissOverlay(): void {
    this.overlayDismissed = true;
    this._overlayVisible.set(false);
  }


  private enterDegraded(): void {
    if (this.isDegraded()) return;

    this._status.set(this.currentFailureStatus());
    this._outageStartedAt.set(Date.now());
    this._attempts.set(0);
    this.overlayDismissed = false;

    this._nowMs.set(Date.now());
    this.tickTimer = setInterval(() => this._nowMs.set(Date.now()), 1_000);
    this.overlayTimer = setTimeout(() => {
      if (this.isDegraded() && !this.overlayDismissed) this._overlayVisible.set(true);
    }, OVERLAY_GRACE_MS);

    this.showToast();
    this.scheduleProbe();
  }

  private recover(): void {
    clearTimeout(this.probeTimer);
    clearTimeout(this.overlayTimer);
    clearInterval(this.tickTimer);
    this.dismissToast();

    this._status.set('online');
    this._overlayVisible.set(false);
    this._outageStartedAt.set(null);
    this._nextProbeAt.set(null);
    this._attempts.set(0);
    this.overlayDismissed = false;

    this.toastr.success('Connection restored. You can continue working.', 'Back online', { timeOut: 4_000 });
    this.reconnected$.next();
  }

  private async runProbe(): Promise<void> {
    if (!this.isDegraded() || this._isChecking()) return;

    clearTimeout(this.probeTimer);
    this._nextProbeAt.set(null);
    this._isChecking.set(true);

    const ok = navigator.onLine && (await this.probe());

    this._isChecking.set(false);
    if (ok) {
      this.recover();
      return;
    }

    this._attempts.update((n) => n + 1);
    const next = this.currentFailureStatus();
    if (next !== this._status()) {
      this._status.set(next);
      this.showToast();
    }
    this.scheduleProbe();
  }

  private scheduleProbe(): void {
    clearTimeout(this.probeTimer);
    const exponential = Math.min(BACKOFF_BASE_MS * 2 ** this._attempts(), BACKOFF_MAX_MS);
    const delay = Math.round(exponential * (0.9 + Math.random() * 0.2)); // ±10% jitter: avoid thundering herd on recovery
    this._nextProbeAt.set(Date.now() + delay);
    this.probeTimer = setTimeout(() => void this.runProbe(), delay);
  }

  private async probe(): Promise<boolean> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS);
    try {
      const response = await fetch(PROBE_URL, { cache: 'no-store', signal: controller.signal });
      return response.ok;
    } catch {
      return false;
    } finally {
      clearTimeout(timeout);
    }
  }

  private currentFailureStatus(): ConnectivityStatus {
    return navigator.onLine ? 'server-unavailable' : 'offline';
  }

  private showToast(): void {
    this.dismissToast();
    const offline = this._status() === 'offline';
    this.activeToast = this.toastr.error(
      offline
        ? 'Check your internet connection. Reconnecting automatically…'
        : 'Cannot reach the migration service. Reconnecting automatically…',
      offline ? "You're offline" : 'Service unavailable',
      { disableTimeOut: true, tapToDismiss: false, closeButton: false }
    );
  }

  private dismissToast(): void {
    if (this.activeToast) {
      this.toastr.clear(this.activeToast.toastId);
      this.activeToast = undefined;
    }
  }
}