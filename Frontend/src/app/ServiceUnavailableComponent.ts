import { ChangeDetectionStrategy, Component, ElementRef, HostListener, computed, effect, inject, viewChild } from '@angular/core';
import { ConnectivityService } from './services/connectivity.service';

@Component({
  selector: 'app-service-unavailable',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    @if (connectivity.overlayVisible()) {
      <div class="su-backdrop" role="alertdialog" aria-modal="true" aria-labelledby="su-title" aria-describedby="su-desc">
        <section class="su-card" [class.su-card--offline]="isOffline()">
          <div class="su-visual" aria-hidden="true">
            <span class="su-pulse"></span>
            <span class="su-pulse su-pulse--delayed"></span>
            @if (isOffline()) {
              <svg class="su-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">
                <path d="M2 8.8a15 15 0 0 1 4.2-2.6M22 8.8a15 15 0 0 0-9.4-4.7M5 12.9a10 10 0 0 1 3.5-2.1M19 12.9a10 10 0 0 0-4.3-2.6M8.5 16.4a5 5 0 0 1 7 0" />
                <circle cx="12" cy="20" r="0.9" fill="currentColor" />
                <path d="M3 3l18 18" />
              </svg>
            } @else {
              <svg class="su-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">
                <rect x="3" y="3.5" width="18" height="7" rx="2" />
                <rect x="3" y="13.5" width="18" height="7" rx="2" />
                <circle cx="7" cy="7" r="0.9" fill="currentColor" />
                <circle cx="7" cy="17" r="0.9" fill="currentColor" />
                <path d="M3 3l18 18" />
              </svg>
            }
          </div>

          <h2 id="su-title" class="su-title">{{ title() }}</h2>
          <p id="su-desc" class="su-lead">{{ description() }}</p>

          <ul class="su-checks" aria-label="Connection diagnostics">
            <li>
              <span class="su-dot" [class.su-dot--ok]="!isOffline()" [class.su-dot--bad]="isOffline()"></span>
              <span>Your internet connection</span>
              <strong>{{ isOffline() ? 'Offline' : 'Connected' }}</strong>
            </li>
            <li>
              <span class="su-dot" [class.su-dot--bad]="!isOffline()"></span>
              <span>Migration service</span>
              <strong>{{ isOffline() ? 'Unknown' : 'Unreachable' }}</strong>
            </li>
          </ul>

          <p class="su-status" aria-live="polite">
            @if (connectivity.isChecking()) {
              <span class="su-spinner"></span> Checking connection…
            } @else {
              Retrying in {{ connectivity.retryInSeconds() }}s
            }
            <span class="su-sep">·</span> Failed checks: {{ connectivity.attempts() }}
            <span class="su-sep">·</span> Down for {{ elapsedLabel() }}
          </p>

          <div class="su-actions">
            <button #retryBtn type="button" class="su-btn su-btn--primary" [disabled]="connectivity.isChecking()" (click)="connectivity.retryNow()">
              Retry now
            </button>
            <button type="button" class="su-btn" (click)="connectivity.dismissOverlay()">Dismiss</button>
          </div>

          <p class="su-note">
            Your work on this page is preserved. A migration interrupted by the outage can be resumed once the service is back.
          </p>
        </section>
      </div>
    }
  `,
  styles: [
    `
      :host {
        --su-bg: #ffffff;
        --su-fg: #1c2430;
        --su-muted: #5f6b7a;
        --su-border: #e3e8ef;
        --su-accent: #dc3545;
        --su-ok: #198754;
        --su-primary: #0d6efd;
        --su-primary-fg: #ffffff;
        --su-surface: #f6f8fb;
      }
      @media (prefers-color-scheme: dark) {
        :host {
          --su-bg: #1b2230;
          --su-fg: #eef2f7;
          --su-muted: #9aa7b8;
          --su-border: #2c3648;
          --su-surface: #222b3b;
        }
      }
      .su-backdrop {
        position: fixed;
        inset: 0;
        z-index: 2000;
        display: grid;
        place-items: center;
        padding: 1rem;
        background: rgba(15, 20, 30, 0.55);
        backdrop-filter: blur(6px);
        animation: su-fade 0.2s ease-out;
      }
      .su-card {
        width: min(100%, 460px);
        padding: 2rem 1.75rem 1.5rem;
        text-align: center;
        color: var(--su-fg);
        background: var(--su-bg);
        border: 1px solid var(--su-border);
        border-radius: 1.25rem;
        box-shadow: 0 24px 64px rgba(0, 0, 0, 0.28);
        animation: su-rise 0.25s ease-out;
      }
      .su-card--offline {
        --su-accent: #f59f00;
      }
      .su-visual {
        position: relative;
        width: 88px;
        height: 88px;
        margin: 0 auto 1.25rem;
        display: grid;
        place-items: center;
        color: var(--su-accent);
      }
      .su-icon {
        position: relative;
        width: 44px;
        height: 44px;
      }
      .su-pulse {
        position: absolute;
        inset: 0;
        border-radius: 50%;
        background: var(--su-accent);
        opacity: 0.18;
        animation: su-pulse 2.4s ease-out infinite;
      }
      .su-pulse--delayed {
        animation-delay: 1.2s;
      }
      .su-title {
        margin: 0 0 0.5rem;
        font-size: 1.4rem;
        font-weight: 700;
      }
      .su-lead {
        margin: 0 0 1.25rem;
        color: var(--su-muted);
        line-height: 1.55;
      }
      .su-checks {
        margin: 0 0 1.1rem;
        padding: 0.35rem 0.9rem;
        list-style: none;
        text-align: left;
        background: var(--su-surface);
        border: 1px solid var(--su-border);
        border-radius: 0.75rem;
      }
      .su-checks li {
        display: flex;
        align-items: center;
        gap: 0.6rem;
        padding: 0.55rem 0;
        font-size: 0.9rem;
      }
      .su-checks li + li {
        border-top: 1px solid var(--su-border);
      }
      .su-checks span:nth-child(2) {
        flex: 1;
      }
      .su-dot {
        width: 9px;
        height: 9px;
        border-radius: 50%;
        background: var(--su-muted);
      }
      .su-dot--ok {
        background: var(--su-ok);
      }
      .su-dot--bad {
        background: var(--su-accent);
      }
      .su-status {
        margin: 0 0 1.1rem;
        font-size: 0.85rem;
        color: var(--su-muted);
        font-variant-numeric: tabular-nums;
      }
      .su-sep {
        margin: 0 0.35rem;
        opacity: 0.6;
      }
      .su-spinner {
        display: inline-block;
        width: 0.8rem;
        height: 0.8rem;
        margin-right: 0.35rem;
        vertical-align: -1px;
        border: 2px solid var(--su-border);
        border-top-color: var(--su-primary);
        border-radius: 50%;
        animation: su-spin 0.8s linear infinite;
      }
      .su-actions {
        display: flex;
        gap: 0.6rem;
        justify-content: center;
        margin-bottom: 1rem;
      }
      .su-btn {
        padding: 0.55rem 1.2rem;
        font: inherit;
        font-weight: 600;
        color: var(--su-fg);
        background: transparent;
        border: 1px solid var(--su-border);
        border-radius: 0.6rem;
        cursor: pointer;
        transition: background 0.15s, opacity 0.15s;
      }
      .su-btn:hover:not(:disabled) {
        background: var(--su-surface);
      }
      .su-btn:disabled {
        opacity: 0.6;
        cursor: progress;
      }
      .su-btn:focus-visible {
        outline: 2px solid var(--su-primary);
        outline-offset: 2px;
      }
      .su-btn--primary {
        color: var(--su-primary-fg);
        background: var(--su-primary);
        border-color: var(--su-primary);
      }
      .su-btn--primary:hover:not(:disabled) {
        background: var(--su-primary);
        opacity: 0.9;
      }
      .su-note {
        margin: 0;
        font-size: 0.78rem;
        line-height: 1.5;
        color: var(--su-muted);
      }
      @keyframes su-fade {
        from { opacity: 0; }
      }
      @keyframes su-rise {
        from { opacity: 0; transform: translateY(12px) scale(0.98); }
      }
      @keyframes su-pulse {
        0% { transform: scale(0.55); opacity: 0.3; }
        100% { transform: scale(1.25); opacity: 0; }
      }
      @keyframes su-spin {
        to { transform: rotate(360deg); }
      }
      @media (prefers-reduced-motion: reduce) {
        .su-backdrop, .su-card, .su-pulse, .su-spinner { animation: none; }
      }
    `
  ]
})
export class ServiceUnavailableComponent {
  protected readonly connectivity = inject(ConnectivityService);

  private readonly retryBtn = viewChild<ElementRef<HTMLButtonElement>>('retryBtn');

  protected readonly isOffline = computed(() => this.connectivity.status() === 'offline');
  protected readonly title = computed(() => (this.isOffline() ? "You're offline" : 'Service temporarily unavailable'));
  protected readonly description = computed(() =>
    this.isOffline()
      ? "Your device has lost its internet connection. We'll reconnect automatically as soon as it's back."
      : "We can't reach the migration service right now. It may be restarting or undergoing maintenance. We'll keep trying automatically."
  );
  protected readonly elapsedLabel = computed(() => {
    const total = this.connectivity.elapsedSeconds();
    const minutes = Math.floor(total / 60);
    const seconds = total % 60;
    return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  });

  constructor() {
    // Move focus into the dialog when it appears.
    effect(() => this.retryBtn()?.nativeElement.focus());
  }

  @HostListener('document:keydown.escape')
  protected onEscape(): void {
    if (this.connectivity.overlayVisible()) this.connectivity.dismissOverlay();
  }
}