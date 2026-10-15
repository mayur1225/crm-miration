import { CommonModule } from '@angular/common';
import { Component, OnInit, inject, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, ActivatedRoute } from '@angular/router';
import { CrmAuthService, CrmConnection } from 'src/app/services/CrmAuthService.service';
import { ToastrService } from 'ngx-toastr';
import { Subscription, Observable, switchMap, delay, forkJoin, of, catchError } from 'rxjs';
import { isConnectionExpired } from 'src/app/services/crm-session.util';

@Component({
  selector: 'app-connection',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './connection.component.html',
  styleUrls: ['./connection.component.scss']
})
export class ConnectionComponent implements OnInit, OnDestroy {
  private router = inject(Router);
  private route = inject(ActivatedRoute);
  private crmAuthService = inject(CrmAuthService);
  private toastr = inject(ToastrService);
  private cdr = inject(ChangeDetectorRef);

  private authSubscription!: Subscription;

  availableCRMs = [
    { id: 'zendesk', name: 'Zendesk', icon: 'icon-headphones' },
    { id: 'salesforce', name: 'Salesforce', icon: 'icon-cloud' },
    { id: 'hubspot', name: 'HubSpot', icon: 'icon-share-2' },
   // { id: 'msdynamics', name: 'MS Dynamics 365', icon: 'icon-cpu' },
    { id: 'zoho', name: 'Zoho CRM', icon: 'icon-layout' }
  ];

  selectedSource: string = '';
  selectedTarget: string = '';

  isSourceConnected: boolean = false;
  isTargetConnected: boolean = false;

  sourceInstanceUrl: string = '';
  targetInstanceUrl: string = '';

  // Source isolated states
  sourceZendeskSubdomain: string = '';
  sourceZohoRegion: string = 'IN';
  sourceSalesforceEnv: string = 'production';

  // Target isolated states
  targetZendeskSubdomain: string = '';
  targetZohoRegion: string = 'IN';
  targetSalesforceEnv: string = 'production';

  showPathSelection: boolean = false;
  isPageLoading: boolean = true;
  isSourceConnecting: boolean = false;
  isTargetConnecting: boolean = false;

  // True when this side's saved login is dead: shown as "session expired" + login button, never "Connected".
  sourceSessionExpired: boolean = false;
  targetSessionExpired: boolean = false;

  private expiredToastShown = false;
  // CRM the user had on a side when the api-mapping page bounced them here (so we can offer "Login" again).
  private forcedExpired: Partial<Record<'source' | 'target', string>> = {};

  ngOnInit() {
    this.isPageLoading = true;

    // Sides the API-mapping page told us have an expired session (consumed once, so a refresh won't repeat it).
    const expiredSides: Array<'source' | 'target'> = [...(history.state?.expiredSides || [])];
    if (expiredSides.length) {
      expiredSides.forEach((side) => {
        const crm = localStorage.getItem(`${side}_crm_slot`);
        if (crm && crm !== 'csv') this.forcedExpired[side] = crm;
      });
      history.replaceState({ ...history.state, expiredSides: undefined }, '');
      this.showExpiredToast();
    }
    let alreadyCleared = false; // only wipe the expired sides on the first emission, not on every query-param change

    this.authSubscription = this.route.queryParams.pipe(
      switchMap(params => {
        const status = params['status'];
        const crm = params['crm'];   

        if (status === 'success') {
          this.toastr.success(`${crm ? crm.toUpperCase() : 'CRM'} Connected Successfully!`);
          this.router.navigate([], { relativeTo: this.route, replaceUrl: true });
        } else if (status === 'error') {
          this.toastr.error('Failed to connect to CRM. Please try again.');
          this.router.navigate([], { relativeTo: this.route, replaceUrl: true });
        }

        // Drop the expired connection(s) first so the page starts clean, then load what is still valid.
        const clear$: Observable<unknown> = !alreadyCleared && expiredSides.length
          ? forkJoin(expiredSides.map((side) => this.crmAuthService.disconnectCrm(side).pipe(catchError(() => of(null)))))
          : of(null);
        alreadyCleared = true;

        return clear$.pipe(switchMap(() => this.crmAuthService.getUserConnections()));
      }),
      delay(0)
    ).subscribe({
      next: (connections: CrmConnection[]) => {
        this.parseConnections(connections);
        this.isPageLoading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        console.error('Failed to load CRM connections', err);
        this.toastr.error('Could not load your saved connections.');
        this.isPageLoading = false;
        this.cdr.detectChanges();
      }
    });
  }

  loadActiveConnections() {
    this.isPageLoading = true;
    this.crmAuthService.getUserConnections().pipe(
      delay(0)
    ).subscribe({
      next: (connections: CrmConnection[]) => {
        this.parseConnections(connections);
        this.isPageLoading = false;
        this.cdr.detectChanges();
      },
      error: () => {
        this.isPageLoading = false;
        this.cdr.detectChanges();
      }
    });
  }

  private parseConnections(connections: CrmConnection[]) {
    let nextSourceConnected = false;
    let nextTargetConnected = false;
    let nextSelectedSource = '';
    let nextSelectedTarget = '';
    let nextSourceUrl = '';
    let nextTargetUrl = '';
    let nextSourceExpired = false;
    let nextTargetExpired = false;

    const expiredRows: CrmConnection[] = [];

    connections.forEach((conn) => {
      const side = conn.connection_role;
      if (side !== 'source' && side !== 'target') return;

      const url = this.applyConnectionDetails(side, conn);

      // Expired token => NOT "Connected". Keep the CRM selected (so the Login button is right there),
      // clear the connected state + URL, and remember to purge the dead row from the server.
      if (isConnectionExpired(conn)) {
        expiredRows.push(conn);
        if (side === 'source') {
          nextSelectedSource = conn.crm_type;
          nextSourceExpired = true;
        } else {
          nextSelectedTarget = conn.crm_type;
          nextTargetExpired = true;
        }
        return;
      }

      if (side === 'source') {
        nextSelectedSource = conn.crm_type;
        nextSourceConnected = true;
        nextSourceUrl = url;
      } else {
        nextSelectedTarget = conn.crm_type;
        nextTargetConnected = true;
        nextTargetUrl = url;
      }
    });

    // api-mapping told us this side expired and we already deleted its row: still show "log in again" for that CRM.
    if (!nextSelectedSource && this.forcedExpired.source) {
      nextSelectedSource = this.forcedExpired.source;
      nextSourceExpired = true;
    }
    if (!nextSelectedTarget && this.forcedExpired.target) {
      nextSelectedTarget = this.forcedExpired.target;
      nextTargetExpired = true;
    }
    this.forcedExpired = {};

    if (expiredRows.length) {
      this.showExpiredToast();
      this.purgeExpiredConnections(expiredRows.map((c) => c.connection_role as 'source' | 'target'));
    }

    this.selectedSource = nextSelectedSource;
    this.selectedTarget = nextSelectedTarget;
    this.isSourceConnected = nextSourceConnected;
    this.isTargetConnected = nextTargetConnected;
    this.sourceInstanceUrl = nextSourceUrl;
    this.targetInstanceUrl = nextTargetUrl;
    this.sourceSessionExpired = nextSourceExpired;
    this.targetSessionExpired = nextTargetExpired;

    // The slots drive the api-mapping page: only a LIVE connection may keep one.
    this.syncSlot('source_crm_slot', nextSourceConnected ? nextSelectedSource : '');
    this.syncSlot('target_crm_slot', nextTargetConnected ? nextSelectedTarget : '');

    this.isSourceConnecting = false;
    this.isTargetConnecting = false;

    this.cdr.detectChanges();
  }

  /** Copies the saved login parameters (env / region / subdomain) into the form; returns the display URL. */
  private applyConnectionDetails(side: 'source' | 'target', conn: CrmConnection): string {
    const isSource = side === 'source';
    let url = '';

    if (conn.crm_type === 'salesforce') {
      url = conn.instance_url || '';
      if (conn.environment) {
        if (isSource) this.sourceSalesforceEnv = conn.environment;
        else this.targetSalesforceEnv = conn.environment;
      }
    } else if (conn.crm_type === 'zoho') {
      url = conn.api_domain || '';
      if (conn.region) {
        if (isSource) this.sourceZohoRegion = conn.region;
        else this.targetZohoRegion = conn.region;
      }
    } else if (conn.crm_type === 'zendesk') {
      url = conn.subdomain ? `https://${conn.subdomain}.zendesk.com` : '';
      if (conn.subdomain) {
        if (isSource) this.sourceZendeskSubdomain = conn.subdomain;
        else this.targetZendeskSubdomain = conn.subdomain;
      }
    }
    return url;
  }

  private syncSlot(key: string, value: string) {
    if (value) localStorage.setItem(key, value);
    else localStorage.removeItem(key);
  }

  /** Fire-and-forget: delete dead connection rows so they stop reappearing (and stop re-toasting) on every visit. */
  private purgeExpiredConnections(sides: Array<'source' | 'target'>) {
    forkJoin(sides.map((side) => this.crmAuthService.disconnectCrm(side).pipe(catchError(() => of(null))))).subscribe(() =>
      window.dispatchEvent(new Event('connections-updated'))
    );
  }

  private showExpiredToast() {
    if (this.expiredToastShown) return;
    this.expiredToastShown = true;
    this.toastr.warning('Your CRM session has expired. Please log in again.', 'Session Expired');
  }

  getCrmConfig(crmId: string) {
    return this.availableCRMs.find(crm => crm.id === crmId);
  }

  onCrmChange(side: 'source' | 'target') {
    if (side === 'source') {
      this.isSourceConnected = false;
      this.sourceSessionExpired = false;
      this.sourceInstanceUrl = '';
    } else {
      this.isTargetConnected = false;
      this.targetSessionExpired = false;
      this.targetInstanceUrl = '';
    }
  }

  loginToCRM(side: 'source' | 'target') {
    const selectedCrmId = side === 'source' ? this.selectedSource : this.selectedTarget;
    const subdomain = side === 'source' ? this.sourceZendeskSubdomain : this.targetZendeskSubdomain;
    const region = side === 'source' ? this.sourceZohoRegion : this.targetZohoRegion;
    const env = side === 'source' ? this.sourceSalesforceEnv : this.targetSalesforceEnv;

    if (selectedCrmId === 'zendesk' && (!subdomain || subdomain.trim() === '')) {
      this.toastr.warning(`Please enter your ${side} Zendesk subdomain to continue.`);
      return;
    }

    if (side === 'source') {
      this.isSourceConnecting = true;
    } else {
      this.isTargetConnecting = true;
    }

    this.crmAuthService.connectCrm(selectedCrmId, side, subdomain, region, env);
    
    setTimeout(() => {
      this.isSourceConnecting = false;
      this.isTargetConnecting = false;
      this.cdr.detectChanges();
    }, 5000);
  }

  disconnectCRM(side: 'source' | 'target') {
    this.isPageLoading = true;
    
    this.crmAuthService.disconnectCrm(side).subscribe({
      next: () => {
        this.toastr.success(`${side.toUpperCase()} disconnected successfully.`);
        
        if (side === 'source') {
          this.isSourceConnected = false;
          this.sourceSessionExpired = false;
          this.selectedSource = '';
          this.sourceInstanceUrl = '';

          localStorage.removeItem('source_crm_slot');
        } else {
          this.isTargetConnected = false;
          this.targetSessionExpired = false;
          this.selectedTarget = '';
          this.targetInstanceUrl = '';

          localStorage.removeItem('target_crm_slot');
        }
        
        window.dispatchEvent(new Event('connections-updated'));
        
        this.isPageLoading = false;
        this.cdr.detectChanges();
      },
      error: () => {
        this.toastr.error('Failed to disconnect. Please try again.');
        this.isPageLoading = false;
        this.cdr.detectChanges();
      }
    });
  }

  goToMappingPage(method: 'api' | 'csv') {
    if (method === 'api' && (!this.isSourceConnected || !this.isTargetConnected)) return;
    if (method === 'csv' && !this.isTargetConnected) return;

    localStorage.setItem('target_crm_slot', this.selectedTarget);

    if (method === 'api') {
      if (!this.isSourceConnected || !this.isTargetConnected) return;
      localStorage.setItem('source_crm_slot', this.selectedSource);
      
      this.router.navigate(['/api-mapping'], {
        state: { sourceCrm: this.selectedSource, targetCrm: this.selectedTarget }
      });
      
    } else if (method === 'csv') {
      if (!this.isTargetConnected) return;
      localStorage.setItem('source_crm_slot', 'csv');
      
      this.router.navigate(['/data-validation'], {
        state: { sourceCrm: 'csv', targetCrm: this.selectedTarget }
      });
    }
  }

  ngOnDestroy() {
    if (this.authSubscription) {
      this.authSubscription.unsubscribe();
    }
  }
}