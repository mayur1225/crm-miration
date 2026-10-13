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
  private expiredToastShown = false;

  ngOnInit() {
    this.isPageLoading = true;

    // Sides the API-mapping page told us have an expired session (consumed once, so a refresh won't repeat it).
    const expiredSides: Array<'source' | 'target'> = history.state?.expiredSides || [];
    if (expiredSides.length) {
      history.replaceState({ ...history.state, expiredSides: undefined }, '');
      this.showExpiredToast();
    }

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
        const clear$: Observable<unknown> = expiredSides.length
          ? forkJoin(expiredSides.map((side) => this.crmAuthService.disconnectCrm(side).pipe(catchError(() => of(null)))))
          : of(null);
        expiredSides.length = 0; // only clear once, even if query params change again

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

    const expiredRoles: string[] = [];

    connections.forEach(conn => {
      // Expired token => treat as not connected (no "Connected" badge, no pre-selected CRM).
      if (isConnectionExpired(conn)) {
        expiredRoles.push(conn.connection_role);
        return;
      }

      if (conn.connection_role === 'source') {
        nextSelectedSource = conn.crm_type;
        nextSourceConnected = true;
        
        if (conn.crm_type === 'salesforce') {
          nextSourceUrl = conn.instance_url || '';
          if (conn.environment) this.sourceSalesforceEnv = conn.environment;
        } else if (conn.crm_type === 'zoho') {
          nextSourceUrl = conn.api_domain || '';
          if (conn.region) this.sourceZohoRegion = conn.region;
        } else if (conn.crm_type === 'zendesk') {
          nextSourceUrl = conn.subdomain ? `https://${conn.subdomain}.zendesk.com` : '';
          if (conn.subdomain) this.sourceZendeskSubdomain = conn.subdomain;
        }
      } 
      else if (conn.connection_role === 'target') {
        nextSelectedTarget = conn.crm_type;
        nextTargetConnected = true;
        
        if (conn.crm_type === 'salesforce') {
          nextTargetUrl = conn.instance_url || '';
          if (conn.environment) this.targetSalesforceEnv = conn.environment;
        } else if (conn.crm_type === 'zoho') {
          nextTargetUrl = conn.api_domain || '';
          if (conn.region) this.targetZohoRegion = conn.region;
        } else if (conn.crm_type === 'zendesk') {
          nextTargetUrl = conn.subdomain ? `https://${conn.subdomain}.zendesk.com` : '';
          if (conn.subdomain) this.targetZendeskSubdomain = conn.subdomain;
        }
      }
    });

    if (expiredRoles.includes('source')) {
      this.sourceZendeskSubdomain = '';
      this.sourceZohoRegion = 'IN';
      this.sourceSalesforceEnv = 'production';
    }
    if (expiredRoles.includes('target')) {
      this.targetZendeskSubdomain = '';
      this.targetZohoRegion = 'IN';
      this.targetSalesforceEnv = 'production';
    }
    if (expiredRoles.length) this.showExpiredToast();

    this.selectedSource = nextSelectedSource;
    this.selectedTarget = nextSelectedTarget;
    this.isSourceConnected = nextSourceConnected;
    this.isTargetConnected = nextTargetConnected;
    this.sourceInstanceUrl = nextSourceUrl;
    this.targetInstanceUrl = nextTargetUrl;

    if (this.selectedSource) {
      localStorage.setItem('source_crm_slot', this.selectedSource);
    } else {
      localStorage.removeItem('source_crm_slot');
    }

    if (this.selectedTarget) {
      localStorage.setItem('target_crm_slot', this.selectedTarget);
    } else {
      localStorage.removeItem('target_crm_slot');
    }

    this.isSourceConnecting = false;
    this.isTargetConnecting = false;

    this.cdr.detectChanges();
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
    } else {
      this.isTargetConnected = false;
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
          this.selectedSource = '';
          this.sourceInstanceUrl = '';

          localStorage.removeItem('source_crm_slot');
        } else {
          this.isTargetConnected = false;
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