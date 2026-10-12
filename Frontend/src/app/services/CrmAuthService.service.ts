import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';
import { AuthService } from '../demo/Services/auth.service';

export interface CrmConnection {
  id?: string;
  user_id?: string;
  crm_type: string;
  connection_role: 'source' | 'target';
  subdomain?: string;
  region?: string;
  instance_url?: string;
  api_domain?: string;
  accounts_server?: string;
  environment?: string;
  token_expired?: boolean;
  status?: 'connected' | 'expired';
}

@Injectable({
  providedIn: 'root'
})
export class CrmAuthService {
  private http = inject(HttpClient);
  private authService = inject(AuthService);

  private apiBaseUrl = `${environment.apiUrl}/api/crm`;

  private getAuthHeaders(): HttpHeaders {
    const token = this.authService.currentUser() ?? '';
    return new HttpHeaders().set('Authorization', `Bearer ${token}`);
  }

  getUserConnections(): Observable<CrmConnection[]> {
    return this.http.get<CrmConnection[]>(`${this.apiBaseUrl}/connections`, {
      headers: this.getAuthHeaders()
    });
  }

  connectCrm(crmId: string, side: 'source' | 'target', subdomain?: string, region?: string, environment: string = 'production'): void {
    const safeCrmId = crmId.toLowerCase();

    let requestUrl = `${this.apiBaseUrl}/auth/${safeCrmId}/login?side=${side}`;

    if (subdomain) {
      requestUrl += `&subdomain=${encodeURIComponent(subdomain)}`;
    }
    if (safeCrmId === 'zoho' && region) {
      requestUrl += `&region=${encodeURIComponent(region)}`;
    }
    if (safeCrmId === 'salesforce') {
      requestUrl += `&environment=${encodeURIComponent(environment)}`;
    }

    this.http.get<{ url: string }>(requestUrl, { headers: this.getAuthHeaders() }).subscribe({
      next: (response) => {
        if (response?.url) {
          window.location.href = response.url;
        } else {
          console.error(`Backend failed to generate a valid OAuth URL for ${crmId}.`);
        }
      },
      error: (err) => {
        console.error(`Failed to initialize ${crmId} authentication path:`, err);
      }
    });
  }

  disconnectCrm(side: 'source' | 'target'): Observable<any> {
    return this.http.delete(`${this.apiBaseUrl}/connections/${side}`, {
      headers: this.getAuthHeaders()
    });
  }
}