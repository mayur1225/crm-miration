import { Injectable, inject, signal } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, tap } from 'rxjs';
import { Router } from '@angular/router';
import { environment } from 'src/environments/environment';

export interface AuthResponse {
  success: boolean;
  token?: string;
  refresh_token?: string;
  message?: string;
  user?: {
    id: string;
    email: string;
    full_name: string;
  };
}

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private http = inject(HttpClient);
  private router = inject(Router);

  private apiUrl = `${environment.apiUrl}/api/auth`;

  currentUser = signal<string | null>(localStorage.getItem('supabase_token'));

  constructor() {
    window.addEventListener('storage', (event: StorageEvent) => {
      if (event.key !== 'supabase_token') {
        return;
      }
      const newToken = event.newValue;
      this.currentUser.set(newToken);
      if (!newToken) {
        this.router.navigate(['/login'], { replaceUrl: true });
      }
    });
  }

  login(credentials: any): Observable<AuthResponse> {
    return this.http.post<AuthResponse>(`${this.apiUrl}/login`, credentials).pipe(
      tap(res => {
        if (res.success && res.token && res.refresh_token) {
          localStorage.setItem('supabase_token', res.token);
          localStorage.setItem('supabase_refresh', res.refresh_token);
          localStorage.setItem('supabase_user', JSON.stringify(res.user));
          this.currentUser.set(res.token);
        }
      })
    );
  }

  signup(data: { email: string; password: string; full_name: string }) {
    return this.http.post<any>(`${this.apiUrl}/signup`, data);
  }

  refreshToken(): Observable<any> {
    const refreshToken = localStorage.getItem('supabase_refresh');
    return this.http.post<any>(`${this.apiUrl}/refresh`, { refresh_token: refreshToken }).pipe(
      tap(res => {
        if (res.success && res.token) {
          localStorage.setItem('supabase_token', res.token);
          localStorage.setItem('supabase_refresh', res.refresh_token);
          this.currentUser.set(res.token);
        }
      })
    );
  }

  updateEmail(newEmail: string) {
    const token = localStorage.getItem('supabase_token');
    const headers = new HttpHeaders().set('Authorization', `Bearer ${token}`);

    return this.http.put(`${this.apiUrl}/update-email`,
      { new_email: newEmail },
      { headers }
    );
  }

  isLoggedIn(): boolean {
    return !!this.currentUser();
  }

  forgotPassword(email: string): Observable<any> {
    return this.http.post<any>(`${this.apiUrl}/forgot-password`, { email });
  }

  forceLogout() {
    this.clearLocalSession();
  }

  private clearLocalSession() {
    localStorage.removeItem('supabase_token');
    localStorage.removeItem('supabase_refresh');
    localStorage.removeItem('supabase_user');
    this.currentUser.set(null);
    this.router.navigate(['/login'], { replaceUrl: true });
  }

  logout() {
    this.http.post(`${this.apiUrl}/logout`, {}).subscribe({
      next: () => this.clearLocalSession(),
      error: () => this.clearLocalSession()
    });
  }
}