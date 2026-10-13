import { HttpInterceptorFn, HttpErrorResponse } from '@angular/common/http';
import { inject } from '@angular/core';
import { Observable, throwError } from 'rxjs';
import { catchError, finalize, map, shareReplay, switchMap } from 'rxjs/operators';
import { AuthService } from './auth.service';
import { environment } from 'src/environments/environment';

let refresh$: Observable<string> | null = null;

function refreshOnce(authService: AuthService): Observable<string> {
  if (!refresh$) {
    refresh$ = authService.refreshToken().pipe(
      map((res: any) => {
        if (!res?.token) throw new Error('Token refresh returned no token');
        return res.token as string;
      }),
      finalize(() => { refresh$ = null; }),
      shareReplay(1)
    );
  }
  return refresh$;
}

function isCrmSessionError(error: HttpErrorResponse): boolean {
  const detail = error?.error?.detail;
  return !!detail && typeof detail === 'object' && detail.code === 'CRM_SESSION_EXPIRED';
}


const PUBLIC_AUTH_SUFFIXES = [
  '/api/auth/login',
  '/api/auth/signup',
  '/api/auth/refresh',
  '/api/auth/logout',
  '/api/auth/forgot-password'
];

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const authService = inject(AuthService);

  if (!req.url.startsWith(environment.apiUrl)) {
    return next(req);
  }

  if (PUBLIC_AUTH_SUFFIXES.some((suffix) => req.url.endsWith(suffix))) {
    return next(req);
  }

  let token = authService.currentUser();
  if (token === 'null' || token === 'undefined' || !token) {
    token = null;
  }

  const authReq = token ? req.clone({ setHeaders: { Authorization: `Bearer ${token}` } }) : req;

  return next(authReq).pipe(
    catchError((error: HttpErrorResponse) => {
      if (error.status !== 401 || !token || isCrmSessionError(error)) {
        return throwError(() => error);
      }

      return refreshOnce(authService).pipe(
        // Only a failed *refresh* ends the app session -- not a failure of the retried request.
        catchError((err) => {
          authService.forceLogout();
          return throwError(() => err);
        }),
        switchMap((newToken) => next(req.clone({ setHeaders: { Authorization: `Bearer ${newToken}` } })))
      );
    })
  );
};