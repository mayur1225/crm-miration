import { HttpContextToken, HttpErrorResponse, HttpInterceptorFn, HttpResponse } from '@angular/common/http';
import { inject } from '@angular/core';
import { retry, tap, throwError, timer } from 'rxjs';
import { ConnectivityService, TRANSIENT_HTTP_STATUSES } from './connectivity.service';

export const SKIP_CONNECTIVITY = new HttpContextToken<boolean>(() => false);

const MAX_GET_RETRIES = 2;
const RETRY_BASE_DELAY_MS = 400;

const isTransient = (error: unknown): error is HttpErrorResponse =>
  error instanceof HttpErrorResponse && TRANSIENT_HTTP_STATUSES.has(error.status);

export const connectivityInterceptor: HttpInterceptorFn = (req, next) => {
  if (req.context.get(SKIP_CONNECTIVITY)) return next(req);

  const connectivity = inject(ConnectivityService);
  const idempotent = req.method === 'GET' || req.method === 'HEAD';

  return next(req).pipe(
    retry({
      count: idempotent ? MAX_GET_RETRIES : 0,
      delay: (error, attempt) => (isTransient(error) ? timer(RETRY_BASE_DELAY_MS * 2 ** (attempt - 1)) : throwError(() => error))
    }),
    tap({
      next: (event) => {
        if (event instanceof HttpResponse) connectivity.reportSuccess();
      },
      error: (error: unknown) => {
        if (isTransient(error)) void connectivity.reportFailure();
      }
    })
  );
};