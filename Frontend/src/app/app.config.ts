import { ApplicationConfig, provideZoneChangeDetection } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { routes } from './app-routing.module';
import { provideToastr } from 'ngx-toastr';
import { provideMonacoEditor } from 'ngx-monaco-editor-v2';

import { authInterceptor } from './demo/Services/auth.interceptor';
import { connectivityInterceptor } from '../app/services/Connectivity.interceptor';

export const appConfig: ApplicationConfig = {
  providers: [
    provideZoneChangeDetection({ eventCoalescing: true }),
    provideRouter(routes),
    provideMonacoEditor({
      baseUrl: '/assets/monaco/vs'
    }),

    provideHttpClient(withInterceptors([connectivityInterceptor, authInterceptor])),

    provideToastr({
      timeOut: 3000,
      positionClass: 'toast-top-right',
      preventDuplicates: true
    })
  ]
};