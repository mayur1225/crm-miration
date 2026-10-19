// Angular import
import { Component, HostListener, inject } from '@angular/core';
import { Router, RouterOutlet } from '@angular/router';

// project import
import { SpinnerComponent } from './theme/shared/components/spinner/spinner.component';
import { AuthService } from '../app/demo/Services/auth.service';
import { ServiceUnavailableComponent } from '../app/ServiceUnavailableComponent';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
  imports: [RouterOutlet, SpinnerComponent, ServiceUnavailableComponent]
})
export class AppComponent {
  title = 'SureShift';
  public authService = inject(AuthService);
  private router = inject(Router);

  onLogout() {
    this.authService.logout();
  }

  @HostListener('window:pageshow', ['$event'])
  onPageShow(event: PageTransitionEvent): void {
    if (event.persisted && !this.authService.isLoggedIn()) {
      this.router.navigate(['/login'], { replaceUrl: true });
    }
  }
}