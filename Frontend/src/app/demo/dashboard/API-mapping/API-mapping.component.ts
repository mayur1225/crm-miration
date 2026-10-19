import { CommonModule } from '@angular/common';
import { Component, OnInit, inject, ChangeDetectorRef, NgZone, HostListener, OnDestroy } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { forkJoin, of, Subject } from 'rxjs';
import { takeUntil, catchError } from 'rxjs/operators';
import { MappingApiService } from 'src/app/services/mapping-api.service';
import { ToastrService } from 'ngx-toastr';
import Swal from 'sweetalert2';
import { EditorComponent } from 'ngx-monaco-editor-v2';
import { environment } from 'src/environments/environment';
import { HttpClient } from '@angular/common/http';
import { AuthService } from 'src/app/demo/Services/auth.service';
import { CrmAuthService } from 'src/app/services/CrmAuthService.service';
import {
  CrmSide,
  getExpiredSidesFromError,
  getSocketExpiredSides,
  isAuthExpiredError,
  isConnectionExpired,
  isSocketAuthExpired
} from 'src/app/services/crm-session.util';
declare const monaco: any;
interface FieldMeta {
  name: string;
  label: string;
  type?: string;
  required?: boolean;
  isRequired?: boolean;
  referenceTo?: string[];
  relationshipName?: string;
  externalId?: boolean;
  unique?: boolean;
  idLookup?: boolean;
  createable?: boolean;
  updateable?: boolean;
}

interface FieldAccessViolation {
  targetField: string;
  label: string;
}

interface MappingRow {
  sourceField: string;
  sourceLabel: string;
  targetField: string;
  isDropdownOpen?: boolean;
  searchQuery?: string;
  relationalExtIdField?: string;
  parentObjectName?: string;
  massUpdateValue?: string;
  _isAiProcessing?: boolean;
  _mappedBy?: 'rule' | 'ai';
  _blockedTargetField?: string;
  _blockedTargetLabel?: string;
  syncPicklistValues?: boolean;
}

interface CrmEntity {
  name: string;
  label: string;
}

interface FileMigrationBudget {
  jobRef: string;
  targetObject: string;
  message: string;
  bindingOrg: 'source' | 'target' | 'none';
  estimatedDownloadCalls: number;
  estimatedUploadCalls: number;
  estimatedTotalCalls: number;
  totalRecordCount: number;
  safeRecordCount: number;
  totalFileCount: number;
  safeFileCount: number;
  sourceAvailable: number;
  targetAvailable: number;
}

type FileMigrationDecisionAction = 'proceed_limited' | 'proceed_full' | 'skip';

interface FileMigrationBudgetPreview {
  fitsInBudget: boolean;
  bindingOrg: 'source' | 'target' | 'none';
  sourceAvailable: number;
  targetAvailable: number;
  estimatedDownloadCalls: number;
  estimatedUploadCalls: number;
  estimatedTotalCalls: number;
  totalRecordCount: number;
  safeRecordCount: number;
  totalFileCount: number;
  safeFileCount: number;
  message: string;
  attachmentFileCount: number;
  contentFileCount: number;
  sourceBudgetVerified?: boolean;
  targetBudgetVerified?: boolean;
}

@Component({
  selector: 'app-api-mapping',
  standalone: true,
  imports: [CommonModule, FormsModule, EditorComponent],
  templateUrl: './API-mapping.component.html',
  styleUrls: ['./API-mapping.component.scss']
})
export class ApiMappingComponent implements OnInit, OnDestroy {
  private router = inject(Router);
  private mappingApi = inject(MappingApiService);
  private cdr = inject(ChangeDetectorRef);
  private zone = inject(NgZone);
  private toastr = inject(ToastrService);
  private authService = inject(AuthService);
  private crmAuthService = inject(CrmAuthService);
  private isRedirectingToLogin = false;
  private validationSocket: WebSocket | null = null;
  private migrationSocket: WebSocket | null = null;
  currentRunId: string | null = null;
  canResume = false;
  canRetryFailed = false;
  retryCount = 0;
  private static readonly LAST_RUN_KEY = 'last_migration_run_id';
  private http = inject(HttpClient);

  private mappingCancel$ = new Subject<void>();

  private allSourceFields: FieldMeta[] = [];
  private readonly allowOrderBy = true;
  private readonly PICKLIST_TYPES_BY_CRM: Record<string, string[]> = {
    salesforce: ['picklist', 'multipicklist'],
    hubspot: ['picklist'],
    zoho: ['picklist'],
    zendesk: ['tagger', 'dropdown', 'multiselect'],
  };

  private readonly SYSTEM_MANAGED_FIELDS = new Set<string>([
    'hs_object_id',
    'url',
    'createddate',
    'lastmodifieddate',
    'createdbyid',
    'lastmodifiedbyid',
    'systemmodstamp',
    'isdeleted',
    'hs_createdate',
    'hs_lastmodifieddate',
    'createdate',
    'archived',
    'created_at',
    'updated_at',
    'submitter_id',
    'created_time',
    'modified_time',
    'created_by',
    'modified_by',
    '$state',
    '$process_flow',
    'createdat',
    'updatedat',
    'updateddate',
    'deleted',
    'ispartner',           // "Partner Account"
    'iscustomerportal'     // "Customer Portal Account"
  ]);

  private isSystemManagedField(fieldName: string): boolean {
    return this.SYSTEM_MANAGED_FIELDS.has((fieldName || '').toLowerCase());
  }

  private isStandardZendeskObject(name: string): boolean {
    if (!name) return false;
    const std = ['tickets', 'users', 'organizations', 'groups', 'macros', 'triggers', 'views'];
    const safeName = name.toLowerCase();
    return std.includes(safeName) || std.includes(safeName + 's');
  }
  // --- ADD THIS HELPER METHOD ---
  private flattenObject(ob: any): any {
    const result: any = {};
    for (const i in ob) {
      if (!ob.hasOwnProperty(i)) continue;

      if (typeof ob[i] === 'object' && ob[i] !== null && !Array.isArray(ob[i])) {
        const flatObject = this.flattenObject(ob[i]);
        for (const x in flatObject) {
          if (!flatObject.hasOwnProperty(x)) continue;
          result[x] = flatObject[x];
        }
      } else {
        result[i] = ob[i];
      }
    }
    return result;
  }

  isGlobalLoading: boolean = false;
  globalLoadingText: string = 'Loading...';
  globalLoadingSubText: string = 'Please wait...';
  // CRM Identifiers from previous step
  sourceCrmId: string = '';
  targetCrmId: string = '';
  sourceSystem = 'Unknown';
  targetSystem = 'Unknown';
  currentSessionId: string = '';
  previewLimit: number = 5;
  isAutoMapping: boolean = false;
  autoMapProgress = { current: 0, total: 0 };

  // Dynamic Entity Lists for Dropdowns
  sourceEntities: CrmEntity[] = [];
  targetEntities: CrmEntity[] = [];
  showReviewPanel: boolean = false;
  reviewPanelMinimized: boolean = false;
  reviewFilter: 'mapped' | 'unmapped' = 'mapped';

  readonly reviewPanelDefaultWidth = 1000;
  readonly reviewPanelDefaultHeight = 680;
  reviewPanelTop = 100;
  reviewPanelLeft = 100;
  private reviewPanelDragging = false;
  private reviewPanelDragOffset = { x: 0, y: 0 };

  selectedSourceObject = '';
  selectedTargetObject = '';
  isLoading = false;

  // Data Preview Variables
  previewHeaders: string[] = [];
  previewRecords: any[] = [];

  // Mapping Variables
  targetFields: FieldMeta[] = [];
  parentFieldsCache: Record<string, FieldMeta[]> = {};
  isLoadingParentFields = false;
  mappings: MappingRow[] = [];
  externalIdField = '';
  mappedCount = 0;
  isStrictMapping = false;
  hideMappedFields = false;
  selectedSourceObjectCount: number | null = null;
  isSourceCountLoading = false;
  restrictMappingToQueryFields = true;

  currentUser: any = null;
isProfileDropdownOpen = false;

  // Execution Variables
  jobStatus = 'Idle';
  logMessages: string[] = [];
  customQuery: string = '';
  isDefaultQuery: boolean = true;
  queryError: string | null = null;
  isPreviewLoading = false;
  sourceFields: FieldMeta[] = [];
  successData: any[] = [];
  isQueueMinimized: boolean = false;
  errorData: any[] = [];
  skippedData: any[] = [];
  aggregateStats = { total: 0, valid: 0, invalid: 0, duplicates: 0 };
  validationResults: any = null;
  isValidating = false;
  errorCurrentPage: number = 1;
  errorPageSize: number = 10;
  mappingSearchQuery = '';
  reviewPanelExpanded: boolean = false;
  isSourceDropdownOpen = false;
  sourceSearchQuery = '';
  isTargetDropdownOpen = false;
  targetSearchQuery = '';
  isHistoryDropdownOpen = false;
  isMigrationFilterOpen = false;

  operationMode: string = 'insert';
  private lastOperationMode: string = 'insert';
  batchSize: number = 5000;
  migrationQueue: any[] = [];

  // Files & Attachments (Salesforce -> Salesforce only)
  migrateAttachments = false;
  migrateFiles = false;
  pendingFileMigrationBudget: FileMigrationBudget | null = null;

  // Pre-flight (informational)
  fileMigrationBudgetPreview: FileMigrationBudgetPreview | null = null;
  isCheckingFileMigrationBudget = false;
  fileMigrationBudgetPreviewError: string | null = null;
  private fileMigrationBudgetPreviewDebounce: ReturnType<typeof setTimeout> | null = null;

  get isSalesforceToSalesforce(): boolean {
    return this.sourceCrmId?.toLowerCase() === 'salesforce' && this.targetCrmId?.toLowerCase() === 'salesforce';
  }

  private readonly FILE_MIGRATION_CRMS = ['salesforce', 'zoho', 'hubspot'];
  private readonly SINGLE_FILE_TYPE_CRMS = ['zoho', 'hubspot'];

  canSyncPicklist(mapping: MappingRow): boolean {
    if (!this.isPicklistSyncSupported || !mapping.sourceField || !mapping.targetField) return false;
    const sourceTypes = this.PICKLIST_TYPES_BY_CRM[this.sourceCrmId.toLowerCase()];
    const targetTypes = this.PICKLIST_TYPES_BY_CRM[this.targetCrmId.toLowerCase()];
    const s = this.getFieldMeta(mapping.sourceField, 'source')?.type?.toLowerCase();
    const t = this.getFieldMeta(mapping.targetField, 'target')?.type?.toLowerCase();
    return !!s && !!t && sourceTypes.includes(s) && targetTypes.includes(t);
  }

   get isPicklistSyncSupported(): boolean {
    const s = this.sourceCrmId?.toLowerCase();
    const t = this.targetCrmId?.toLowerCase();
    return !!s && !!t && !!this.PICKLIST_TYPES_BY_CRM[s] && !!this.PICKLIST_TYPES_BY_CRM[t];
  }

get fileMigrationSupported(): boolean {
  const s = this.sourceCrmId?.toLowerCase();
  const t = this.targetCrmId?.toLowerCase();
  return !!s && !!t && this.FILE_MIGRATION_CRMS.includes(s) && this.FILE_MIGRATION_CRMS.includes(t);
}

get isSourceZoho(): boolean {
  return this.sourceCrmId?.toLowerCase() === 'zoho';
}

get isSourceSingleFileType(): boolean {
  return this.SINGLE_FILE_TYPE_CRMS.includes(this.sourceCrmId?.toLowerCase());
}

get fileMigrationNotes(): string[] {
  const s = this.sourceCrmId?.toLowerCase();
  const t = this.targetCrmId?.toLowerCase();
  const notes: string[] = [];
  if (t === 'zoho') notes.push('Zoho accepts attachments up to 20 MB per file; larger files are reported as errors.');
  if (s === 'hubspot') notes.push('Files attached to a record\'s Notes, Calls, Meetings and Tasks are migrated from HubSpot (files in the standalone Files tool are not).');
  if (s === 'zoho' && t === 'salesforce') notes.push('Zoho attachments are created as Salesforce Files.');
  return notes;
}

  recentQueries: string[] = [];
  // --- MONACO EDITOR CONFIGURATION ---
  editorOptions = {
    theme: 'vs', // Use 'vs-dark' if you want a dark code editor!
    language: 'sql', // Highlights SELECT, WHERE, AND, OR automatically
    minimap: { enabled: false }, // Hides the code minimap on the right
    scrollBeyondLastLine: false,
    wordWrap: 'on',
    lineNumbers: 'off',
    renderLineHighlight: 'none',
    fontSize: 13,
    padding: { top: 10, bottom: 10 },
    suggestOnTriggerCharacters: true,
    automaticLayout: true // re-fits the editor when its container is resized/expanded
  };

  // --- EXPANDABLE QUERY EDITOR ---
  isQueryEditorExpanded = false;
  queryEditorHeight = 120; // px; also changes when the user drags the resize handle

  toggleQueryEditorSize(): void {
    this.isQueryEditorExpanded = !this.isQueryEditorExpanded;
    this.queryEditorHeight = this.isQueryEditorExpanded ? 350 : 120;
    setTimeout(() => this.monacoEditorInstance?.layout(), 0);
  }

  monacoEditorInstance: any;
  completionProvider: any;
  hoverProvider: any;

  recoverableSessions: any[] = [];
  isSessionsLoading = false;

  ngOnInit(): void {
  this.getUserData();
  void this.restoreLastRun();

  const navState = history.state;
  this.sourceCrmId = navState?.sourceCrm || localStorage.getItem('source_crm_slot');
  this.targetCrmId = navState?.targetCrm || localStorage.getItem('target_crm_slot');

  if (!this.sourceCrmId || !this.targetCrmId) {
    this.toastr.warning('Please connect your Source and Target systems first.', 'Connections Required');
    this.router.navigate(['/connection']);
    return;
  }

  this.sourceSystem = this.sourceCrmId;
  this.targetSystem = this.targetCrmId;
  this.migrationTimeFilter.field = this.timeFilterFieldOptions[0]?.value || '';
  this.batchSize = this.batchConfig.default;

  localStorage.setItem('source_crm_slot', this.sourceCrmId);
  localStorage.setItem('target_crm_slot', this.targetCrmId);

  this.recentQueries = JSON.parse(localStorage.getItem('crm_query_history') || '[]');

  this.showSchemaLoader();
  this.verifyCrmSessions(() => this.fetchRecoverableSessions());
  this.preloadEntirePage();
}

private showSchemaLoader(): void {
  this.isLoading = true;
  this.isGlobalLoading = true;
  this.globalLoadingText = 'Fetching Live Schemas...';
  this.globalLoadingSubText = 'Please wait while we sync the CRM engines';
}

  // ==========================================
  //  CRM SESSION EXPIRY
  // ==========================================
  private verifyCrmSessions(onValid: () => void): void {
    this.crmAuthService.getUserConnections().subscribe({
      next: (connections: any[]) => {
        const isLive = (role: 'source' | 'target') =>
          connections.some((c) => c.connection_role === role && !isConnectionExpired(c));

        const expiredSides = (['source', 'target'] as const).filter((role) => !isLive(role));
        if (expiredSides.length) {
          this.handleSessionExpired(expiredSides);
          return;
        }
        onValid();
      },
      error: (err) => {
        if (isAuthExpiredError(err)) {
          this.handleSessionExpired(getExpiredSidesFromError(err));
        } else {
          onValid(); 
        }
      }
    });
  }

  handleSessionExpired(sides?: CrmSide[]): void {
    if (this.isRedirectingToLogin) return;
    this.isRedirectingToLogin = true;

    const expiredSides: CrmSide[] = sides && sides.length ? sides : ['source', 'target'];

    this.mappingCancel$.next();
    this.closeSocket(this.validationSocket);
    this.closeSocket(this.migrationSocket);
    this.validationSocket = null;
    this.migrationSocket = null;
    this.isLoading = false;
    this.isGlobalLoading = false;
    this.isValidating = false;
    this.pendingFileMigrationBudget = null;

    Swal.close();

    window.dispatchEvent(new Event('connections-updated'));

    this.zone.run(() => {
      this.router.navigate(['/connection'], { replaceUrl: true, state: { expiredSides } });
    });
  }

  ngOnDestroy() {
    this.mappingCancel$.next();
    this.mappingCancel$.complete();

    if (this.pendingFileMigrationBudget && this.migrationSocket && this.migrationSocket.readyState === WebSocket.OPEN) {
      this.sendFileMigrationDecision(this.migrationSocket, this.pendingFileMigrationBudget.jobRef, 'skip');
      this.pendingFileMigrationBudget = null;
    }

    // 1. Kill active websockets
    this.closeSocket(this.validationSocket);
    this.closeSocket(this.migrationSocket);
    this.validationSocket = null;
    this.migrationSocket = null;

    if (this.completionProvider) {
      this.completionProvider.dispose();
      this.completionProvider = null;
    }
    if (this.hoverProvider) {
      this.hoverProvider.dispose();
      this.hoverProvider = null;
    }
    this.monacoEditorInstance = null;
  }

  @HostListener('window:unhandledrejection', ['$event'])
  onUnhandledRejection(event: PromiseRejectionEvent): void {
    const reason: any = event.reason;
    if (reason && reason.name === 'Canceled' && reason.message === 'Canceled') {
      event.preventDefault();
    }
  }

  private closeSocket(socket: WebSocket | null): void {
    if (!socket) return;
    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
      socket.close();
    }
  }

  async fetchRecoverableSessions(): Promise<void> {
  this.isSessionsLoading = true;
  try {
    const response = await fetch(`${environment.apiUrl}/api/validation/sessions`, {
      headers: { Authorization: `Bearer ${localStorage.getItem('supabase_token') || ''}` }
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      if (isAuthExpiredError({ status: response.status, error: errorData })) {
        this.handleSessionExpired();
        return;
      }
      throw new Error(errorData.detail || `Failed to load sessions (${response.status})`);
    }

    const data = await response.json();
    this.recoverableSessions = data.sessions || [];
  } catch (error) {
    console.error('Failed to fetch recoverable sessions:', error);
    this.recoverableSessions = [];
  } finally {
    this.isSessionsLoading = false;
    this.cdr.detectChanges();
  }
}

  resumeSession(sessionId: string, crm: string, object: string) {
    this.currentSessionId = sessionId;
    this.sourceCrmId = crm.toLowerCase();

    if (this.selectedSourceObject !== object.toLowerCase()) {
      this.resetMigrationTimeFilter();
    }
    this.selectedSourceObject = object.toLowerCase();

    this.toastr.info(`Restoring previous session...`, 'Resuming');
    this.validateData(true, []);
  }

  validateBatchSize() {
    const config = this.batchConfig;
    if (this.batchSize > config.max) {
      this.batchSize = config.max;
      this.toastr.info(`Batch size reduced to ${config.max} to comply with ${this.targetSystem} API limits.`);
    } else if (this.batchSize < config.min) {
      this.batchSize = config.min;
    }
  }

  get batchConfig() {
    const crm = (this.targetCrmId || '').toLowerCase();
    switch (crm) {
      case 'hubspot':
        return { min: 10, max: 100, step: 10, default: 100, tooltip: 'HubSpot max limit: 100' };
      case 'zendesk':
        return { min: 10, max: 100, step: 10, default: 100, tooltip: 'Zendesk max limit: 100' };
      case 'zoho':
        return { min: 10, max: 100, step: 10, default: 100, tooltip: 'Zoho max limit: 100' };
      case 'salesforce':
      default:
        return { min: 100, max: 10000, step: 100, default: 5000, tooltip: 'Salesforce max limit: 10,000' };
    }
  }

  getQuerySelectedFields(): string[] | null {
    const crm = (this.sourceCrmId || '').toLowerCase();
    const query = (this.customQuery || '').trim();
    if (!query) return null;

    switch (crm) {
      case 'salesforce':
      case 'zoho':
        return this.parseSqlStyleSelectedFields(query);
      case 'hubspot':
        return this.parseHubspotSelectedFields(query);
      case 'zendesk':
        return this.parseZendeskSelectedFields(query);
      default:
        return null;
    }
  }

  private normalizeSql(query: string): string {
    return (query || '').replace(/\s+/g, ' ').trim();
  }

  private getTopLevelFromObject(sql: string): string | null {
    let depth = 0;
    let inString = false;

    for (let i = 0; i < sql.length; i++) {
      const ch = sql[i];

      if (ch === "'" && sql[i - 1] !== '\\') {
        inString = !inString;
        continue;
      }
      if (inString) continue;

      if (ch === '(') depth++;
      else if (ch === ')') depth = Math.max(0, depth - 1);
      else if (depth === 0 && sql.startsWith(' from ', i)) {
        return sql.slice(i + 6).trim().split(/[\s,)]/)[0] || null;
      }
    }
    return null;
  }

  private splitTopLevel(input: string, delimiter: string): string[] {
    const parts: string[] = [];
    let depth = 0;
    let inString = false;
    let current = '';

    for (let i = 0; i < input.length; i++) {
      const ch = input[i];

      if (inString) {
        current += ch;
        if (ch === '\\' && i + 1 < input.length) {
          current += input[++i];
        } else if (ch === "'") {
          inString = false;
        }
        continue;
      }

      if (ch === "'") {
        inString = true;
      } else if (ch === '(') {
        depth++;
      } else if (ch === ')') {
        depth = Math.max(0, depth - 1);
      } else if (ch === delimiter && depth === 0) {
        parts.push(current);
        current = '';
        continue;
      }
      current += ch;
    }

    parts.push(current);
    return parts;
  }

  private findTopLevelFrom(sql: string, searchFrom: number): number {
    let depth = 0;
    let inString = false;

    for (let i = searchFrom; i < sql.length; i++) {
      const ch = sql[i];

      if (inString) {
        if (ch === '\\') i++;
        else if (ch === "'") inString = false;
        continue;
      }

      if (ch === "'") inString = true;
      else if (ch === '(') depth++;
      else if (ch === ')') depth = Math.max(0, depth - 1);
      else if (depth === 0 && /^from(\s|$)/i.test(sql.slice(i, i + 5)) && (i === 0 || /[\s)]/.test(sql[i - 1]))) {
        return i;
      }
    }
    return -1;
  }

  private parseSqlStyleSelectedFields(query: string): string[] | null {
    const sql = this.normalizeSql(query);

    const selectMatch = /^select\s+/i.exec(sql);
    if (!selectMatch) return null;

    const selectStart = selectMatch[0].length;
    const fromIdx = this.findTopLevelFrom(sql, selectStart);
    if (fromIdx === -1) return null;

    const selectClause = sql.substring(selectStart, fromIdx).trim();
    if (!selectClause) return null;

    const seen = new Set<string>();
    const fields: string[] = [];

    for (const rawItem of this.splitTopLevel(selectClause, ',')) {
      const item = rawItem.trim();
      if (!item) continue;

      if (item === '*' || /^fields\s*\(/i.test(item)) return null;

      if (item.startsWith('(')) continue;

      const fnMatch = /^[A-Za-z_][A-Za-z0-9_]*\s*\(\s*([A-Za-z0-9_.]+)\s*\)/.exec(item);
      const expression = fnMatch ? fnMatch[1] : item.split(/\s+/)[0];

      const root = expression.split('.')[0].trim();
      if (!root || seen.has(root.toLowerCase())) continue;

      seen.add(root.toLowerCase());
      fields.push(root);
    }

    return fields.length > 0 ? fields : null;
  }

  private parseHubspotSelectedFields(query: string): string[] | null {
    let parsed: any;
    try {
      parsed = JSON.parse(query.replace(/\/\*[\s\S]*?\*\//g, '').trim());
    } catch {
      return null; 
    }

    const properties = parsed?.properties;
    if (!Array.isArray(properties) || properties.length === 0) return null;

    const fields = properties.map((p: any) => String(p).trim()).filter((p: string) => p.length > 0);
    return fields.length > 0 ? fields : null;
  }

  private parseZendeskSelectedFields(query: string): string[] | null {
    if (!this.selectedSourceObject || this.isStandardZendeskObject(this.selectedSourceObject)) return null;

    let parsed: any;
    try {
      parsed = JSON.parse(query.replace(/\/\*[\s\S]*?\*\//g, '').trim());
    } catch {
      return null;
    }

    const fields = parsed?.fields;
    if (!Array.isArray(fields) || fields.length === 0) return null;

    const cleaned = fields.map((f: any) => String(f).trim()).filter((f: string) => f.length > 0);
    return cleaned.length > 0 ? cleaned : null;
  }

migrationTimeFilter = {
  field: '',
  startDate: '' as string, // ISO yyyy-MM-dd
  endDate: '' as string,   // ISO yyyy-MM-dd
  utcOffsetMinutes: -new Date().getTimezoneOffset()
};

dateRangeError: string | null = null;

get timeFilterFieldOptions(): { value: string; label: string }[] {
  const crm = this.sourceSystem?.toLowerCase();
  if (crm === 'zoho') {
    return [
      { value: 'Modified_Time', label: 'Last Modified' },
      { value: 'Created_Time', label: 'Created' }
    ];
  }
  if (crm === 'zendesk') {
    return [
      { value: 'updated', label: 'Last Updated' },
      { value: 'created', label: 'Created' }
    ];
  }
  if (crm === 'hubspot') {
    return [
      { value: 'hs_lastmodifieddate', label: 'Last Modified' },
      { value: 'createdate', label: 'Created' }
    ];
  }
  return [
    { value: 'LastModifiedDate', label: 'Last Modified' },
    { value: 'CreatedDate', label: 'Created' }
  ];
}

get todayIsoDate(): string {
  return new Date().toISOString().split('T')[0];
}

get migrationFilterSummary(): string {
  const f = this.migrationTimeFilter;
  if (f.startDate && f.endDate) {
    return `${f.startDate} → ${f.endDate}`;
  }
  if (f.startDate) {
    return `${f.startDate} → today`;
  }
  return 'No Filter';
}

get isMigrationFilterActive(): boolean {
  return !!this.migrationTimeFilter.startDate;
}

triggerLivePreview(): void {
  if (!this.validateDateRange()) return;
  this.applyFilter();
  this.scheduleFileMigrationBudgetRecheck();
}

validateDateRange(): boolean {
  this.dateRangeError = null;
  const { startDate, endDate } = this.migrationTimeFilter;

  if (!startDate && !endDate) return true;

  if (endDate && !startDate) {
    this.dateRangeError = "Please select a 'From' date as well — an end date on its own isn't enough to filter by.";
    return false;
  }

  const start = new Date(startDate);
  if (isNaN(start.getTime())) {
    this.dateRangeError = 'Please enter a valid start date.';
    return false;
  }

  if (!endDate) {
    return true;
  }

  const end = new Date(endDate);
  if (isNaN(end.getTime())) {
    this.dateRangeError = 'Please enter a valid end date.';
    return false;
  }

  if (start > end) {
    this.dateRangeError = 'Start date must be on or before the end date.';
    return false;
  }

  if (end > new Date(this.todayIsoDate)) {
    this.dateRangeError = 'End date cannot be in the future.';
    return false;
  }

  return true;
}

onDateRangeChange(): void {
  this.activeQuickRangePreset = null;
  this.triggerLivePreview();
}

clearDateRange(): void {
  this.migrationTimeFilter.startDate = '';
  this.migrationTimeFilter.endDate = '';
  this.activeQuickRangePreset = null;
  this.dateRangeError = null;
  this.applyFilter();
  this.scheduleFileMigrationBudgetRecheck();
}

activeQuickRangePreset: string | null = null;

readonly quickRangePresets: { key: string; label: string }[] = [
  { key: 'today', label: 'Today' },
  { key: '7d', label: 'Last 7 Days' },
  { key: '30d', label: 'Last 30 Days' },
  { key: 'thisMonth', label: 'This Month' },
  { key: 'lastMonth', label: 'Last Month' }
];

applyQuickRange(preset: string): void {
  const end = new Date();
  let start = new Date();

  switch (preset) {
    case 'today':
      start = new Date();
      break;
    case '7d':
      start.setDate(end.getDate() - 6);
      break;
    case '30d':
      start.setDate(end.getDate() - 29);
      break;
    case 'thisMonth':
      start = new Date(end.getFullYear(), end.getMonth(), 1);
      break;
    case 'lastMonth': {
      const lastMonthStart = new Date(end.getFullYear(), end.getMonth() - 1, 1);
      const lastMonthEnd = new Date(end.getFullYear(), end.getMonth(), 0);
      this.setDateRange(lastMonthStart, lastMonthEnd, preset);
      return;
    }
    default:
      return;
  }

  this.setDateRange(start, end, preset);
}

private setDateRange(start: Date, end: Date, preset: string): void {
  this.migrationTimeFilter.startDate = this.toIsoDate(start);
  this.migrationTimeFilter.endDate = this.toIsoDate(end);
  this.activeQuickRangePreset = preset;
  this.dateRangeError = null;
  this.triggerLivePreview();
}

private toIsoDate(d: Date): string {
  return d.toISOString().split('T')[0];
}

private getFilterSuffix(): string {
  const f = this.migrationTimeFilter;
  if (f.startDate && f.endDate) {
    return ` (filtered: ${f.startDate} to ${f.endDate})`;
  }
  if (f.startDate) {
    return ` (filtered: ${f.startDate} to today)`;
  }
  return '';
}

get isEligibleForTimeFilter(): boolean {
  const crm = this.sourceSystem?.toLowerCase();
  return crm === 'salesforce' || crm === 'zoho' || crm === 'zendesk' || crm === 'hubspot';
}

  onQueryEdited() {
    this.queryError = null;
    this.isDefaultQuery = false;
    this.scheduleFileMigrationBudgetRecheck();
  }

  private getQueryFieldFilterSet(): Set<string> | null {
    if (!this.restrictMappingToQueryFields) return null;
    const querySelectedFields = this.getQuerySelectedFields();
    if (!querySelectedFields) return null;
    return new Set(querySelectedFields.map((f) => f.toLowerCase()));
  }

  private restrictToQueryFields<T extends { sourceField: string }>(rows: T[]): T[] {
    const fieldSet = this.getQueryFieldFilterSet();
    if (!fieldSet) return rows;
    return rows.filter((r) => fieldSet.has((r.sourceField || '').toLowerCase()));
  }

  get visibleMappings() {
    let filtered = this.mappings;
    const queryFieldSet = this.getQueryFieldFilterSet();
    if (queryFieldSet) {
      filtered = filtered.filter(
        (m) =>
          m.isDropdownOpen ||
          queryFieldSet.has((m.sourceField || '').toLowerCase()) ||
          !!m.targetField 
      );
    }

    if (this.hideMappedFields) {
      filtered = filtered.filter((m) => m.isDropdownOpen || !m.targetField);
    }

    if (this.mappingSearchQuery) {
      const query = this.mappingSearchQuery.toLowerCase().trim();
      filtered = filtered.filter(
        (m) =>
          m.isDropdownOpen ||
          (m.sourceLabel && m.sourceLabel.toLowerCase().includes(query)) || (m.sourceField && m.sourceField.toLowerCase().includes(query))
      );
    }

    return filtered;
  }

  get showMigrationFilter(): boolean {
    return this.isEligibleForTimeFilter && (!this.restrictMappingToQueryFields || this.getQuerySelectedFields() === null);
  }

  get isQueryFieldFilterActive(): boolean {
    return this.getQueryFieldFilterSet() !== null;
  }

  get queryFilteredFieldCount(): number {
    return this.getQuerySelectedFields()?.length ?? 0;
  }

  private getLiveRecordHeaders(): string[] {
    if (!this.previewRecords || this.previewRecords.length === 0) return [];
    const headerSet = new Set<string>();
    const sampleSize = Math.min(this.previewRecords.length, 25); 
    for (let i = 0; i < sampleSize; i++) {
      Object.keys(this.previewRecords[i] || {}).forEach((k) => headerSet.add(k));
    }
    return Array.from(headerSet);
  }

  get visiblePreviewHeaders(): string[] {
    const liveHeaders = this.getLiveRecordHeaders();
    const baseHeaders = liveHeaders.length > 0 ? liveHeaders : this.previewHeaders;

    const fieldSet = this.getQueryFieldFilterSet();
    if (!fieldSet) return baseHeaders;

    const narrowed = baseHeaders.filter((h) => fieldSet.has(h.toLowerCase()));
    return narrowed.length > 0 ? narrowed : baseHeaders;
  }

  getUserData(): void {
  const storedUser = localStorage.getItem('supabase_user');
  if (!storedUser) {
    this.currentUser = null;
    return;
  }
  try {
    this.currentUser = JSON.parse(storedUser);
  } catch (error) {
    console.error('Failed to parse user data from local storage', error);
    this.currentUser = null;
  }
}

toggleProfileDropdown(event: Event): void {
  event.stopPropagation();
  const wasOpen = this.isProfileDropdownOpen;
  this.closeAllDropdowns();
  this.isProfileDropdownOpen = !wasOpen;
}

  changePreviewLimit(newLimit: number) {

    this.previewLimit = Number(newLimit);

    if (this.selectedSourceObject) {
      this.applyFilter();
    }
  }

  getFieldMeta(fieldName: string, side: 'source' | 'target'): FieldMeta | undefined {
    return side === 'source' ? this.sourceFields.find((f) => f.name === fieldName) : this.targetFields.find((f) => f.name === fieldName);
  }

  getFieldTypeBadge(type: string | undefined): string {
    if (!type) return 'bg-secondary';
    const t = type.toLowerCase();
    if (t.includes('string') || t.includes('text') || t.includes('email')) return 'bg-info text-dark';
    if (t.includes('number') || t.includes('int') || t.includes('double') || t.includes('currency')) return 'bg-success';
    if (t.includes('boolean')) return 'bg-warning text-dark';
    if (t.includes('reference') || t.includes('lookup')) return 'bg-primary';
    if (t.includes('date') || t.includes('time')) return 'bg-danger';
    if (t.includes('picklist')) return 'bg-secondary';
    return 'bg-secondary';
  }

  get hasPendingEdits(): boolean {
    if (!this.validationResults?.invalidRecords) return false;

  
    return this.validationResults.invalidRecords.some((rec: any) => rec._editedFields && Object.keys(rec._editedFields).length > 0);
  }

  private static readonly NUMERIC_TYPES = ['number', 'integer', 'double', 'currency', 'percent', 'float'];
  private static readonly DATE_TYPES = ['date', 'datetime'];
  private static readonly TEXT_TARGET_TYPES = ['string', 'text', 'textarea', 'picklist', 'reference'];

  areTypesCompatible(srcTypeRaw: string | undefined, tgtTypeRaw: string | undefined): boolean {
    const srcType = (srcTypeRaw || 'string').toLowerCase();
    const tgtType = (tgtTypeRaw || 'string').toLowerCase();

    if (srcType === 'id' || tgtType === 'id') return true;
    if (srcType === tgtType) return true;
    if (srcType.includes('string') && ApiMappingComponent.TEXT_TARGET_TYPES.includes(tgtType)) return true;
    if (ApiMappingComponent.NUMERIC_TYPES.includes(srcType) && ApiMappingComponent.NUMERIC_TYPES.includes(tgtType)) return true;
    if (ApiMappingComponent.DATE_TYPES.includes(srcType) && ApiMappingComponent.DATE_TYPES.includes(tgtType)) return true;
    return false;
  }

  isTypeMismatch(mapping: any): boolean {
    if (!mapping.targetField) return false;
    return !this.areTypesCompatible(
      this.getFieldMeta(mapping.sourceField, 'source')?.type,
      this.getFieldMeta(mapping.targetField, 'target')?.type
    );
  }

  onEditorInit(editor: any) {
    this.monacoEditorInstance = editor;

    if (!this.completionProvider) {
      // ==========================================
      //  DYNAMIC AUTOCOMPLETE
      // ==========================================
      this.completionProvider = monaco.languages.registerCompletionItemProvider('sql', {
        provideCompletionItems: (model: any, position: any) => {
          const word = model.getWordUntilPosition(position);
          const range = {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn
          };

          const suggestions: any[] = [];
          const crm = this.sourceCrmId.toLowerCase();

          // ------------------------------------
          //  ZENDESK MODE
          // ------------------------------------
          if (crm === 'zendesk') {
            // --- Context-Aware Value Suggestions ---
            const lineContent = model.getLineContent(position.lineNumber);
            const textBeforeCursor = lineContent.substring(0, position.column - 1);

            // 1. If typing a Status
            if (textBeforeCursor.match(/\bstatus:[a-zA-Z]*$/)) {
              const statuses = ['new', 'open', 'pending', 'hold', 'solved', 'closed'];
              statuses.forEach((s) =>
                suggestions.push({
                  label: s,
                  kind: monaco.languages.CompletionItemKind.EnumMember,
                  insertText: s + ' ',
                  range: range,
                  detail: 'Zendesk Ticket Status'
                })
              );
              return { suggestions: suggestions };
            }

            // 2. If typing a Priority
            if (textBeforeCursor.match(/\bpriority:[a-zA-Z]*$/)) {
              const priorities = ['low', 'normal', 'high', 'urgent'];
              priorities.forEach((p) =>
                suggestions.push({
                  label: p,
                  kind: monaco.languages.CompletionItemKind.EnumMember,
                  insertText: p + ' ',
                  range: range,
                  detail: 'Zendesk Ticket Priority'
                })
              );
              return { suggestions: suggestions };
            }

            // 3. If typing a Type
            if (textBeforeCursor.match(/\btype:[a-zA-Z]*$/)) {
              const types = ['ticket', 'user', 'organization', 'group'];
              types.forEach((t) =>
                suggestions.push({
                  label: t,
                  kind: monaco.languages.CompletionItemKind.EnumMember,
                  insertText: t + ' ',
                  range: range,
                  detail: 'Zendesk Record Type'
                })
              );
              return { suggestions: suggestions };
            }

            // --- STANDARD ZENDESK SUGGESTIONS ---

            // 4. Zendesk Smart Snippet
            suggestions.push({
              label: 'Active Tickets (Snippet)',
              kind: monaco.languages.CompletionItemKind.Snippet,
              insertText: `type:ticket status<solved `,
              insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
              documentation: 'Auto-generates a search for all unresolved tickets.',
              range: range
            });

            // 5. Zendesk Keywords
            const zdKeywords = [
              'type:',
              'status:',
              'priority:',
              'tags:',
              'assignee:',
              'requester:',
              'group:',
              'created>',
              'updated>',
              'order_by:',
              'sort:'
            ];
            zdKeywords.forEach((kw) => {
              suggestions.push({
                label: kw,
                kind: monaco.languages.CompletionItemKind.Keyword,
                insertText: kw,
                range: range
              });
            });

            // 6. Zendesk CRM Fields
            if (this.sourceFields && this.sourceFields.length > 0) {
              this.sourceFields.forEach((field) => {
                suggestions.push({
                  label: field.name,
                  detail: `${field.label} (${field.type})`,
                  kind: monaco.languages.CompletionItemKind.Field,
                  insertText: field.name + ':',
                  range: range
                });
              });
            }
          } else if (crm === 'hubspot') {
            suggestions.push({
              label: 'Basic Filter (Snippet)',
              kind: monaco.languages.CompletionItemKind.Snippet,
              insertText: `{\n  "filterGroups": [\n    {\n      "filters": [\n        {\n          "propertyName": "email",\n          "operator": "EQ",\n          "value": ""\n        }\n      ]\n    }\n  ]\n}`,
              insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
              documentation: 'Auto-generates a basic HubSpot JSON filter block.',
              range: range
            });

            const hsOperators = [
              'EQ',
              'NEQ',
              'LT',
              'LTE',
              'GT',
              'GTE',
              'BETWEEN',
              'IN',
              'NOT_IN',
              'HAS_PROPERTY',
              'NOT_HAS_PROPERTY',
              'CONTAINS_TOKEN',
              'NOT_CONTAINS_TOKEN'
            ];
            hsOperators.forEach((op) => {
              suggestions.push({
                label: op,
                kind: monaco.languages.CompletionItemKind.Value,
                insertText: `"${op}"`,
                range: range
              });
            });
          }
          // ------------------------------------
          //  SALESFORCE & ZOHO (SQL) MODE
          // ------------------------------------
          else {
            const isSalesforce = crm === 'salesforce';
            const snippet = monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet;
            const kinds = monaco.languages.CompletionItemKind;

            const queryFields: FieldMeta[] = this.allSourceFields.length > 0 ? this.allSourceFields : this.sourceFields;

            const snippetFields =
              this.sourceFields && this.sourceFields.length > 0
                ? this.sourceFields.slice(0, 15).map((f) => f.name).join(', ')
                : 'Id';

            suggestions.push({
              label: 'SELECT (Basic)',
              kind: kinds.Snippet,
              insertText: `SELECT ${snippetFields} FROM ${this.selectedSourceObject || 'Object'} WHERE \${1:${isSalesforce ? 'Id' : 'id'}} = '\${2:value}'`,
              insertTextRules: snippet,
              documentation: 'Auto-generates a SELECT query using real field names for the current object, with a WHERE ready to fill in.',
              range: range
            });

            // --- Context-aware operators: right after a known field name, offer the operators valid for ITS type ---
            const lineText: string = model.getLineContent(position.lineNumber);
            const beforeWord = lineText.substring(0, word.startColumn - 1);
            const prevToken = beforeWord.match(/([A-Za-z0-9_.]+)\s+$/);
            const prevField = prevToken
              ? queryFields.find((f) => f.name.toLowerCase() === prevToken[1].toLowerCase())
              : undefined;

            if (prevField) {
              const t = (prevField.type || 'string').toLowerCase();
              const isNumeric = ['number', 'integer', 'int', 'double', 'currency', 'percent', 'float', 'long', 'bigint', 'decimal'].includes(t);
              const isDate = t.includes('date') || t.includes('time');
              const isBool = t === 'boolean';

              let ops: string[];
              if (isBool) {
                ops = ['=', '!='];
              } else {
                ops = ['=', '!=', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL'];
                if (isNumeric || isDate) {
                  ops.push('<', '<=', '>', '>=');
                  if (!isSalesforce) ops.push('BETWEEN', 'NOT BETWEEN');
                } else {
                  ops.push('LIKE', 'NOT LIKE');
                  if (!isSalesforce) ops.push('STARTS WITH', 'ENDS WITH', 'CONTAINS', 'NOT CONTAINS');
                  if (isSalesforce && t.includes('multipicklist')) ops.push('INCLUDES', 'EXCLUDES');
                }
              }
              ops.forEach((op) =>
                suggestions.push({
                  label: op,
                  kind: kinds.Operator,
                  insertText: op + ' ',
                  detail: `Operator for ${prevField.name} (${prevField.type || 'string'})`,
                  sortText: '0_' + op,
                  range: range
                })
              );
            }

            // --- Keywords (only things the editor accepts AND the migration can run) ---
            const keywords = [
              'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT',
              'LIKE', 'NOT LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL',
              'NULL', 'TRUE', 'FALSE', 'LIMIT',
              ...(isSalesforce ? ['INCLUDES', 'EXCLUDES'] : ['BETWEEN', 'NOT BETWEEN', 'STARTS WITH', 'ENDS WITH', 'CONTAINS', 'NOT CONTAINS']),
              ...(this.allowOrderBy ? ['ORDER BY', 'ASC', 'DESC'] : [])
            ];
            keywords.forEach((kw) => {
              suggestions.push({ label: kw, kind: kinds.Keyword, insertText: kw + ' ', range: range });
            });

            // --- Value / shape snippets ---
            const snippets: Array<{ label: string; text: string; doc: string }> = [
              { label: "IN ('…', '…')", text: "IN ('${1:value1}', '${2:value2}')", doc: 'Match any value in a list.' },
              { label: "LIKE '%…%'", text: "LIKE '%${1:text}%'", doc: 'Text contains a value.' }
            ];
            if (!isSalesforce) {
              snippets.push(
                { label: 'BETWEEN … AND …', text: 'BETWEEN ${1:min} AND ${2:max}', doc: 'Zoho COQL range filter.' },
                { label: 'Datetime literal (Zoho)', text: "'${1:2026-01-01}T00:00:00+00:00'", doc: 'Zoho COQL datetime format: YYYY-MM-DDThh:mm:ss+hh:mm' }
              );
            }
            snippets.forEach((s) =>
              suggestions.push({ label: s.label, kind: kinds.Snippet, insertText: s.text, insertTextRules: snippet, documentation: s.doc, range: range })
            );

            // --- Salesforce date literals (SOQL only) ---
            if (isSalesforce) {
              ['TODAY', 'YESTERDAY', 'TOMORROW', 'THIS_WEEK', 'LAST_WEEK', 'THIS_MONTH', 'LAST_MONTH',
               'THIS_QUARTER', 'LAST_QUARTER', 'THIS_YEAR', 'LAST_YEAR'].forEach((d) =>
                suggestions.push({ label: d, kind: kinds.Constant, insertText: d + ' ', detail: 'SOQL date literal', range: range })
              );
              ['LAST_N_DAYS', 'NEXT_N_DAYS', 'LAST_N_WEEKS', 'LAST_N_MONTHS', 'LAST_N_YEARS'].forEach((d) =>
                suggestions.push({
                  label: d + ':n',
                  kind: kinds.Constant,
                  insertText: d + ':${1:30}',
                  insertTextRules: snippet,
                  detail: 'SOQL date literal',
                  range: range
                })
              );
            }

            // --- Fields ---
            const seenRelationships = new Set<string>();
            queryFields.forEach((field) => {
              suggestions.push({
                label: field.name,
                detail: `${field.label} (${field.type})`,
                kind: kinds.Field,
                insertText: field.name,
                range: range
              });

              // Parent traversal, e.g. Account.Name / Owner.Email (Salesforce)
              if (isSalesforce && field.relationshipName && !seenRelationships.has(field.relationshipName)) {
                seenRelationships.add(field.relationshipName);
                suggestions.push({
                  label: field.relationshipName + '.',
                  detail: `Parent relationship (${(field.referenceTo || []).join(', ') || 'lookup'})`,
                  kind: kinds.Reference,
                  insertText: field.relationshipName + '.',
                  range: range
                });
              }
            });
          }

          return { suggestions: suggestions };
        }
      });

      // ==========================================
      //  SMART HOVER TOOLTIPS
      // ==========================================
      this.hoverProvider = monaco.languages.registerHoverProvider('sql', {
        provideHover: (model: any, position: any) => {
          const wordInfo = model.getWordAtPosition(position);
          if (!wordInfo) return null;

          const cleanWord = wordInfo.word.replace(/[:<>]/g, '').toLowerCase();

          const field = this.sourceFields.find((f) => f.name.toLowerCase() === cleanWord);

          if (field) {
            return {
              range: new monaco.Range(position.lineNumber, wordInfo.startColumn, position.lineNumber, wordInfo.endColumn),
              contents: [
                { value: `**${field.label}**` },
                { value: `API Name: \`${field.name}\`` },
                { value: `Type: **${field.type || 'String'}** | Required: **${field.isRequired || field.required ? 'Yes' : 'No'}**` }
              ]
            };
          }
          return null;
        }
      });
    }
  }


  injectFieldAtCursor(fieldName: string) {
    if (this.monacoEditorInstance) {
      const position = this.monacoEditorInstance.getPosition();

      this.monacoEditorInstance.executeEdits('custom-inject', [
        {
          range: new monaco.Range(position.lineNumber, position.column, position.lineNumber, position.column),
          text: fieldName + ' ',
          forceMoveMarkers: true
        }
      ]);

      this.monacoEditorInstance.focus();
      this.customQuery = this.monacoEditorInstance.getValue();
    } else {
      if (!this.customQuery) {
        this.customQuery = fieldName;
      } else {
        this.customQuery += ` ${fieldName}`;
      }
    }
    this.validateQuery();
  }

  @HostListener('document:click', ['$event'])
  clickout(event: Event) {
    this.closeAllDropdowns();
  }

  private readonly OPERATION_MODE_LABELS: Record<string, string> = {
    insert: 'Insert',
    update: 'Update',
    upsert: 'Upsert',
    delete: 'Delete'
  };

  async onOperationModeChange(): Promise<void> {
    const newMode = this.operationMode;
    const previousMode = this.lastOperationMode;

    if (newMode === previousMode) {
      if (newMode === 'delete') {
        this.externalIdField = '';
      }
      this.updateMappedCount();
      return;
    }

    const mappedFieldCount = this.mappings.filter((m) => m.targetField !== '').length;

    if (mappedFieldCount === 0) {
      this.lastOperationMode = newMode;
      if (newMode === 'delete') {
        this.externalIdField = '';
      }
      this.updateMappedCount();
      return;
    }

    const previousLabel = this.OPERATION_MODE_LABELS[previousMode] || previousMode;
    const newLabel = this.OPERATION_MODE_LABELS[newMode] || newMode;
    const fieldWord = mappedFieldCount > 1 ? 'fields' : 'field';

    const confirmResult = await Swal.fire({
      title: `Switching to ${newLabel} Mode`,
      html: `
        <div class="text-start px-2">
          <p class="mb-2">
            You have <strong>${mappedFieldCount}</strong> mapped ${fieldWord} carried over from
            <strong>${previousLabel}</strong> mode.
          </p>
          <p class="mb-0">
            Keep using ${mappedFieldCount > 1 ? 'these mappings' : 'this mapping'} for
            <strong>${newLabel}</strong>, or clear everything and remap them from scratch?
          </p>
        </div>
      `,
      icon: 'question',
      showCancelButton: true,
      confirmButtonColor: '#198754',
      cancelButtonColor: '#dc3545',
      confirmButtonText: `Yes, Keep My ${mappedFieldCount} Mapped ${fieldWord.charAt(0).toUpperCase() + fieldWord.slice(1)}`,
      cancelButtonText: 'No, Clear Mapped Fields',
      reverseButtons: true,
      allowOutsideClick: true,
      allowEscapeKey: true,
      customClass: { popup: 'rounded-4 shadow-lg border-0' }
    });

    if (confirmResult.isConfirmed) {
      this.lastOperationMode = newMode;
      if (newMode === 'delete') {
        this.externalIdField = '';
      }

      this.toastr.info(
        `Keeping your mapped ${fieldWord} for ${newLabel} mode. Any without ${newMode === 'insert' ? 'create' : 'edit'} ` +
        `access in ${this.targetSystem} will be auto-unmapped.`,
        'Mappings Retained'
      );
    } else if (confirmResult.dismiss === Swal.DismissReason.cancel) {
      this.lastOperationMode = newMode;
      if (newMode === 'delete') {
        this.externalIdField = '';
      }

      this.mappings.forEach((m) => {
        m.targetField = '';
        m.relationalExtIdField = '';
        delete m._mappedBy;
        delete m._blockedTargetField;
        delete m._blockedTargetLabel;
      });
      this.externalIdField = '';

      this.toastr.info(
        `Cleared ${mappedFieldCount} mapped ${fieldWord}. Map your fields for ${newLabel} mode and validate again.`,
        'Mappings Cleared'
      );
    } else {
      this.operationMode = previousMode;
      this.toastr.info(
        `Mode change cancelled -- staying on ${previousLabel} mode. Your mappings are unchanged.`,
        'Cancelled'
      );
    }

    this.updateMappedCount();
    this.cdr.detectChanges();
  }

  onExternalIdFieldChange(): void {
    this.invalidateValidationOnMappingChange();
    this.cdr.detectChanges();
  }

  onRelationalExtIdFieldChange(mapping: MappingRow): void {
    this.invalidateValidationOnMappingChange();
    this.cdr.detectChanges();
  }

  closeAllDropdowns() {
  this.mappings.forEach((m) => (m.isDropdownOpen = false));
  this.isSourceDropdownOpen = false;
  this.isTargetDropdownOpen = false;
  this.isHistoryDropdownOpen = false;
  this.isProfileDropdownOpen = false;
  this.isMigrationFilterOpen = false;
}

toggleMigrationFilterDropdown(event: Event) {
  event.stopPropagation();
  const wasOpen = this.isMigrationFilterOpen;
  this.closeAllDropdowns();
  this.isMigrationFilterOpen = !wasOpen;
}

onRestrictToQueryFieldsChange(): void {
  if (!this.restrictMappingToQueryFields && this.isEligibleForTimeFilter) {
    this.isMigrationFilterOpen = true;
  }
}


openReviewPanel(): void {
  this.reviewPanelExpanded = false;
  this.reviewPanelMinimized = false;
  this.reviewPanelLeft = Math.max(20, (window.innerWidth - this.reviewPanelDefaultWidth) / 2);
  this.reviewPanelTop = Math.max(20, (window.innerHeight - this.reviewPanelDefaultHeight) / 2);
  this.showReviewPanel = true;
}

startReviewPanelDrag(event: MouseEvent): void {
  if (this.reviewPanelExpanded) return;
  if ((event.target as HTMLElement).closest('button')) return;

  const panelEl = (event.currentTarget as HTMLElement).closest('.floating-review-panel') as HTMLElement;
  if (!panelEl) return;

  const rect = panelEl.getBoundingClientRect();
  this.reviewPanelDragOffset = { x: event.clientX - rect.left, y: event.clientY - rect.top };
  this.reviewPanelDragging = true;
  event.preventDefault();
}

@HostListener('document:mousemove', ['$event'])
onReviewPanelDrag(event: MouseEvent): void {
  if (!this.reviewPanelDragging) return;

  const margin = 60;
  const maxLeft = window.innerWidth - margin;
  const maxTop = window.innerHeight - margin;

  this.reviewPanelLeft = Math.min(Math.max(event.clientX - this.reviewPanelDragOffset.x, 0), maxLeft);
  this.reviewPanelTop = Math.min(Math.max(event.clientY - this.reviewPanelDragOffset.y, 0), maxTop);
}

@HostListener('document:mouseup')
onReviewPanelDragEnd(): void {
  this.reviewPanelDragging = false;
}

  // --- ADD THIS TEMPLATE CONSTANT ---
  readonly ZENDESK_CUSTOM_OBJECT_TEMPLATE = `/* Zendesk Custom Object Query Template 
 - Leave blank to fetch all records.
 - Prefix custom fields with 'custom_object_fields.' 
 - Optional: add a top-level "fields": ["field_a", "field_b"] array to restrict
   which fields appear in the mapping table and get migrated (same role as a
   SOQL/COQL SELECT list). Omit it to keep the full object schema mappable.
*/
{
  "filter": {
    "$and": [
      { "custom_object_fields.your_field_key": { "$eq": "Your Value" } }
    ]
  }
}`;

  readonly HUBSPOT_SEARCH_TEMPLATE = `/* HubSpot Search Filter 
 - Leave blank to fetch all records.
 - Uses HubSpot's JSON Search syntax.
 - Optional: add a top-level "properties": ["field_a", "field_b"] array to
   restrict which fields HubSpot returns and which appear in the mapping table
   (same role as a SOQL/COQL SELECT list). Omit it to keep the full schema.
*/
{
  "filterGroups": [
    {
      "filters": [
        {
          "propertyName": "email",
          "operator": "EQ",
          "value": "test@domain.com"
        }
      ]
    }
  ]
}`;

  buildDefaultQuery(entityName: string) {
    const crm = this.sourceCrmId.toLowerCase();
    this.isDefaultQuery = true;

    if (crm === 'zendesk') {
      if (this.isStandardZendeskObject(entityName)) {
        let singularName = entityName.toLowerCase();
        if (singularName.endsWith('s') && singularName !== 'macros') {
          singularName = singularName.slice(0, -1);
        }
        this.customQuery = `type:${singularName} `;
      } else {
        this.customQuery = this.ZENDESK_CUSTOM_OBJECT_TEMPLATE;
      }
    } else if (crm === 'hubspot') {
      this.customQuery = this.HUBSPOT_SEARCH_TEMPLATE;
    } else if (crm === 'salesforce') {
      this.customQuery = `SELECT Id FROM ${entityName}`;
    } else if (crm === 'zoho') {
      this.customQuery = `SELECT id FROM ${entityName}`;
    } else {
      this.customQuery = `SELECT Id FROM ${entityName} WHERE `;
    }
  }

  toggleHistoryDropdown(event: Event) {
    event.stopPropagation();
    const wasOpen = this.isHistoryDropdownOpen;
    this.closeAllDropdowns();
    this.isHistoryDropdownOpen = !wasOpen;
  }

  toggleDropdown(mapping: any, event: Event) {
    event.stopPropagation();
    const wasOpen = mapping.isDropdownOpen;
    this.closeAllDropdowns();
    mapping.isDropdownOpen = !wasOpen;
    if (mapping.isDropdownOpen) mapping.searchQuery = '';
  }

  isReferenceField(fieldName: string): boolean {
    if (!fieldName) return false;
    const fieldMeta = this.targetFields.find((f) => f.name === fieldName);

    if (!fieldMeta) return false;

    if (fieldMeta.type === 'reference' || (fieldMeta.referenceTo && fieldMeta.referenceTo.length > 0)) {
      return true;
    }

    if (fieldName !== 'Id' && fieldName.endsWith('Id')) {
      return true;
    }

    return false;
  }

  isLookupFieldMeta(field: FieldMeta | undefined | null): boolean {
    if (!field) return false;

    if (field.type === 'reference' || (field.referenceTo && field.referenceTo.length > 0)) {
      return true;
    }

    if (field.idLookup) return true;

    if (field.name && field.name.toLowerCase() !== 'id' && field.name.endsWith('Id')) {
      return true;
    }

    return false;
  }

  isLookupField(fieldName: string | undefined | null, side: 'source' | 'target'): boolean {
    if (!fieldName) return false;
    return this.isLookupFieldMeta(this.getFieldMeta(fieldName, side));
  }

  toggleSyncPicklist(mapping: MappingRow): void {
    if (!this.canSyncPicklist(mapping)) return;
    mapping.syncPicklistValues = !mapping.syncPicklistValues;
  }

  getMissingRequiredFields(): string[] {
    if (this.operationMode === 'delete') return [];
    if (!this.targetFields || this.targetFields.length === 0) return [];

    const requiredFields = this.targetFields.filter((f) => f.isRequired || f.required).map((f) => f.name);

    const mappedFields = this.mappings.filter((m) => m.targetField !== '').map((m) => m.targetField);

    return requiredFields.filter((reqField) => !mappedFields.includes(reqField));
  }

  isFieldWritable(field: FieldMeta | undefined | null, mode: string = this.operationMode): boolean {
    if (!field) return true;
    if (mode === 'delete') return true;
    const flag = mode === 'insert' ? field.createable : field.updateable;
    return flag !== false;
  }

  private isSourceFieldWritable(field: FieldMeta | undefined | null): boolean {
    if (!field) return true;
    if (field.name?.toLowerCase() === 'id' || field.externalId || field.idLookup) return true;
    return field.createable !== false || field.updateable !== false;
  }

  getFieldAccessViolations(): FieldAccessViolation[] {
    if (this.operationMode === 'delete') return [];
    if (!this.targetFields || this.targetFields.length === 0) return [];

    const seen = new Set<string>();
    const violations: FieldAccessViolation[] = [];

    this.mappings.forEach((m) => {
      if (!m.targetField || seen.has(m.targetField)) return;
      const fieldMeta = this.targetFields.find((f) => f.name === m.targetField);
      if (fieldMeta && !this.isFieldWritable(fieldMeta)) {
        seen.add(m.targetField);
        violations.push({ targetField: m.targetField, label: fieldMeta.label });
      }
    });

    return violations;
  }

  getFieldAccessViolationsLabel(): string {
    return this.getFieldAccessViolations().map((v) => v.label).join(', ');
  }

  getIncompleteReferenceMappings(): string[] {
    const incomplete: string[] = [];
    this.mappings.forEach((m) => {
      if (m.targetField && this.isReferenceField(m.targetField) && !m.relationalExtIdField) {
        incomplete.push(m.targetField);
      }
    });
    return incomplete;
  }

  selectField(mapping: any, fieldName: string) {
    if (fieldName) {
      const fieldMeta = this.targetFields.find((f) => f.name === fieldName);
      if (fieldMeta && !this.isFieldWritable(fieldMeta)) {
        this.toastr.error(
          `"${fieldMeta.label}" can't be mapped -- the connected ${this.targetSystem} user doesn't have field-level ` +
          `${this.operationMode === 'insert' ? 'create' : 'edit'} access to it. Choose a different field, or connect ` +
          `with a user that has write access to it.`,
          'No Write Access'
        );
        mapping._blockedTargetField = fieldMeta.name;
        mapping._blockedTargetLabel = fieldMeta.label;
        mapping.isDropdownOpen = false;
        return;
      }
    }

    mapping.targetField = fieldName;
     mapping.syncPicklistValues = false;
    mapping.isDropdownOpen = false;
    delete mapping._mappedBy;
    delete mapping._blockedTargetField;
    delete mapping._blockedTargetLabel;

    if (this.isReferenceField(fieldName)) {
      mapping.relationalExtIdField = 'Id';
      const fieldMeta = this.targetFields.find((f) => f.name === fieldName);
      mapping.parentObjectName = fieldMeta?.referenceTo?.[0];
    } else {
      mapping.relationalExtIdField = undefined;
      mapping.parentObjectName = undefined;
    }

    this.updateMappedCount();
  }

  hasActiveTypeMismatches(): boolean {
    return this.mappings.some((mapping) => mapping.targetField && this.isTypeMismatch(mapping));
  }

  get validateBlockReason(): string | null {
    if (this.getMissingRequiredFields().length > 0) {
      return `Missing required field(s): ${this.getMissingRequiredFields().join(', ')}. Map them before validating.`;
    }
    if (this.getFieldAccessViolations().length > 0) {
      const fieldList = this.getFieldAccessViolationsLabel();
      return `Remove field(s) with no write access before validating: ${fieldList}.`;
    }
    if (this.hasActiveTypeMismatches()) {
      return 'Fix the highlighted data-type mismatches before validating.';
    }
    return null;
  }

  onValidateClick(): void {
    const reason = this.validateBlockReason;
    if (reason) {
      this.toastr.warning(reason, 'Validate Unavailable');
      return;
    }
    this.validateData();
  }

  get runJobBlockReason(): string | null {
    if (this.jobStatus !== 'Validation Passed' && this.jobStatus !== 'Validation Warning') {
      return 'Validate your mapping before running the migration.';
    }
    if (this.hasPendingEdits) {
      return 'You have un-validated fixes in the grid. Click "Re-Validate Fixes" before running the migration.';
    }
    if (this.getMissingRequiredFields().length > 0) {
      return `Missing required field(s): ${this.getMissingRequiredFields().join(', ')}. Map them before running.`;
    }
    if (this.getFieldAccessViolations().length > 0) {
      const fieldList = this.getFieldAccessViolationsLabel();
      return `Remove field(s) with no write access before running: ${fieldList}.`;
    }
    if (this.hasActiveTypeMismatches()) {
      return 'Fix the highlighted data-type mismatches before running.';
    }
    return null;
  }

  onRunJobClick(): void {
    const reason = this.runJobBlockReason;
    if (reason) {
      this.toastr.warning(reason, 'Run Job Unavailable');
      return;
    }
    this.runMigration();
  }

  getFilteredTargetFields(query: string | undefined, sourceFieldName: string): any[] {
    const claimedByOtherRows = new Set(
      this.mappings.filter((m) => m.sourceField !== sourceFieldName && m.targetField).map((m) => m.targetField)
    );
    let filtered = this.targetFields.filter(
      (t) =>
        !claimedByOtherRows.has(t.name) &&
        !this.isSystemManagedField(t.name) &&
        this.isFieldWritable(t)
    );

    if (this.isStrictMapping) {
      const sourceMeta = this.sourceFields.find((f) => f.name === sourceFieldName);

      if (sourceMeta && sourceMeta.type) {
        filtered = filtered.filter((t) => {
          if (sourceMeta.type === 'string' && ['string', 'picklist', 'reference'].includes(t.type || '')) {
            return true;
          }
          return t.type === sourceMeta.type;
        });
      }
    }

    if (query) {
      const lowerQuery = query.toLowerCase();
      filtered = filtered.filter((f) => f.label.toLowerCase().includes(lowerQuery) || f.name.toLowerCase().includes(lowerQuery));
    }

    return filtered;
  }

  getTargetFieldLabel(fieldName: string): string {
    if (!fieldName) return '';
    const field = this.targetFields.find((f) => f.name === fieldName);
    return field ? `${field.label} (${field.name})` : fieldName;
  }

  isExternalIdEligible(field: FieldMeta): boolean {
    const crm = (this.targetCrmId || '').toLowerCase();
    if (crm === 'zoho') {
      return field.name === 'id' || !!field.unique || !!field.externalId;
    }
    if (crm === 'hubspot') {
      return field.name === 'id' || field.name === 'hs_object_id' || !!field.unique || !!field.externalId;
    }
    if (crm === 'zendesk') {
      return !!field.externalId;
    }

    return field.name === 'Id' || !!field.externalId || !!field.idLookup || !!field.unique;
  }

  getExternalIdEligibleFields(): FieldMeta[] {
    if (!this.targetFields) return [];
    return this.targetFields.filter((f) => this.isExternalIdEligible(f));
  }

  // ==========================================================
  // PARENT OBJECT EXTERNAL-ID MATCHING (manual/no-database approach)
  // ==========================================================
  private getAllReferencedParentObjects(): string[] {
    const names = new Set<string>();
    (this.targetFields || []).forEach((f) => {
      const refs = this.normalizeReferenceTo(f.referenceTo);
      if (f.type === 'reference' || refs.length > 0) {
        refs.forEach((r) => names.add(r));
      }
    });
    return Array.from(names);
  }

  private prefetchParentFieldMetadata(): void {
    const isHubspotTarget = (this.targetCrmId || '').toLowerCase() === 'hubspot';
    const parentObjects = this.getAllReferencedParentObjects().filter(
      (n) => !(isHubspotTarget && n.toUpperCase() === 'OWNER')
    );
    this.parentFieldsCache = {};

    if (parentObjects.length === 0) {
      this.isLoadingParentFields = false;
      return;
    }

    this.isLoadingParentFields = true;
    const requests: Record<string, any> = {};
    parentObjects.forEach((name) => {
      requests[name] = this.mappingApi.getFields(this.targetCrmId, name, 'target').pipe(
        catchError((err) => {
          console.warn(`Parent metadata unavailable for ${name}; defaulting to Id matching.`, err);
          return of({ fields: [] });
        })
      );
    });

    forkJoin(requests)
      .pipe(takeUntil(this.mappingCancel$))
      .subscribe({
        next: (results: any) => {
          parentObjects.forEach((name) => {
            this.parentFieldsCache[name] = this.normalizeFields(results[name]?.fields || []);
          });
          this.isLoadingParentFields = false;
          this.cdr.detectChanges();
        },
        error: (err) => {
        if (isAuthExpiredError(err)) { this.handleSessionExpired(getExpiredSidesFromError(err)); return; }
          console.error('Failed to load parent object metadata for External ID matching:', err);
          this.isLoadingParentFields = false;
          this.toastr.warning(
            'Could not load field metadata for one or more referenced objects (e.g. Account) -- ' +
            'the "Match By" list may be incomplete. Reference fields will default to matching by Id.',
            'Reference Metadata Failed'
          );
          this.cdr.detectChanges();
        }
      });
  }

  getReferenceParentCandidates(mapping: MappingRow): string[] {
    const fieldMeta = this.targetFields.find((f) => f.name === mapping.targetField);
    return this.normalizeReferenceTo(fieldMeta?.referenceTo);
  }

  getReferenceParentObjectName(mapping: MappingRow): string | undefined {
    const candidates = this.getReferenceParentCandidates(mapping);
    return mapping.parentObjectName || candidates[0];
  }

  getParentExternalIdFields(mapping: MappingRow): FieldMeta[] {
    const parentName = this.getReferenceParentObjectName(mapping);
    if (!parentName) return [];
    const parentFields = this.parentFieldsCache[parentName] || [];
    return parentFields.filter((f) => this.isExternalIdEligible(f));
  }

  getParentExternalIdOptions(mapping: MappingRow): FieldMeta[] {
    const eligible = this.getParentExternalIdFields(mapping).map((f) => {

      const isTrueExternalId = f.name === 'Id' || !!f.externalId || !!f.idLookup;
      const tag = isTrueExternalId ? '' : ' -- Unique only, not flagged External ID';
      return {
        name: f.name,
        label: `${f.label} (${f.name})${tag}`
      };
    });
    const options: FieldMeta[] = [];

    if (!eligible.some((f) => f.name.toLowerCase() === 'id')) {
      options.push({ name: 'Id', label: 'Id (Record ID)' });
    }
    options.push(...eligible);

    const current = mapping.relationalExtIdField;
    if (current && !options.some((f) => f.name === current)) {
      options.push({ name: current, label: `${current} (current selection)` });
    }

    return options;
  }

  onParentObjectChange(mapping: MappingRow): void {
    mapping.relationalExtIdField = 'Id';
    this.invalidateValidationOnMappingChange();
    this.cdr.detectChanges();
  }

  toggleSourceDropdown(event: Event) {
    event.stopPropagation();
    const wasOpen = this.isSourceDropdownOpen;
    this.closeAllDropdowns();
    this.isSourceDropdownOpen = !wasOpen;
    if (this.isSourceDropdownOpen) this.sourceSearchQuery = '';
  }

  toggleTargetDropdown(event: Event) {
    event.stopPropagation();
    if (!this.selectedSourceObject) return;

    const wasOpen = this.isTargetDropdownOpen;
    this.closeAllDropdowns();
    this.isTargetDropdownOpen = !wasOpen;
    if (this.isTargetDropdownOpen) this.targetSearchQuery = '';
  }

  selectSourceEntity(entityName: string) {
    if (this.selectedSourceObject !== entityName) {
      this.resetMigrationTimeFilter();
      this.fileMigrationBudgetPreview = null;
      this.fileMigrationBudgetPreviewError = null;
    }

    this.selectedSourceObject = entityName;
    this.isSourceDropdownOpen = false;

    this.buildDefaultQuery(entityName);
    this.loadSourceObjectCount(entityName);

    this.loadMetadata();
  }

  selectTargetObject(objName: string) {
    if (this.selectedTargetObject !== objName) {
      this.resetMigrationTimeFilter();
    }

    this.selectedTargetObject = objName;
    this.isTargetDropdownOpen = false;
    this.loadMetadata();
  }

  private resetMigrationTimeFilter(): void {
    this.migrationTimeFilter.startDate = '';
    this.migrationTimeFilter.endDate = '';
    this.migrationTimeFilter.field = this.timeFilterFieldOptions[0]?.value || '';
    this.activeQuickRangePreset = null;
    this.dateRangeError = null;
    this.isMigrationFilterOpen = false;
    this.previewRecords = [];
    this.selectedSourceObjectCount = null;
  }

  private rankEntityMatches(entities: any[], query: string): any[] {
    const q = query.trim().toLowerCase();
    if (!q) return entities;

    const rank = (e: any): number => {
      const name = (e.name || '').toLowerCase();
      const label = (e.label || '').toLowerCase();
      if (name === q || label === q) return 0;
      if (name.startsWith(q) || label.startsWith(q)) return 1;
      return 2;
    };

    return entities
      .filter((e) => (e.label || '').toLowerCase().includes(q) || (e.name || '').toLowerCase().includes(q))
      .sort((a, b) => rank(a) - rank(b) || (a.label || '').localeCompare(b.label || ''));
  }

  getFilteredSourceEntities(): any[] {
    if (!this.sourceSearchQuery) return this.sourceEntities;
    return this.rankEntityMatches(this.sourceEntities, this.sourceSearchQuery);
  }

  getFilteredTargetEntities(): any[] {
    if (!this.targetSearchQuery) return this.targetEntities;
    return this.rankEntityMatches(this.targetEntities, this.targetSearchQuery);
  }

  getSourceEntityLabel(entityName: string): string {
    if (!entityName) return '';
    const entity = this.sourceEntities.find((e) => e.name === entityName);
    return entity ? `${entity.label} (${entity.name})` : entityName;
  }

  getTargetObjectLabel(objName: string): string {
    if (!objName) return '';
    const obj = this.targetEntities.find((e) => e.name === objName);
    return obj ? `${obj.label} (${obj.name})` : objName;
  }

  get paginatedErrorRecords() {
    if (!this.validationResults?.invalidRecords) return [];
    const records = this.validationResults.invalidRecords;
    const start = (this.errorCurrentPage - 1) * this.errorPageSize;
    return records.slice(start, start + this.errorPageSize);
  }

  get errorTotalPages() {
    if (!this.validationResults?.invalidRecords) return 1;
    const total = this.validationResults.invalidRecords.length;
    return Math.ceil(total / this.errorPageSize) || 1;
  }

  get currentErrorMaxBound() {
    if (!this.validationResults?.invalidRecords) return 0;
    const total = this.validationResults.invalidRecords.length;
    return Math.min(this.errorCurrentPage * this.errorPageSize, total);
  }

  nextErrorPage() {
    if (this.errorCurrentPage < this.errorTotalPages) this.errorCurrentPage++;
  }

  prevErrorPage() {
    if (this.errorCurrentPage > 1) this.errorCurrentPage--;
  }

  get queryContext() {
    const crm = this.sourceCrmId.toLowerCase();

    if (crm === 'zendesk') {
      const isCustom = this.selectedSourceObject && !this.isStandardZendeskObject(this.selectedSourceObject);

      return {
        title: isCustom ? 'Zendesk Custom Object Filter' : 'Zendesk Search Filter',
        placeholder: isCustom
          ? '{\n  "filter": {\n    "$and": [\n      { "custom_object_fields.your_field": { "$eq": "value" } }\n    ]\n  }\n}'
          : 'e.g., type:ticket status<solved created>2023-01-01',
        helpText: isCustom
          ? "Use JSON. Prefix custom fields with 'custom_object_fields.'. Leave blank for all records. Add a top-level \"fields\": [...] array to restrict which fields are mappable."
          : 'Use Zendesk native search syntax to filter by tags, status, or dates.',
        icon: 'icon-search',
        buttonText: 'Apply Filter',
        loadingText: 'Filtering...'
      };
    } else if (crm === 'hubspot') {
      return {
        title: 'HubSpot Search Filter',
        placeholder:
          '{\n  "filterGroups": [\n    {\n      "filters": [\n        { "propertyName": "hs_object_id", "operator": "GT", "value": "0" }\n      ]\n    }\n  ]\n}',
        helpText: 'Use HubSpot JSON search syntax to filter records. Leave blank to fetch all. Add a top-level "properties": [...] array to restrict which fields are returned and mappable.',
        icon: 'icon-filter',
        buttonText: 'Apply Filter',
        loadingText: 'Filtering...'
      };
       } else if (crm === 'salesforce') {
      return {
        title: 'SOQL Query Editor',
        placeholder: 'e.g., SELECT Id, Name FROM Account WHERE Amount > 5000 LIMIT 100',
        helpText: 'Write your full SOQL query to filter data, or append LIMIT to restrict the migration size.',
        icon: 'icon-database',
        buttonText: 'Run Query',
        loadingText: 'Querying...'
      };
    } else if (crm === 'zoho') {
      return {
        title: 'Zoho COQL Editor',
        placeholder: "e.g., SELECT id, Account_Name FROM Accounts WHERE Industry = 'Technology' LIMIT 200",
        helpText: "Write your full COQL query. The engine automatically handles Zoho's strict ID rules behind the scenes.",
        icon: 'icon-database',
        buttonText: 'Run Query',
        loadingText: 'Querying...'
      };
    } else {
      return {
        title: `${this.sourceSystem} Query Editor`,
        placeholder: "e.g., status = 'Active' OR created_at > '2024-01-01'",
        helpText: `Enter the specific database query to extract records from ${this.sourceSystem}.`,
        icon: 'icon-terminal',
        buttonText: 'Run Query',
        loadingText: 'Querying...'
      };
    }
  }

  scrollToBottom() {
    const logContainer = document.querySelector('.bg-dark.overflow-auto, .bg-light.overflow-auto, .shadow-inner');
    if (logContainer) {
      logContainer.scrollTop = logContainer.scrollHeight;
    }

    const terminalCard = document.getElementById('execution-terminal');
    if (terminalCard) {
      terminalCard.scrollIntoView({ behavior: 'smooth', block: 'end' });
    }
  }

  async applyFilter() {
    if (!this.customQuery || !this.selectedSourceObject) return;

    if (!this.validateDateRange()) {
      return;
    }

    if (!this.validateQuery()) {
      return;
    }

    this.isPreviewLoading = true;
    this.previewRecords = [];
    this.cdr.detectChanges();
    this.saveQueryToHistory(this.customQuery);

    let safeQuery = this.customQuery.trim();

    if (this.sourceCrmId.toLowerCase() === 'zendesk' && !this.isStandardZendeskObject(this.selectedSourceObject)) {
      safeQuery = safeQuery.replace(/\/\*[\s\S]*?\*\//g, '').trim();

      const cleanBlankTemplate = this.ZENDESK_CUSTOM_OBJECT_TEMPLATE.replace(/\/\*[\s\S]*?\*\//g, '').trim();
      if (safeQuery === cleanBlankTemplate) {
        safeQuery = '';
      }
    }

    if (this.sourceCrmId.toLowerCase() === 'hubspot') {
      safeQuery = safeQuery.replace(/\/\*[\s\S]*?\*\//g, '').trim();
      const cleanBlankTemplate = this.HUBSPOT_SEARCH_TEMPLATE.replace(/\/\*[\s\S]*?\*\//g, '').trim();
      if (safeQuery === cleanBlankTemplate) {
        safeQuery = '';
      }
    }

    const payload = {
      crmId: this.sourceCrmId,
      objectName: this.selectedSourceObject,
      query: safeQuery,
      headers: this.previewHeaders,
      limit: this.previewLimit,
      authToken: localStorage.getItem('supabase_token') || '',
      migrationTimeFilter: this.migrationTimeFilter
    };

    try {
      const response = await fetch(`${environment.apiUrl}/api/metadata/preview-filter`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${localStorage.getItem('supabase_token') || ''}`
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: 'Unknown Server Error' }));
        if (isAuthExpiredError({ status: response.status, error: errorData }, true)) {
          this.handleSessionExpired(getExpiredSidesFromError({ error: errorData }));
          return;
        }
        throw new Error(errorData.detail || 'Failed to fetch filtered data.');
      }

            const data = await response.json();
      this.previewRecords = data.records || [];
      this.loadSourceObjectCount(this.selectedSourceObject, safeQuery, this.migrationTimeFilter);
      const filterSuffix = this.getFilterSuffix();
      const executedQuery = data.queryUsed || safeQuery || 'default query';
      this.logMessages = [...this.logMessages, `System: Source preview updated${filterSuffix} -> [${executedQuery}]`];
    } catch (error: any) {
      console.error('Filter Error:', error);
      this.previewRecords = [];

      const errorMessage = error.message || 'Invalid Query Syntax rejected by CRM.';
      this.queryError = `API Error: ${errorMessage}`;

      this.toastr.error('Your query was rejected by the source CRM.', 'Query Error');
      this.logMessages = [...this.logMessages, ` API Error: ${errorMessage}`];
    } finally {
      this.isPreviewLoading = false;
      this.cdr.detectChanges();
    }
  }

  async loadSourceObjectCount(objectName: string, query: string = '', timeFilter: { field: string; startDate: string; endDate: string; utcOffsetMinutes: number } | null = null) {
    if (!objectName || !this.sourceCrmId) {
      this.selectedSourceObjectCount = null;
      return;
    }

    this.isSourceCountLoading = true;
    this.cdr.detectChanges();

    try {
      const params = new URLSearchParams({ role: 'source' });
      if (query) params.set('query', query);
      if (timeFilter && timeFilter.startDate) {
        params.set('timeFilter', JSON.stringify(timeFilter));
      }

      const response = await fetch(
        `${environment.apiUrl}/api/metadata/${this.sourceCrmId}/count/${encodeURIComponent(objectName)}?${params.toString()}`,
        { headers: { Authorization: `Bearer ${localStorage.getItem('supabase_token') || ''}` } }
      );

      if (!response.ok) {
        const errBody = await response.json().catch(() => ({}));
        if (isAuthExpiredError({ status: response.status, error: errBody }, true)) {
          this.handleSessionExpired(getExpiredSidesFromError({ error: errBody }));
          return;
        }
        throw new Error('Failed to fetch object count.');
      }

      const data = await response.json();
      this.selectedSourceObjectCount = typeof data.count === 'number' ? data.count : null;
    } catch (error) {
      console.error('Object Count Error:', error);
      this.selectedSourceObjectCount = null;
    } finally {
      this.isSourceCountLoading = false;
      this.cdr.detectChanges();
    }
  }

  private logError(message: string) {
    this.logMessages = [...this.logMessages, message];
  }

  private logWarning(message: string) {
    this.logMessages = [...this.logMessages, message];
  }

  private normalizeReferenceTo(raw: any): string[] {
    if (!raw) return [];
    const items = Array.isArray(raw) ? raw : [raw];
    return items
      .map((r: any) =>
        typeof r === 'string' ? r : (r?.module?.api_name || r?.api_name || r?.name || r?.apiName || '')
      )
      .filter((r: string) => !!r);
  }

  private normalizeFields(fields: any[]): FieldMeta[] {
    return (fields || []).map((f) => ({ ...f, referenceTo: this.normalizeReferenceTo(f.referenceTo) }));
  }

  validateQuery(): boolean {
    this.queryError = null;

    if (this.monacoEditorInstance) {
      monaco.editor.setModelMarkers(this.monacoEditorInstance.getModel(), 'sql-validation', []);
    }

    if (!this.customQuery) return true;

    const queryLower = this.customQuery.trim().toLowerCase();
    const crm = this.sourceCrmId.toLowerCase();


    const applySquiggle = (errorMsg: string, offendingText: string): boolean => {
      this.queryError = errorMsg;

      if (this.monacoEditorInstance && offendingText) {
        const model = this.monacoEditorInstance.getModel();
        const fullText = model.getValue().toLowerCase();

        const startIndex = fullText.indexOf(offendingText.toLowerCase());

        if (startIndex !== -1) {
          const startPos = model.getPositionAt(startIndex);
          const endPos = model.getPositionAt(startIndex + offendingText.length);

          monaco.editor.setModelMarkers(model, 'sql-validation', [
            {
              startLineNumber: startPos.lineNumber,
              startColumn: startPos.column,
              endLineNumber: endPos.lineNumber,
              endColumn: endPos.column,
              message: errorMsg,
              severity: monaco.MarkerSeverity.Error
            }
          ]);
        }
      }
      return false;
    };

    // ==========================================
    // ZENDESK STRICT TOKEN VALIDATION
    // ==========================================
    if (crm === 'zendesk') {
      const isStandard = this.isStandardZendeskObject(this.selectedSourceObject);

      if (!isStandard) {
        // --- CUSTOM OBJECT VALIDATION ---
        const cleanJsonText = this.customQuery.replace(/\/\*[\s\S]*?\*\//g, '').trim();

        if (cleanJsonText === '') return true;

        // 2. Must be JSON
        if (!cleanJsonText.startsWith('{')) {
          return applySquiggle('Zendesk Custom Objects require a valid JSON filter block.', this.customQuery.trim().split(' ')[0] || ' ');
        }
        // 3. Must be VALID JSON syntax
        try {
          JSON.parse(cleanJsonText);
        } catch (e) {
          return applySquiggle('Invalid JSON Syntax. Please check your brackets, keys, and trailing commas.', '{');
        }
        return true;
      }

      // --- STANDARD OBJECT VALIDATION (Text Search) ---
      if (queryLower.startsWith('select ') || queryLower.includes(' from ')) {
        return applySquiggle("Zendesk doesn't support SQL. Format: type:ticket status<solved", 'select');
      } else if (queryLower.includes(',')) {
        return applySquiggle('Do not use commas. Format: status:open tags:urgent', ',');
      } else if (queryLower.includes(' = ')) {
        return applySquiggle('Use colons for exact matches. Format: status:open', '=');
      }

      const tokens = queryLower.split(/\s+/);
      const zendeskSystemFields = [
        'type',
        'tags',
        'status',
        'priority',
        'group_id',
        'assignee_id',
        'requester_id',
        'submitter_id',
        'organization_id',
        'created',
        'updated',
        'order_by',
        'sort'
      ];

      for (const token of tokens) {
        const match = token.match(/^(-)?([a-zA-Z0-9_]+)[:<>](.*)$/);
        if (match) {
          const fieldName = match[2];
          if (!zendeskSystemFields.includes(fieldName)) {
            const schemaField = this.sourceFields.find((f) => f.name.toLowerCase() === fieldName);
            if (!schemaField) {
              return applySquiggle(`Invalid Zendesk Field: '${fieldName}' does not exist.`, fieldName);
            }
          }
        } else if (token.length > 0 && !token.includes('*') && !token.includes('-')) {
          return applySquiggle(`Invalid Syntax: '${token}' is not formatted correctly.`, token);
        }
      }
    }

    // ==========================================
    // HUBSPOT JSON VALIDATION
    // ==========================================
    else if (crm === 'hubspot') {
      const cleanJsonText = this.customQuery.replace(/\/\*[\s\S]*?\*\//g, '').trim();

      if (cleanJsonText === '') return true; // Blank is fine (fetches all)

      if (!cleanJsonText.startsWith('{')) {
        return applySquiggle('HubSpot filters require a valid JSON block.', this.customQuery.trim().split(' ')[0] || ' ');
      }
      try {
        JSON.parse(cleanJsonText);
      } catch (e) {
        return applySquiggle('Invalid JSON Syntax. Please check your brackets, keys, and trailing commas.', '{');
      }
      return true;
    }

    // ==========================================
    //  SALESFORCE & ZOHO SQL VALIDATION
    // ==========================================
    else if (crm === 'salesforce' || crm === 'zoho') {
      // Multi-line pasted queries ("SELECT\n Id,\n Name\nFROM Account") must validate like single-line ones.
      const sqlLower = this.normalizeSql(this.customQuery).toLowerCase();

      if (this.customQuery.replace(/'(?:[^'\\]|\\.)*'/g, "''").includes('"')) {
        return applySquiggle('Salesforce strictly requires single quotes (\') for text values. Do not use double quotes (").', '"');
      }

      if (!this.allowOrderBy && /\border by\b/.test(sqlLower)) {
        return applySquiggle('Do not use ORDER BY.', 'order by');
      } else if (sqlLower.endsWith(';')) {
        return applySquiggle('Do not end your query with a semicolon (;).', ';');
      }

      const hasSelect = sqlLower.startsWith('select ');
      const hasWhere = sqlLower.includes(' where ');
      let conditionPart = '';

      if (hasSelect) {
        if (hasWhere) {
          conditionPart = sqlLower.split(' where ')[1];
        }
      } else {
        conditionPart = sqlLower;
      }

      if (conditionPart.trim() !== '') {
        const hasOperator =
          /(=|<|>)/.test(conditionPart) ||
          /\b(like|in|includes|excludes|between|starts\s+with|ends\s+with|contains)\b/.test(conditionPart) ||
          /\bis\s+(not\s+)?null\b/.test(conditionPart) ||
          conditionPart.includes(' is ');

        if (!hasOperator) {
          return applySquiggle(
            'Invalid Syntax: Missing SQL operators (e.g., =, <, >, LIKE, IN, BETWEEN).',
            conditionPart.trim().split(' ')[0]
          );
        }
      }

      if (hasSelect) {
        const objPart = this.getTopLevelFromObject(sqlLower);
        if (objPart && objPart.toLowerCase() !== this.selectedSourceObject.toLowerCase()) {
          return applySquiggle(
            `Object Mismatch: You selected '${this.selectedSourceObject}', but your query says FROM '${objPart}'.`,
            objPart
          );
        }
      }

      const sqlRegex = /\b([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)*)(?:\s*(?:<>|!=|<=|>=|=|<|>)|\s+(?:like|is(?:\s+not)?))\s*('(?:[^'\\]|\\.)*'|[^\s()',]+)/gi;
      const reservedWords = new Set(['select', 'from', 'where', 'and', 'or', 'null', 'is', 'like', 'not']);
      const numericTypes = new Set(['number', 'currency', 'double', 'int', 'integer', 'percent', 'long', 'bigint', 'decimal', 'float']);

      // Skip until describe metadata has loaded, otherwise every field would be flagged invalid.
      if (this.allSourceFields.length > 0) {
        let match: RegExpExecArray | null;

        while ((match = sqlRegex.exec(this.customQuery)) !== null) {
          const rawPath = match[1];
          if (reservedWords.has(rawPath.toLowerCase())) continue;

          if (rawPath.includes('.')) continue;

          const schemaField = this.allSourceFields.find((f) => f.name.toLowerCase() === rawPath.toLowerCase());

          if (!schemaField) {
            return applySquiggle(`Invalid Field: '${rawPath}' does not exist on ${this.selectedSourceObject}.`, rawPath);
          }

          const val = match[2].replace(/^'|'$/g, '').trim();
          const type = schemaField.type?.toLowerCase() || 'string';

          if (val.includes('*') || val.includes('%') || val.toLowerCase() === 'null') continue;

          if (numericTypes.has(type) && isNaN(Number(val))) {
            return applySquiggle(`Type Mismatch: '${rawPath}' is a Number, but you entered text ('${val}').`, match[2]);
          }

          if (type === 'boolean' && !['true', 'false', '1', '0'].includes(val.toLowerCase())) {
            return applySquiggle(`Type Mismatch: '${rawPath}' is a Boolean. You entered '${val}'.`, match[2]);
          }
        }
      }
    }

    return this.queryError === null;
  }

  loadHistoricalQuery(query: string) {
    this.customQuery = query;
    this.isDefaultQuery = false;
    this.isHistoryDropdownOpen = false;
    this.validateQuery();
  }

  saveQueryToHistory(query: string) {
    if (!query) return;
    this.recentQueries = this.recentQueries.filter((q) => q !== query);
    this.recentQueries.unshift(query);
    if (this.recentQueries.length > 5) this.recentQueries.pop();

    localStorage.setItem('crm_query_history', JSON.stringify(this.recentQueries));
  }

  preloadEntirePage() {
    this.isLoading = true;
    this.isGlobalLoading = true;
    this.globalLoadingText = 'Fetching Live Schemas...';
    this.globalLoadingSubText = 'Please wait while we sync the CRM engines';
    this.showSchemaLoader();
    this.cdr.detectChanges();

    forkJoin({
      sourceObjs: this.mappingApi.getObjects(this.sourceCrmId, 'source'),
      targetObjs: this.mappingApi.getObjects(this.targetCrmId, 'target')
    })
    .pipe(takeUntil(this.mappingCancel$))
    .subscribe({
      next: ({ sourceObjs, targetObjs }) => {
        this.sourceEntities = sourceObjs || [];
        this.targetEntities = targetObjs || [];

        if (this.sourceEntities.length > 0) {
          const defaultSrc = this.sourceEntities.find(
            (e) =>
              e.name.toLowerCase().includes('account') ||
              e.name.toLowerCase().includes('ticket') ||
              e.name.toLowerCase().includes('contacts')
          );
          this.selectedSourceObject = defaultSrc ? defaultSrc.name : this.sourceEntities[0].name;

          this.buildDefaultQuery(this.selectedSourceObject);
          this.loadSourceObjectCount(this.selectedSourceObject);
        }

        if (this.targetEntities.length > 0) {
          const defaultTgt = this.targetEntities.find(
            (e) =>
              e.name.toLowerCase().includes('account') || e.name.toLowerCase().includes('user') || e.name.toLowerCase().includes('contacts')
          );
          this.selectedTargetObject = defaultTgt ? defaultTgt.name : this.targetEntities[0].name;
        }

        this.cdr.detectChanges();
        this.loadMetadata();

        this.isGlobalLoading = false;
        this.cdr.detectChanges();
        this.openPendingRecovery();
      },
      error: (err) => {
        if (isAuthExpiredError(err)) { this.handleSessionExpired(getExpiredSidesFromError(err)); return; }
        console.error('Failed to preload base entity dropdowns:', err);
        this.logMessages.unshift(`Configuration Error: Could not fetch core CRM schemas.`);
        this.isLoading = false;
        this.isGlobalLoading = false;
        this.cdr.detectChanges();
        this.openPendingRecovery();
      }
    });
  }

  insertFieldIntoQuery(event: Event) {
    const selectElement = event.target as HTMLSelectElement;
    const fieldName = selectElement.value;

    if (!fieldName) return;

    this.injectFieldAtCursor(fieldName);

    selectElement.value = '';
  }

  loadMetadata() {
    if (!this.selectedSourceObject || !this.selectedTargetObject) {
      this.isLoading = false;
      this.cdr.detectChanges();
      return;
    }
    this.cancelPendingMappingWork();

    this.isLoading = true;
    this.cdr.detectChanges();

    forkJoin({
      sourceData: this.mappingApi.getFields(this.sourceCrmId, this.selectedSourceObject, 'source'),
      targetData: this.mappingApi.getFields(this.targetCrmId, this.selectedTargetObject, 'target')
    })
      .pipe(takeUntil(this.mappingCancel$))
      .subscribe({
        next: ({ sourceData, targetData }) => {
          this.targetFields = this.normalizeFields(targetData.fields || []);
          this.allSourceFields = this.normalizeFields(sourceData.fields || []);
          this.sourceFields = this.allSourceFields.filter(
            (field: FieldMeta) => this.isSourceFieldWritable(field) && !this.isSystemManagedField(field.name)
          );
          this.prefetchParentFieldMetadata();

          this.previewHeaders = sourceData.headers || [];
          this.previewRecords = sourceData.sampleRecords || [];

          const crmLower = this.sourceCrmId.toLowerCase();
          if (this.isDefaultQuery && this.previewHeaders.length > 0) {
            const defaultFields = this.previewHeaders.slice(0, 2);

            if (crmLower === 'salesforce' || crmLower === 'zoho') {
              this.customQuery = `SELECT ${defaultFields.join(', ')} FROM ${this.selectedSourceObject}`;
            } else if (crmLower === 'hubspot') {
              this.customQuery = JSON.stringify({ properties: defaultFields }, null, 2);
            } else if (crmLower === 'zendesk' && !this.isStandardZendeskObject(this.selectedSourceObject)) {
              this.customQuery = JSON.stringify({ fields: defaultFields }, null, 2);
            }
          }

          this.mappings = this.sourceFields.map((field: FieldMeta) => ({
            sourceField: field.name,
            sourceLabel: `${field.label} (${field.name})`,
            targetField: ''
          }));

          this.showReviewPanel = false;
          this.reviewPanelMinimized = false;
          this.reviewFilter = 'mapped';
          this.mappingSearchQuery = '';

  
          this.externalIdField = '';
          this.jobStatus = 'Idle';
          this.validationResults = null;
          this.aggregateStats = { total: 0, valid: 0, invalid: 0, duplicates: 0 };
          this.currentSessionId = '';
          this.successData = [];
          this.errorData = [];
          this.skippedData = [];
          this.isValidating = false;

          this.updateMappedCount();
          this.isLoading = false;
          this.cdr.detectChanges();

          if (this.customQuery && this.previewHeaders.length > 0) {
            this.applyFilter();
          }
        },
        error: (err) => {
        if (isAuthExpiredError(err)) { this.handleSessionExpired(getExpiredSidesFromError(err)); return; }
          console.error('Metadata payload extraction failed:', err);
          this.logMessages.unshift(`API Error: Unable to fetch live dataset metrics from ${this.sourceSystem}.`);
          this.isLoading = false;
          this.cdr.detectChanges();
        }
      });
  }

  
  private cancelPendingMappingWork(): void {
    this.mappingCancel$.next();

    this.isAutoMapping = false;
    this.autoMapProgress = { current: 0, total: 0 };
    this.mappings.forEach((m) => (m._isAiProcessing = false));
  }

  clearMapping(mapping: any) {
    mapping.targetField = '';
     mapping.syncPicklistValues = false;
    mapping.relationalExtIdField = '';
    delete mapping._mappedBy;
    delete mapping._blockedTargetField;
    delete mapping._blockedTargetLabel;
    this.updateMappedCount();
  }

  removeMapping(mapping: any) {
    this.mappings = this.mappings.filter((m) => m !== mapping);
    this.updateMappedCount();
  }

  resetAllMappings() {
    this.mappings.forEach((m) => {
      m.syncPicklistValues = false;
      m.targetField = '';
      m.relationalExtIdField = '';
      delete m._mappedBy;
      delete m._blockedTargetField;
      delete m._blockedTargetLabel;
    });

    this.updateMappedCount();
  }

  autoMap(): void {
    if (this.isAutoMapping || this.isLoading) return;

    this.isAutoMapping = true;

    let heuristicMatchCount = 0;

    // =========================================================
    // PHASE 1: SYNCHRONOUS LOCAL TEXT MATCHING
    // =========================================================
    const claimedTargetFields = new Set<string>(this.mappings.filter((m) => m.targetField).map((m) => m.targetField));
    const queryFieldSet = this.getQueryFieldFilterSet(); 

    this.mappings.forEach((m) => {
      if (m.targetField) return;
      if (queryFieldSet && !queryFieldSet.has((m.sourceField || '').toLowerCase())) return;

      const sourceMeta = this.sourceFields.find((sf) => sf.name === m.sourceField);
      if (!sourceMeta) return;

      const srcApiExact = sourceMeta.name.toLowerCase();
      const srcLabelExact = sourceMeta.label.toLowerCase();

      const srcApiClean = srcApiExact
        .replace(/__c$/, '')
        .replace(/__r$/, '')
        .replace(/[^a-z0-9]/g, '');
      const srcLabelClean = srcLabelExact.replace(/[^a-z0-9]/g, '');
      const srcType = (sourceMeta.type || 'string').toLowerCase();

      let bestMatch: any = null;
      let highestScore = 0;
      let bestBlockedMatch: any = null;
      let highestBlockedScore = 0;

      this.targetFields.forEach((t) => {
        const tgtApiExact = t.name.toLowerCase();
        if (this.isSystemManagedField(tgtApiExact)) return;
        if (claimedTargetFields.has(t.name)) return;
        const fieldIsWritable = this.isFieldWritable(t);

        let score = 0;
        const tgtLabelExact = t.label.toLowerCase();

        const tgtApiClean = tgtApiExact
          .replace(/__c$/, '')
          .replace(/__r$/, '')
          .replace(/[^a-z0-9]/g, '');
        const tgtLabelClean = tgtLabelExact.replace(/[^a-z0-9]/g, '');
        const tgtType = (t.type || 'string').toLowerCase();

        if (srcType === 'reference' || tgtType === 'reference' || srcType === 'id' || tgtType === 'id') {
          return;
        }

        const isExactTypeMatch = srcType === tgtType;
        const isCompatible = this.areTypesCompatible(srcType, tgtType);
        const isForgivingTypeMatch = isCompatible && !isExactTypeMatch;

        if (!isCompatible) return;

        if (tgtApiExact === srcApiExact) score += 100;
        else if (tgtApiClean === srcApiClean) score += 90;
        else if (tgtLabelExact === srcLabelExact) score += 85;
        else if (tgtLabelClean === srcLabelClean) score += 75;
        else if (srcApiClean.length > 3 && (tgtApiClean.includes(srcApiClean) || srcApiClean.includes(tgtApiClean))) score += 40;
        else if (srcLabelClean.length > 3 && (tgtLabelClean.includes(srcLabelClean) || srcLabelClean.includes(tgtLabelClean))) score += 30;

        if (score >= 30) {
          if (isExactTypeMatch) score += 20;
          else if (isForgivingTypeMatch) score += 10;
        }

        if (score > highestScore && score >= 50 && fieldIsWritable) {
          highestScore = score;
          bestMatch = t;
        } else if (score > highestBlockedScore && score >= 50 && !fieldIsWritable) {
          highestBlockedScore = score;
          bestBlockedMatch = t;
        }
      });

      if (bestMatch) {
        m.targetField = bestMatch['name'];
        claimedTargetFields.add(bestMatch['name']);
        if (typeof this.isReferenceField === 'function' && this.isReferenceField(bestMatch['name'])) {
          m.relationalExtIdField = 'Id';
        }
        m._mappedBy = 'rule';
        delete m._blockedTargetField;
        delete m._blockedTargetLabel;
        heuristicMatchCount++;
      } else if (bestBlockedMatch) {
        m._blockedTargetField = bestBlockedMatch['name'];
        m._blockedTargetLabel = bestBlockedMatch['label'];
      }
    });

    const unmappedSourcePayload = this.sourceFields.filter((sf) => {
      if (queryFieldSet && !queryFieldSet.has(sf.name.toLowerCase())) return false;
      const activeMap = this.mappings.find((m) => m.sourceField === sf.name);
      return activeMap && !activeMap.targetField;
    });

    if (unmappedSourcePayload.length === 0) {
      this.mappings = [...this.mappings];
      this.cdr.detectChanges();
      if (typeof this.updateMappedCount === 'function') this.updateMappedCount();

      this.isAutoMapping = false;

      if (heuristicMatchCount > 0) {
        if (this.toastr) this.toastr.success(`Intelligently aligned ${heuristicMatchCount} fields!`, 'Auto-Map Complete');
        if (this.logMessages) this.logMessages.unshift(`System: Fast-pass local mapping matched ${heuristicMatchCount} fields.`);

        this.reviewFilter = 'mapped';
        this.openReviewPanel();
      } else {
        if (this.toastr) this.toastr.info(`No matches found.`, 'Auto-Map Finished');
      }
      return;
    }

    // =========================================================
    // PHASE 2 & 3: UI SETUP AND AI FETCH
    // =========================================================
    setTimeout(() => {
      this.isAutoMapping = true;
      this.autoMapProgress = { current: 0, total: unmappedSourcePayload.length };

      // Turn on inline spinners
      unmappedSourcePayload.forEach((field) => {
        const mappingRow = this.mappings.find((m) => m.sourceField === field.name);
        if (mappingRow) mappingRow._isAiProcessing = true;
      });

      this.mappings = [...this.mappings];

      const CHUNK_SIZE = 50;
      let currentIndex = 0;
      let aiMatchCount = 0;

      const processNextChunk = () => {
        if (currentIndex >= unmappedSourcePayload.length) {
          setTimeout(() => {
            this.isAutoMapping = false;
            const totalMapped = heuristicMatchCount + aiMatchCount;

            if (totalMapped > 0) {
              if (this.toastr) this.toastr.success(`Successfully mapped ${totalMapped} fields!`, 'Hybrid Auto-Map Complete');
              if (this.logMessages) this.logMessages.unshift(`System: Hybrid Auto-mapping complete.`);

              this.reviewFilter = 'mapped';
            } else {
              if (this.toastr) this.toastr.info(`No matches found by rules or AI.`, 'Auto-Map Finished');

              this.reviewFilter = 'unmapped';
            }

            this.mappings = [...this.mappings];
            if (typeof this.updateMappedCount === 'function') this.updateMappedCount();
            this.openReviewPanel();
            this.cdr.detectChanges();
          }, 600);

          return;
        }

        const currentChunk = unmappedSourcePayload.slice(currentIndex, currentIndex + CHUNK_SIZE);
        const targetFieldsAtRequestTime = new Set(this.targetFields.map((t) => t.name));

        this.mappingApi
          .getAiAutoMapping(currentChunk, this.targetFields)
          .pipe(takeUntil(this.mappingCancel$))
          .subscribe({
          next: (response: any) => {
            if (response && Array.isArray(response.mappings)) {
              response.mappings.forEach((backendMap: any) => {
                const localRow = this.mappings.find((m) => m.sourceField === backendMap.sourceField);
                const targetAlreadyClaimed = this.mappings.some(
                  (m) => m.sourceField !== backendMap.sourceField && m.targetField === backendMap.targetField
                );
 
                const isStillValidTarget = backendMap.targetField && targetFieldsAtRequestTime.has(backendMap.targetField);
                const suggestedFieldMeta = backendMap.targetField
                  ? this.targetFields.find((f) => f.name === backendMap.targetField)
                  : undefined;
                const isWritable = this.isFieldWritable(suggestedFieldMeta);
                const sourceFieldMeta = this.sourceFields.find((sf) => sf.name === backendMap.sourceField);
                const typeCompatible =
                  !suggestedFieldMeta || !sourceFieldMeta || this.areTypesCompatible(sourceFieldMeta.type, suggestedFieldMeta.type);

                if (localRow && !localRow.targetField && isStillValidTarget && !targetAlreadyClaimed && isWritable && typeCompatible) {
                  localRow.targetField = backendMap.targetField;

                  if (typeof this.isReferenceField === 'function' && this.isReferenceField(backendMap.targetField)) {
                    localRow.relationalExtIdField = 'Id';
                  }
                  localRow._mappedBy = 'ai';
                  delete localRow._blockedTargetField;
                  delete localRow._blockedTargetLabel;
                  aiMatchCount++;
                } else if (
                  localRow && !localRow.targetField && isStillValidTarget && !targetAlreadyClaimed &&
                  !isWritable && typeCompatible && suggestedFieldMeta
                ) {
                  localRow._blockedTargetField = suggestedFieldMeta.name;
                  localRow._blockedTargetLabel = suggestedFieldMeta.label;
                }
              });
            }

            currentChunk.forEach((field) => {
              const mappingRow = this.mappings.find((m) => m.sourceField === field.name);
              if (mappingRow) mappingRow._isAiProcessing = false;
            });

            this.autoMapProgress.current += currentChunk.length;
            this.mappings = [...this.mappings];
            this.cdr.detectChanges();

            if (typeof this.updateMappedCount === 'function') this.updateMappedCount();

            currentIndex += CHUNK_SIZE;
            processNextChunk();
          },
          error: (error: any) => {
        if (isAuthExpiredError(error)) { this.handleSessionExpired(getExpiredSidesFromError(error)); return; }
            console.error('[FRONTEND ERROR]: AI Chunk failed:', error);
            this.isAutoMapping = false;

            this.mappings.forEach((m) => (m._isAiProcessing = false));
            this.mappings = [...this.mappings];

            if (typeof this.updateMappedCount === 'function') this.updateMappedCount();
            this.reviewFilter = heuristicMatchCount > 0 ? 'mapped' : 'unmapped';
            this.openReviewPanel();
            this.cdr.detectChanges();

            if (this.toastr) {
              if (error.status === 404) {
                this.toastr.error('Backend endpoint not found (404).', 'Connection Error');
              } else if (heuristicMatchCount > 0) {
                this.toastr.warning(
                  `The SureShift Agent connection was interrupted, but ${heuristicMatchCount} field(s) matched by rules were kept.`,
                  'Incomplete'
                );
              } else {
                this.toastr.warning('The SureShift Agent connection was interrupted. Partial mappings saved.', 'Incomplete');
              }
            }
          }
        });
      };

      processNextChunk();
    });
  }

  private readonly VALIDATION_LIVE_STATUSES = new Set(['Idle', 'Connecting...', 'Re-validating...', 'Initializing...']);

  private invalidateValidationOnMappingChange(): void {
    if (this.isValidating || this.VALIDATION_LIVE_STATUSES.has(this.jobStatus)) return;

    this.jobStatus = 'Idle';
    this.validationResults = null;
    this.aggregateStats = { total: 0, valid: 0, invalid: 0, duplicates: 0 };
    this.currentSessionId = '';
    this.successData = [];
    this.errorData = [];
    this.skippedData = [];

    this.toastr.info(
      'Your mapping changed since the last validation run -- validate again before running the migration.',
      'Re-Validation Required'
    );
  }

  updateMappedCount() {
    this.invalidateValidationOnMappingChange();

    const validTargetFieldNames = new Set(this.targetFields.map((f) => f.name));
    const autoUnmappedLabels: string[] = [];

    this.mappings.forEach((m) => {
      if (!m.targetField) return;

      if (!validTargetFieldNames.has(m.targetField)) {
        m.targetField = '';
        m.relationalExtIdField = '';
        delete m._mappedBy;
        return;
      }

      const fieldMeta = this.targetFields.find((f) => f.name === m.targetField);
      if (fieldMeta && !this.isFieldWritable(fieldMeta)) {
        autoUnmappedLabels.push(fieldMeta.label);
        m.targetField = '';
        m.relationalExtIdField = '';
        delete m._mappedBy;
        m._blockedTargetField = fieldMeta.name;
        m._blockedTargetLabel = fieldMeta.label;
      }
    });

    if (autoUnmappedLabels.length > 0) {
      this.toastr.warning(
        `Automatically unmapped ${autoUnmappedLabels.length} field(s) with no field-level write access for the ` +
        `connected ${this.targetSystem} user (${this.operationMode.toUpperCase()} mode): ${autoUnmappedLabels.join(', ')}.`,
        'Mapping Adjusted'
      );
    }

    this.mappedCount = this.mappings.filter((m) => m.targetField !== '').length;
    this.cdr.markForCheck();
  }

  isMappingBlockedByWriteAccess(mapping: MappingRow): boolean {
    if (!mapping._blockedTargetField) return false;
    const fieldMeta = this.targetFields.find((f) => f.name === mapping._blockedTargetField);
    return !!fieldMeta && !this.isFieldWritable(fieldMeta);
  }

  async validateData(isRevalidation: boolean = false, fixedRecords: any[] = []) {
    if (this.mappedCount === 0) {
      this.toastr.error('Validation Aborted: You must map at least one field to validate data.');
      return;
    }

    const accessViolations = this.getFieldAccessViolations();
    if (accessViolations.length > 0) {
      this.jobStatus = 'Validation Failed';
      const fieldList = accessViolations.map((v) => v.label).join(', ');
      this.toastr.error(
        `The connected ${this.targetSystem} user doesn't have write access to: ${fieldList}. ` +
        `Remove ${accessViolations.length > 1 ? 'these fields' : 'this field'} from the mapping, or connect ` +
        `with a user that has field-level ${this.operationMode === 'insert' ? 'create' : 'edit'} rights, before validating.`,
        'No Field-Level Write Access'
      );
      window.scrollTo({ top: 0, behavior: 'smooth' });
      return;
    }

    if (!isRevalidation && this.customQuery && !this.validateQuery()) {
      this.jobStatus = 'Validation Failed';
      this.toastr.error('Please fix your query criteria before validating.', 'Query Error');
      window.scrollTo({ top: 0, behavior: 'smooth' });
      return;
    }

    if (!this.validateDateRange()) {
      this.jobStatus = 'Validation Failed';
      this.toastr.error(this.dateRangeError!, 'Migration Filter Error');
      window.scrollTo({ top: 0, behavior: 'smooth' });
      return;
    }

    if ((this.operationMode === 'update' || this.operationMode === 'upsert') && !this.externalIdField) {
      this.jobStatus = 'Validation Failed';
      this.toastr.error(
        `Please select an External ID field to match existing ${this.targetSystem} records for ${this.operationMode.toUpperCase()}.`,
        'External ID Required'
      );
      return;
    }

    if (
      (this.operationMode === 'update' || this.operationMode === 'upsert') &&
      this.externalIdField &&
      !this.isExternalIdEligible(this.targetFields.find((f) => f.name === this.externalIdField) || { name: '', label: '' })
    ) {
      this.jobStatus = 'Validation Failed';
      this.toastr.error(
        `"${this.getTargetFieldLabel(this.externalIdField)}" isn't marked as an External ID (or indexed/lookup) field in ${this.targetSystem}, ` +
        `so it can't be used to match records for ${this.operationMode.toUpperCase()}. Mark the field as an External ID in ${this.targetSystem} Setup, or choose a different field.`,
        'Field Not Usable for Matching'
      );
      return;
    }

    if (
      (this.operationMode === 'update' || this.operationMode === 'upsert') &&
      this.externalIdField &&
      !this.restrictToQueryFields(this.mappings).some((m) => m.targetField === this.externalIdField)
    ) {
      this.jobStatus = 'Validation Failed';
      const rawMapping = this.mappings.find((m) => m.targetField === this.externalIdField);
      const message = rawMapping && this.getQueryFieldFilterSet()
        ? `Your External ID source field "${rawMapping.sourceField}" isn't in your query's SELECT list, so it won't be extracted. Add it to your query, or unmap it and choose a field the query actually selects.`
        : `The External ID field "${this.getTargetFieldLabel(this.externalIdField)}" isn't mapped to a source column, so every record would fail to match. Map a source field to it first.`;
      this.toastr.error(message, 'External ID Not Mapped');
      return;
    }

    if (!isRevalidation) {
      const confirmResult = await Swal.fire({
        title: `Are you sure you want to start the ${this.selectedSourceObject} object validation?`,
        text: `This will securely stream and test all live records from ${this.selectedSourceObject} in chunks. It can safely handle millions of rows without crashing your browser.`,
        icon: 'info',
        showCancelButton: true,
        confirmButtonColor: '#0d6efd',
        cancelButtonColor: '#6c757d',
        confirmButtonText: 'Yes, Start Validation Stream'
      });

      if (!confirmResult.isConfirmed) return;

      this.jobStatus = 'Connecting...';
      this.logMessages = [];
      this.aggregateStats = { total: 0, valid: 0, invalid: 0, duplicates: 0 };
      this.validationResults = { invalidRecords: [] };
      this.currentSessionId = '';
    } else {
      this.jobStatus = 'Re-validating...';
      this.toastr.info('Checking fixed records against Salesforce rules...', 'Re-validating');
    }

    this.isValidating = true;
    this.errorCurrentPage = 1;
    this.cdr.detectChanges();

    const activeMappings = this.restrictToQueryFields(this.mappings.filter((m) => m.targetField)).map((m) => {
        const targetMeta = this.targetFields.find((t) => t.name === m.targetField);
        return {
          csvField: m.sourceField,
          sfField: m.targetField,
          sourceField: m.sourceField,
          targetField: m.targetField,
          type: targetMeta?.type || 'string',
          isRequired: targetMeta?.isRequired || targetMeta?.required || false,
          syncPicklistValues: this.canSyncPicklist(m) && !!m.syncPicklistValues
        };
      });

    const sfRules: any = {};
    this.targetFields.forEach((field) => {
      sfRules[field.name] = field;
    });

    let safeQuery = this.customQuery.trim();
    const crmContext = this.sourceCrmId.toLowerCase();
    if (crmContext === 'zendesk' && !this.isStandardZendeskObject(this.selectedSourceObject)) {
      safeQuery = safeQuery.replace(/\/\*[\s\S]*?\*\//g, '').trim();

      const cleanBlankTemplate = this.ZENDESK_CUSTOM_OBJECT_TEMPLATE.replace(/\/\*[\s\S]*?\*\//g, '').trim();
      if (safeQuery === cleanBlankTemplate) {
        safeQuery = '';
      }
    }

    if (this.sourceCrmId.toLowerCase() === 'hubspot') {
      safeQuery = safeQuery.replace(/\/\*[\s\S]*?\*\//g, '').trim();
      const cleanBlankTemplate = this.HUBSPOT_SEARCH_TEMPLATE.replace(/\/\*[\s\S]*?\*\//g, '').trim();
      if (safeQuery === cleanBlankTemplate) {
        safeQuery = '';
      }
    }

    const payload = {
      isRevalidation: isRevalidation,
      sessionId: this.currentSessionId,
      fixedRecords: fixedRecords,
      crmId: this.sourceCrmId,
      targetCrmId: this.targetCrmId,
      objectName: this.selectedSourceObject,
      query: safeQuery,
      mappings: activeMappings,
      dedupeKey: this.externalIdField,
      sfRules: sfRules,
      operationMode: this.operationMode,
      authToken: localStorage.getItem('supabase_token') || '',
      migrationTimeFilter: this.migrationTimeFilter
    };


    this.closeSocket(this.validationSocket);

    const ws = new WebSocket(`${environment.wsUrl}/ws/validate-stream`);
    this.validationSocket = ws;

    ws.onopen = () => {
      ws.send(JSON.stringify(payload));
    };

    ws.onmessage = (event) => {
      this.zone.run(() => {
        const data = JSON.parse(event.data);

        if (isSocketAuthExpired(data)) {
          this.handleSessionExpired(getSocketExpiredSides(data));
          return;
        }
        if (data.log) {
          this.logMessages.push(data.log);
          if (this.logMessages.length > 50) this.logMessages.shift();
        }

        if (data.status) {
          this.jobStatus = data.status;
        }

        if (data.fieldAccessErrors && data.fieldAccessErrors.length > 0) {
          const fieldList = data.fieldAccessErrors.map((v: any) => v.label || v.field).join(', ');
          this.toastr.error(
            `The connected ${this.targetSystem} user doesn't have write access to: ${fieldList}. Remove them from the mapping and try again.`,
            'No Field-Level Write Access'
          );
          this.isValidating = false;
        }

        if (data.stats) {
          this.aggregateStats = data.stats;
        }

        if ((data.status === 'Validation Passed' || data.status === 'Validation Warning') && data.invalidRecords) {
          this.validationResults.invalidRecords = data.invalidRecords;
          if (data.sessionId) {
            this.currentSessionId = data.sessionId;
          }

          if (data.invalidRecords.length >= 500) {
            this.toastr.warning('Showing the first 500 errors to prevent browser lag.', 'Max Errors Reached');
          }
          this.isValidating = false;
        }

        this.cdr.detectChanges();
        setTimeout(() => this.scrollToBottom(), 10);
      });
    };

    ws.onerror = () => {
      this.zone.run(() => {
        if (this.validationSocket === ws) this.validationSocket = null;
        this.logError(' WebSocket Error: Validation stream disconnected.');
        this.jobStatus = 'Validation Failed';
        this.isValidating = false;
        Swal.fire({
          title: 'Connection Lost',
          text: 'The server disconnected unexpectedly while streaming data. Please check your network or server logs and try again.',
          icon: 'error',
          confirmButtonColor: '#0d6efd',
          customClass: { popup: 'rounded-4 shadow-lg border-0' }
        });
        this.cdr.detectChanges();
      });
    };

    ws.onclose = () => {
      this.zone.run(() => {
        if (this.validationSocket === ws) this.validationSocket = null;
        if (this.jobStatus !== 'Validation Passed' && this.jobStatus !== 'Validation Warning' && this.jobStatus !== 'Validation Failed') {
          this.jobStatus = 'Disconnected';
        }
        this.isValidating = false;
        this.cdr.detectChanges();
      });
    };
  }

  applyMassUpdate(sourceField: string, value: string | undefined) {
    if (value === undefined) value = '';

    if (!this.validationResults || !this.validationResults.invalidRecords || this.validationResults.invalidRecords.length === 0) {
      return;
    }

    let updatedCount = 0;

    this.validationResults.invalidRecords.forEach((record: any) => {
      if (this.hasCellError(record, sourceField)) {
        record.originalRow[sourceField] = value;
        this.markAsEdited(record, sourceField);
        updatedCount++;
      }
    });

    if (updatedCount > 0) {
      this.toastr.success(
        `Updated '${sourceField}' across ${updatedCount} records. Correct data was left untouched!`,
        'Mass Update Applied'
      );
    } else {
      this.toastr.info(`No records had an error in '${sourceField}', so nothing was changed.`, 'No Updates Needed');
    }

    this.cdr.detectChanges();
  }

  hasErrorsInColumn(sourceField: string): boolean {
    if (!this.validationResults?.invalidRecords) return false;
    const searchStr = `[${sourceField}:`;
    return this.validationResults.invalidRecords.some((record: any) => record.errors.includes(searchStr));
  }

  get sortedErrorTableFields(): MappingRow[] {
    const mapped = this.mappings.filter((m) => m.targetField);
    return [...mapped].sort((a, b) => {
      const aHasError = this.hasErrorsInColumn(a.sourceField) ? 0 : 1;
      const bHasError = this.hasErrorsInColumn(b.sourceField) ? 0 : 1;
      return aHasError - bHasError;
    });
  }

  get errorColumnCount(): number {
    return this.sortedErrorTableFields.filter((m) => this.hasErrorsInColumn(m.sourceField)).length;
  }

  hasCellError(record: any, sourceField: string): boolean {
    if (!record || !record.errors) return false;
    const searchStr = `[${sourceField}:`;
    return record.errors.includes(searchStr);
  }

  markAsEdited(record: any, fieldName: string) {
    if (!record._editedFields) record._editedFields = {};
    record._editedFields[fieldName] = true;
  }

  async revalidatePreview() {
    if (!this.validationResults?.invalidRecords?.length) return;

    const recordsToTest = this.validationResults.invalidRecords.map((ir: any) => ir.originalRow);

    await this.validateData(true, recordsToTest);
  }

  goBackToConnection(): void {
    this.router.navigate(['/connection']);
  }

  returnToHome(): void {
    this.router.navigate(['/connection']);
  }

  logout(): void {
    Swal.fire({
      title: 'Ready to Leave?',
      text: 'Are you sure you want to log out? Any unsaved mapping progress will be lost.',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#dc3545',
      cancelButtonColor: '#6c757d',
      confirmButtonText: 'Yes, Logout',
      customClass: { popup: 'rounded-4 shadow-lg border-0' }
    }).then((result) => {
      if (result.isConfirmed) {
        if (this.validationSocket && this.validationSocket.readyState === WebSocket.OPEN) {
          this.validationSocket.close();
        }
        if (this.migrationSocket && this.migrationSocket.readyState === WebSocket.OPEN) {
          this.migrationSocket.close();
        }


        forkJoin([
          this.crmAuthService.disconnectCrm('source').pipe(catchError(() => of(null))),
          this.crmAuthService.disconnectCrm('target').pipe(catchError(() => of(null)))
        ]).subscribe(() => {
          localStorage.removeItem('source_crm_slot');
          localStorage.removeItem('target_crm_slot');
          window.dispatchEvent(new Event('connections-updated'));

          this.toastr.success('You have been securely logged out.', 'Goodbye!');
          this.authService.logout();
        });
      }
    });
  }

  resumeMigration(includeFailed = false): void {
    if (!this.currentRunId || this.isGlobalLoading) return;
    this.isGlobalLoading = true;
    this.jobStatus = 'Running';
    this.openMigrationSocket({
      mode: 'resume',
      includeFailed,
      runId: this.currentRunId,
      authToken: localStorage.getItem('supabase_token') || ''
    });
  }

  retryFailedRows(): void {
    if (!this.currentRunId || this.isGlobalLoading) return;
    this.isGlobalLoading = true;
    this.jobStatus = 'Running';
    this.logMessages = [];
    this.openMigrationSocket({
      mode: 'retry_failed',
      runId: this.currentRunId,
      authToken: localStorage.getItem('supabase_token') || ''
    });
  }

  private rememberRun(runId: string): void {
    localStorage.setItem(ApiMappingComponent.LAST_RUN_KEY, runId);
  }

  private forgetRun(): void {
    localStorage.removeItem(ApiMappingComponent.LAST_RUN_KEY);
  }

  // ======================================================================================
  // RUN RECOVERY: failed-record panel, resume / retry / start over, hand-over between users
  // ======================================================================================
  showRecoveryPanel = false;
  recoveryMinimized = false;
  recoveryExpanded = false;
  recoveryTop = 80;
  recoveryLeft = 120;
  readonly recoveryDefaultWidth = 1100;
  readonly recoveryDefaultHeight = 700;
  private recoveryDragging = false;
  private recoveryDragOffset = { x: 0, y: 0 };

  recoverableRuns: any[] = [];
  activeRun: any = null;
  runJobs: any[] = [];
  failureRows: any[] = [];
  failureGroups: any[] = [];
  failureTotal = 0;
  failurePage = 0;
  failurePageSize = 50;
  readonly failurePageSizes = [25, 50, 100, 200];
  failureSearch = '';
  failureJobIdx: number | null = null;
  isLoadingFailures = false;
  isSavingEdits = false;
  private failureSearchTimer: any = null;

  get activeColumns(): any[] {
    const job = this.runJobs.find((j) => j.jobIdx === this.failureJobIdx) || this.runJobs[0];
    return job?.columns || [];
  }

  get dirtyRowCount(): number {
    return this.failureRows.filter((r) => r._dirty).length;
  }

  recoveryView: 'cards' | 'table' = 'cards';
  bulkField = '';
  bulkValue = '';

  recoveryTab: 'failed' | 'queued' = 'failed';
  queuedRows: any[] = [];
  queuedTotal = 0;
  queuedWaiting = 0;
  queuedEditedTotal = 0;
  queuedPage = 0;
  queuedFilter: 'all' | 'edited' = 'all';
  isLoadingQueued = false;
  massPreview: { field: string; value: string; count: number; filter: string } | null = null;
  isApplyingMass = false;
  changeBanner: { title: string; rows: number; unblocked: number; field?: string; value?: string } | null = null;

  get queuedCount(): number {
    return this.activeRun?.counts?.pending || 0;
  }

  get currentJobLabel(): string {
    const job = this.runJobs.find((j) => j.jobIdx === this.failureJobIdx) || this.runJobs[0];
    return job?.targetObject || 'this object';
  }

  get queuedPageCount(): number {
    return Math.max(1, Math.ceil(this.queuedTotal / this.failurePageSize));
  }

  get queuedRangeStart(): number {
    return this.queuedTotal ? this.queuedPage * this.failurePageSize + 1 : 0;
  }

  get queuedRangeEnd(): number {
    return Math.min(this.queuedTotal, this.queuedPage * this.failurePageSize + this.queuedRows.length);
  }

  toggleRecoveryExpanded(): void {
    if (this.recoveryMinimized) return;
    this.recoveryExpanded = !this.recoveryExpanded;
  }

  async setRecoveryTab(tab: 'failed' | 'queued'): Promise<void> {
    if (tab === this.recoveryTab) return;
    if (tab === 'queued' && this.dirtyRowCount) await this.saveRunEdits(true);
    this.recoveryTab = tab;
    this.massPreview = null;
    if (tab === 'queued') {
      this.queuedPage = 0;
      await this.loadQueued();
    } else {
      await this.loadFailures();
    }
    this.cdr.detectChanges();
  }

  showQueued(): void {
    void this.setRecoveryTab('queued');
  }

  async setQueuedFilter(filter: 'all' | 'edited'): Promise<void> {
    if (filter === this.queuedFilter) return;
    this.queuedFilter = filter;
    this.queuedPage = 0;
    await this.loadQueued();
  }

  async changeQueuedPage(delta: number): Promise<void> {
    const next = this.queuedPage + delta;
    if (next < 0 || next >= this.queuedPageCount) return;
    this.queuedPage = next;
    await this.loadQueued();
  }

  async loadQueued(): Promise<void> {
    if (!this.activeRun) return;
    this.isLoadingQueued = true;
    this.cdr.detectChanges();
    try {
      const q = new URLSearchParams({
        offset: String(this.queuedPage * this.failurePageSize),
        limit: String(this.failurePageSize)
      });
      if (this.failureJobIdx !== null) q.set('jobIdx', String(this.failureJobIdx));
      const search = this.activeGroup ? '' : this.failureSearch.trim();
      if (search) q.set('search', search);
      if (this.queuedFilter === 'edited') q.set('editedOnly', 'true');
      const res = await this.runApi(`/api/runs/${encodeURIComponent(this.activeRun.runId)}/queued?${q}`);
      this.queuedRows = res.rows || [];
      this.queuedTotal = res.total || 0;
      this.queuedWaiting = res.waitingTotal || 0;
      this.queuedEditedTotal = res.editedTotal || 0;
    } catch (e: any) {
      this.toastr.error(e?.message || 'Could not load the queued records.', 'Recovery');
    } finally {
      this.isLoadingQueued = false;
      this.cdr.detectChanges();
    }
  }

  queuedPreview(row: any): Array<{ label: string; value: string }> {
    return this.columnsForJob(row.jobIdx)
      .slice(0, 4)
      .map((c: any) => ({ label: c.sourceField, value: this.formatCellValue(row.record?.[c.sourceField]) }));
  }

  requestMassUpdate(field: string, value: string): void {
    if (!field || !this.activeRun || this.failureJobIdx === null || !this.failureTotal) return;
    this.massPreview = { field, value: value ?? '', count: this.failureTotal, filter: this.failureSearch.trim() };
  }

  cancelMassUpdate(): void {
    this.massPreview = null;
  }

  async confirmMassUpdate(): Promise<void> {
    const p = this.massPreview;
    if (!p || !this.activeRun || this.failureJobIdx === null || this.isApplyingMass) return;
    this.isApplyingMass = true;
    this.cdr.detectChanges();
    try {
      const res = await this.runApi(`/api/runs/${encodeURIComponent(this.activeRun.runId)}/records/mass-update`, {
        method: 'POST',
        body: JSON.stringify({ jobIdx: this.failureJobIdx, field: p.field, value: p.value, errorContains: p.filter })
      });
      this.massPreview = null;
      this.bulkValue = '';
      this.changeBanner = { title: 'Mass update applied', rows: res.rowsRequeued || 0, unblocked: res.rowsUnblocked || 0, field: p.field, value: p.value };
      this.failurePage = 0;
      this.queuedPage = 0;
      this.recoveryTab = 'queued';
      await this.refreshActiveRun();
      await this.loadFailures(); // also reloads the queued list while the Ready tab is open
    } catch (e: any) {
      this.toastr.error(e?.message || 'Mass update failed.', 'Recovery');
    } finally {
      this.isApplyingMass = false;
      this.cdr.detectChanges();
    }
  }

  /** % of all rows in the run that already succeeded (drives the progress ring). */
  get successPct(): number {
    const c = this.activeRun?.counts || {};
    const total = (c.success || 0) + (c.error || 0) + (c.skipped || 0) + (c.pending || 0);
    return total ? Math.round(((c.success || 0) / total) * 100) : 0;
  }

  pct(n: number | undefined): number {
    const c = this.activeRun?.counts || {};
    const total = (c.success || 0) + (c.error || 0) + (c.skipped || 0) + (c.pending || 0);
    return total ? ((n || 0) / total) * 100 : 0;
  }

  get recoveryStep(): number {
    if (this.dirtyRowCount > 0) return 2;
    if ((this.activeRun?.counts?.error || 0) > 0) return 1;
    return 3;
  }

  get activeGroup(): any | null {
    const s = this.failureSearch.trim();
    return s ? this.failureGroups.find((g) => g.message === s) || null : null;
  }

  explainError(message: string): { title: string; tone: string; icon: string; fix: string } {
    const m = String(message || '').toLowerCase();
    const rules: Array<{ re: RegExp; title: string; tone: string; icon: string; fix: string }> = [
      { re: /duplicate|already exists|duplicate_value|not unique|unique/, title: 'Duplicate record', tone: 'warn', icon: 'icon-copy',
        fix: 'A record with the same unique value already exists in the target. Change the value, or leave these rows out of the retry.' },
      { re: /required|missing|cannot be blank|must not be empty|may not be null|mandatory/, title: 'Required value missing', tone: 'danger', icon: 'icon-alert-circle',
        fix: 'A mandatory field is empty. Fill it in for these rows (use “Fix all at once” if the same value works for all of them).' },
      { re: /picklist|restricted|bad value|invalid_option|not a valid option|invalid value for/, title: 'Value not allowed in dropdown', tone: 'warn', icon: 'icon-list',
        fix: 'The target field only accepts specific dropdown values. Type one of the allowed values — “Fix all at once” changes every matching row in one click.' },
      { re: /invalid_cross_reference|cross reference|lookup|reference|malformed id|invalid id|entity is deleted|parent|not found/, title: 'Linked record not found', tone: 'link', icon: 'icon-link',
        fix: 'The record this row points to does not exist in the target. Retry the parent object first, or correct the linked ID / value.' },
      { re: /too long|max.*length|string_too_long|exceeds|too large|value too large/, title: 'Text too long', tone: 'warn', icon: 'icon-type',
        fix: 'The text is longer than the target field allows. Shorten it in the highlighted field.' },
      { re: /validation|custom_validation|validation rule/, title: 'Blocked by a validation rule', tone: 'danger', icon: 'icon-shield',
        fix: 'The target org has a rule that rejected this record. Adjust the highlighted value, or ask an admin to relax the rule during migration.' },
      { re: /insufficient_access|permission|not authorized|forbidden|field.?level|read.?only|not writeable|not updateable|cannot be updated/, title: 'No permission to write', tone: 'danger', icon: 'icon-lock',
        fix: 'The connected user cannot write this field or object. Check field-level security / permissions, or remove the mapping.' },
      { re: /date|datetime|invalid_type|invalid format|format|cannot deserialize|not a valid|invalid number|invalid email|invalid phone/, title: 'Wrong format', tone: 'info', icon: 'icon-calendar',
        fix: 'A value is not in the format the target expects (dates like 2025-01-31, numbers without text, valid email / phone). Correct the highlighted field.' },
      { re: /quota|rate limit|too many requests|429|api limit|request_limit/, title: 'API limit reached', tone: 'info', icon: 'icon-clock',
        fix: 'The CRM is throttling requests. No data fix needed — wait a few minutes and retry.' },
      { re: /timeout|timed out|unavailable|50[023]|unable_to_lock_row|lock/, title: 'Temporary CRM problem', tone: 'info', icon: 'icon-clock',
        fix: 'This is usually temporary. Retry without changing anything.' }
    ];
    const hit = rules.find((r) => r.re.test(m));
    return hit
      ? { title: hit.title, tone: hit.tone, icon: hit.icon, fix: hit.fix }
      : { title: 'Rejected by the CRM', tone: 'danger', icon: 'icon-help-circle',
          fix: 'Read the CRM message, correct the highlighted values, then retry. Copy the message if you need to share it with an admin.' };
  }

  private columnsForJob(jobIdx: any): any[] {
    const job = this.runJobs.find((j) => j.jobIdx === jobIdx) || this.runJobs.find((j) => j.jobIdx === this.failureJobIdx) || this.runJobs[0];
    return job?.columns || [];
  }

  private guessGroupField(g: any): string {
    const text = String(g?.message || '').toLowerCase();
    const hit = this.columnsForJob(g?.jobIdx).find(
      (c: any) =>
        (c.targetField && text.includes(String(c.targetField).toLowerCase())) ||
        (c.sourceField && text.includes(String(c.sourceField).toLowerCase()))
    );
    return hit?.sourceField || '';
  }

  selectGroup(g: any): void {
    this.massPreview = null;
    if (this.failureSearch.trim() === g.message) {
      this.clearFailureFilter();
      return;
    }
    this.bulkField = g._field || this.bulkField;
    this.filterByGroup(g);
  }

  clearFailureFilter(): void {
    this.massPreview = null;
    this.queuedPage = 0;
    this.failureSearch = '';
    this.failurePage = 0;
    void this.loadFailures();
  }

  orderedColumns(row: any): any[] {
    if (!row._cols) {
      const cols = this.activeColumns;
      const flagged = cols.filter((c) => this.hasRunCellError(row, c));
      row._cols = [...flagged, ...cols.filter((c) => !flagged.includes(c))];
      row._flagged = flagged.length;
    }
    return row._cols;
  }

  flaggedCount(row: any): number {
    this.orderedColumns(row);
    return row._flagged || 0;
  }

  get failureRangeStart(): number {
    return this.failureTotal ? this.failurePage * this.failurePageSize + 1 : 0;
  }

  get failureRangeEnd(): number {
    return Math.min(this.failureTotal, this.failurePage * this.failurePageSize + this.failureRows.length);
  }

  async setFailurePageSize(value: string): Promise<void> {
    const size = Number(value);
    if (!this.failurePageSizes.includes(size) || size === this.failurePageSize) return;
    if (this.dirtyRowCount) await this.saveRunEdits(true);
    this.failurePageSize = size;
    this.failurePage = 0;
    await this.loadFailures();
  }

  revertCell(row: any, field: string): void {
    if (!row._orig || !(field in row._orig)) return;
    row.record[field] = row._orig[field];
    delete row._edited[field];
    delete row._orig[field];
    row._dirty = Object.keys(row._edited).length > 0;
  }

  async copyError(text: string): Promise<void> {
    try {
      await navigator.clipboard.writeText(String(text || ''));
      this.toastr.info('Error message copied.', 'Copied');
    } catch {
      this.toastr.warning('Could not copy automatically.', 'Copy');
    }
  }

  get failurePageCount(): number {
    return Math.max(1, Math.ceil(this.failureTotal / this.failurePageSize));
  }

  private async runApi<T = any>(path: string, init: RequestInit = {}): Promise<T> {
    const response = await fetch(`${environment.apiUrl}${path}`, {
      ...init,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${localStorage.getItem('supabase_token') || ''}`,
        ...((init.headers as Record<string, string>) || {})
      }
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      if (isAuthExpiredError({ status: response.status, error: body })) {
        this.handleSessionExpired(getExpiredSidesFromError({ error: body }));
      }
      const detail = body?.detail;
      const err: any = new Error(typeof detail === 'string' ? detail : detail?.message || `Request failed (${response.status})`);
      err.status = response.status;
      throw err;
    }
    return body as T;
  }

  private pendingRecovery: any = null;

  private async restoreLastRun(): Promise<void> {
    try {
      const { runs } = await this.runApi('/api/runs/recoverable');
      this.recoverableRuns = runs || [];
      const candidate = this.recoverableRuns[0];
      if (!candidate) return;

      this.currentRunId = candidate.runId;
      this.rememberRun(candidate.runId);
      this.canResume = !!candidate.resumable;
      this.canRetryFailed = !!candidate.retryable;
      this.retryCount = candidate.counts?.error || 0;

      this.pendingRecovery = candidate;
      this.openPendingRecovery();
    } catch {
      // offline / server restarting: the next visit asks again
    } finally {
      this.cdr.detectChanges();
    }
  }

  private openPendingRecovery(): void {
    const candidate = this.pendingRecovery;
    if (!candidate || this.isGlobalLoading || this.migrationSocket) return;
    this.pendingRecovery = null;
    void this.openRunRecovery(candidate.runId);
    const who = candidate.isMine ? 'You' : candidate.startedBy?.name || 'Another user';
    this.logMessages = [
      ...this.logMessages,
      `${who} started a migration on this org (${candidate.runId}) that ${
        candidate.resumable ? 'stopped before finishing' : candidate.retryable ? 'finished with failed rows' : 'is still being wrapped up'
      }. Review it in the Fix & retry panel.`
    ];
  }

  async openRunRecovery(runId: string): Promise<void> {
    try {
      const detail = await this.runApi(`/api/runs/${encodeURIComponent(runId)}`);
      this.activeRun = detail;
      this.scheduleLockRefresh();
      this.runJobs = detail.jobColumns || [];
      this.currentRunId = detail.runId;
      this.canResume = !!detail.resumable;
      this.canRetryFailed = !!detail.retryable;
      this.retryCount = detail.counts?.error || 0;
      const withErrors = (detail.jobs || []).find((j: any) => j.counts?.error);
      this.failureJobIdx = withErrors?.jobIdx ?? this.runJobs[0]?.jobIdx ?? null;
      this.failurePage = 0;
      this.failureSearch = '';
      this.bulkField = '';
      this.bulkValue = '';
      this.recoveryTab = 'failed';
      this.queuedPage = 0;
      this.queuedFilter = 'all';
      this.massPreview = null;
      this.changeBanner = null;
      if (!this.showRecoveryPanel) this.positionRecoveryPanel();
      this.showRecoveryPanel = true;
      this.recoveryMinimized = false;
      await this.loadFailures();
      if (this.failureTotal === 0 && (detail.counts?.pending || 0) > 0) {
        this.recoveryTab = 'queued';
        await this.loadQueued();
      }
    } catch (e: any) {
      if (e?.status === 404) {
        this.handleStaleRun(runId);
      } else {
        this.toastr.error(e?.message || 'Could not load the run.', 'Recovery');
      }
    } finally {
      this.cdr.detectChanges();
    }
  }

  /** The server no longer has this run (cleaned up, other folder, other account): stop offering Retry/Resume for it. */
  private handleStaleRun(runId?: string): void {
    this.forgetRun();
    if (!runId || this.currentRunId === runId) {
      this.currentRunId = null;
      this.canResume = false;
      this.canRetryFailed = false;
    }
    this.activeRun = null;
    this.showRecoveryPanel = false;
    this.recoverableRuns = this.recoverableRuns.filter((r) => r.runId !== runId);
    this.toastr.warning(
      'That run is no longer available on the server, so it cannot be retried or resumed. Start a new migration.',
      'Run not found'
    );
  }

  async loadFailures(): Promise<void> {
    if (!this.activeRun) return;
    this.isLoadingFailures = true;
    this.cdr.detectChanges();
    try {
      const q = new URLSearchParams({
        offset: String(this.failurePage * this.failurePageSize),
        limit: String(this.failurePageSize)
      });
      if (this.failureJobIdx !== null) q.set('jobIdx', String(this.failureJobIdx));
      if (this.failureSearch.trim()) q.set('search', this.failureSearch.trim());
      const res = await this.runApi(`/api/runs/${encodeURIComponent(this.activeRun.runId)}/failures?${q}`);
      this.failureRows = (res.rows || []).map((r: any) => ({ ...r, _edited: {}, _orig: {}, _dirty: false, _info: this.explainError(r.error) }));
      this.failureTotal = res.total || 0;
      this.failureGroups = (res.groups || []).map((g: any) => ({ ...g, _info: this.explainError(g.message), _field: this.guessGroupField(g) }));
      if (this.recoveryTab === 'queued') void this.loadQueued();
    } catch (e: any) {
      this.toastr.error(e?.message || 'Could not load failed records.', 'Recovery');
    } finally {
      this.isLoadingFailures = false;
      this.cdr.detectChanges();
    }
  }

  private async refreshActiveRun(): Promise<void> {
    if (!this.activeRun) return;
    const detail = await this.runApi(`/api/runs/${encodeURIComponent(this.activeRun.runId)}`);
    this.activeRun = detail;
    this.canResume = !!detail.resumable;
    this.canRetryFailed = !!detail.retryable;
    this.retryCount = detail.counts?.error || 0;
    this.scheduleLockRefresh();
  }

  onFailureSearchChange(value: string): void {
    this.failureSearch = value;
    clearTimeout(this.failureSearchTimer);
    this.failureSearchTimer = setTimeout(() => {
      this.massPreview = null;
      this.queuedPage = 0;
      this.failurePage = 0;
      void this.loadFailures();
    }, 350);
  }

  async onFailureJobChange(value: string): Promise<void> {
    this.failureJobIdx = value === '' ? null : Number(value);
    this.massPreview = null;
    this.queuedPage = 0;
    this.failurePage = 0;
    await this.loadFailures();
  }

  filterByGroup(group: any): void {
    this.massPreview = null;
    this.queuedPage = 0;
    this.failureJobIdx = group.jobIdx;
    this.failureSearch = group.message;
    this.failurePage = 0;
    void this.loadFailures();
  }

  async changeFailurePage(delta: number): Promise<void> {
    const next = this.failurePage + delta;
    if (next < 0 || next >= this.failurePageCount) return;
    if (this.dirtyRowCount) await this.saveRunEdits(true);
    this.failurePage = next;
    await this.loadFailures();
  }

  /** The CRM usually names the offending field in the error text: highlight those cells like the validation grid. */
  hasRunCellError(row: any, col: any): boolean {
    const text = String(row?.error || '').toLowerCase();
    if (!text) return false;
    return (
      (!!col.targetField && text.includes(String(col.targetField).toLowerCase())) ||
      (!!col.sourceField && text.includes(String(col.sourceField).toLowerCase()))
    );
  }

  onRunCellEdit(row: any, field: string, value: string): void {
    if (String(row.record[field] ?? '') === value) return;
    row._orig = row._orig || {};
    if (!(field in row._orig)) row._orig[field] = row.record[field];
    row.record[field] = value;
    row._edited[field] = true;
    row._dirty = true;
  }

  async saveRunEdits(silent = false): Promise<void> {
    const dirty = this.failureRows.filter((r) => r._dirty);
    if (!dirty.length || !this.activeRun) return;
    this.isSavingEdits = true;
    this.cdr.detectChanges();
    try {
      const edits = dirty.map((r) => ({
        jobIdx: r.jobIdx,
        rowIdx: r.rowIdx,
        fields: Object.fromEntries(Object.keys(r._edited).map((k) => [k, r.record[k]]))
      }));
      const res = await this.runApi(`/api/runs/${encodeURIComponent(this.activeRun.runId)}/records`, {
        method: 'PATCH',
        body: JSON.stringify({ edits })
      });
      await this.refreshActiveRun();
      if (!silent) {
        this.changeBanner = { title: 'Changes saved', rows: res.rowsRequeued || 0, unblocked: res.rowsUnblocked || 0 };
        this.queuedPage = 0;
        this.recoveryTab = 'queued';
        await this.loadFailures(); // also reloads the queued list while the Ready tab is open
      }
    } catch (e: any) {
      this.toastr.error(e?.message || 'Could not save your edits.', 'Recovery');
    } finally {
      this.isSavingEdits = false;
      this.cdr.detectChanges();
    }
  }

  async resumeFromPanel(includeFailed = false): Promise<void> {
    if (!this.activeRun) return;
    const atRisk = this.activeRun.insertAtRiskRows || 0;
    if (atRisk > 0) {
      const ok = await Swal.fire({
        title: 'Some rows may already exist',
        html: `<b>${atRisk}</b> record(s) were being inserted when the run stopped, and there is no key to check whether the target already received them. Resuming sends them again, so <b>duplicates are possible</b> for those rows. Check the target first if that matters.`,
        icon: 'warning',
        showCancelButton: true,
        confirmButtonText: 'Resume anyway',
        customClass: { popup: 'rounded-4 shadow-lg border-0' }
      });
      if (!ok.isConfirmed) return;
    }
    if (this.dirtyRowCount) await this.saveRunEdits(true);
    this.showRecoveryPanel = false;
    this.currentRunId = this.activeRun.runId;
    this.globalLoadingText = 'Resuming migration...';
    this.globalLoadingSubText = 'Continuing from the last saved checkpoint.';
    this.resumeMigration(includeFailed);
  }

  async retryFromPanel(): Promise<void> {
    if (!this.activeRun) return;
    if (this.dirtyRowCount) await this.saveRunEdits(true);
    this.showRecoveryPanel = false;
    this.currentRunId = this.activeRun.runId;
    this.globalLoadingText = 'Retrying failed records...';
    this.globalLoadingSubText = 'Only failed and corrected records are re-sent.';
    this.retryFailedRows();
  }

  async restartFromPanel(): Promise<void> {
    if (!this.activeRun || this.isGlobalLoading) return;
    const created = this.activeRun.counts?.success || 0;
    const confirm = await Swal.fire({
      title: 'Start over?',
      html: created
        ? `This run already sent <b>${created}</b> record(s). Starting over re-extracts and re-sends <b>everything</b>; in Insert mode that can create duplicates in the target org. Use <b>Resume</b> to continue from where it stopped instead.`
        : 'Starts a fresh run with the same configuration. Nothing has been sent yet.',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonText: 'Start over',
      confirmButtonColor: '#dc3545',
      customClass: { popup: 'rounded-4 shadow-lg border-0' }
    });
    if (!confirm.isConfirmed) return;
    this.showRecoveryPanel = false;
    this.isGlobalLoading = true;
    this.jobStatus = 'Running';
    this.logMessages = [];
    this.openMigrationSocket({
      mode: 'restart',
      runId: this.activeRun.runId,
      acknowledgeDuplicates: created > 0,
      authToken: localStorage.getItem('supabase_token') || ''
    });
  }

  stopReasonLabel(code: string | undefined): string {
    switch (code) {
      case 'client_left': return 'Browser tab closed / connection lost';
      case 'cancelled': return 'Cancelled by a user';
      case 'quota': return 'CRM API limit reached';
      case 'crm_unavailable': return 'CRM stopped responding';
      case 'session_expired': return 'CRM login expired';
      case 'org_mismatch': return 'Connected to a different org';
      case 'error': return 'Unexpected error';
      default: return 'Stopped';
    }
  }

  private lockRefreshTimer: any = null;

  /**
   * After a backend crash the dead session's lock lapses on its own (~30s). Re-check the run when it does, so
   * Resume appears without the user having to reload. A genuinely live run is re-checked the same way.
   */
  private scheduleLockRefresh(): void {
    clearTimeout(this.lockRefreshTimer);
    const wait = this.activeRun?.lockRemainingSec || 0;
    if (this.showRecoveryPanel && this.activeRun?.status === 'running' && wait > 0) {
      this.lockRefreshTimer = setTimeout(async () => {
        try {
          await this.refreshActiveRun();
        } catch {
          /* ignore: the next user action reloads */
        }
        this.cdr.detectChanges();
      }, (wait + 1) * 1000);
    }
  }

  /** True when the panel is closed but the current run still has something to resume / retry. */
  get canReopenRecovery(): boolean {
    return !this.showRecoveryPanel && !this.isGlobalLoading && !!this.currentRunId && (this.canResume || this.canRetryFailed);
  }

  minimizeRecoveryPanel(): void {
    this.recoveryMinimized = true;
    this.recoveryExpanded = false;
  }

  restoreRecoveryPanel(): void {
    this.recoveryMinimized = false;
  }

  /** Dismiss the panel. Unsaved cell edits are discarded, so ask first when there are any. */
  async cancelRecoveryPanel(): Promise<void> {
    const dirty = this.dirtyRowCount;
    if (dirty > 0) {
      const confirm = await Swal.fire({
        title: 'Discard unsaved edits?',
        html: `<b>${dirty}</b> edited record(s) have not been saved. Closing the panel discards those edits.`,
        icon: 'warning',
        showCancelButton: true,
        confirmButtonText: 'Discard and close',
        cancelButtonText: 'Keep editing',
        confirmButtonColor: '#dc3545',
        customClass: { popup: 'rounded-4 shadow-lg border-0' }
      });
      if (!confirm.isConfirmed) return;
    }
    clearTimeout(this.failureSearchTimer);
    this.showRecoveryPanel = false;
    this.recoveryMinimized = false;
    this.recoveryExpanded = false;
    this.cdr.detectChanges();
  }

  /** Always reloads from the server so counts and failed rows are current after a retry / resume. */
  async reopenRecoveryPanel(): Promise<void> {
    if (this.currentRunId) await this.openRunRecovery(this.currentRunId);
  }

  @HostListener('document:keydown.escape')
  onRecoveryEscape(): void {
    if (this.showRecoveryPanel && !this.recoveryMinimized && !Swal.isVisible()) {
      this.minimizeRecoveryPanel();
      this.cdr.detectChanges();
    }
  }

  positionRecoveryPanel(): void {
    this.recoveryExpanded = false;
    this.recoveryLeft = Math.max(20, (window.innerWidth - this.recoveryDefaultWidth) / 2);
    this.recoveryTop = Math.max(20, (window.innerHeight - this.recoveryDefaultHeight) / 2);
  }

  startRecoveryPanelDrag(event: MouseEvent): void {
    if (this.recoveryExpanded || this.recoveryMinimized) return;
    if ((event.target as HTMLElement).closest('button')) return;
    const panelEl = (event.currentTarget as HTMLElement).closest('.floating-review-panel') as HTMLElement;
    if (!panelEl) return;
    const rect = panelEl.getBoundingClientRect();
    this.recoveryDragOffset = { x: event.clientX - rect.left, y: event.clientY - rect.top };
    this.recoveryDragging = true;
    event.preventDefault();
  }

  @HostListener('document:mousemove', ['$event'])
  onRecoveryPanelDrag(event: MouseEvent): void {
    if (!this.recoveryDragging) return;
    const margin = 60;
    this.recoveryLeft = Math.min(Math.max(event.clientX - this.recoveryDragOffset.x, 0), window.innerWidth - margin);
    this.recoveryTop = Math.min(Math.max(event.clientY - this.recoveryDragOffset.y, 0), window.innerHeight - margin);
  }

  @HostListener('document:mouseup')
  onRecoveryPanelDragEnd(): void {
    this.recoveryDragging = false;
  }

  formatCellValue(val: any): string {
    if (val === null || val === undefined) return '';
    if (typeof val === 'object') return JSON.stringify(val);
    return String(val);
  }

  downloadCSV(raw_data: any[], filename: string) {
    if (!raw_data || raw_data.length === 0) return;

    const data = raw_data.map((row) => this.flattenObject(row));

    const headerSet = new Set<string>();
    data.forEach((row) => Object.keys(row).forEach((key) => headerSet.add(key)));
    const headers = Array.from(headerSet);

    const csvRows = [];
    csvRows.push(headers.join(','));

    for (const row of data) {
      const values = headers.map((header) => {
        let val = row[header];

        if (val !== null && typeof val === 'object') {
          val = JSON.stringify(val);
        }

        const stringVal = String(val !== undefined && val !== null ? val : '');

        if (/^\d{16,}$/.test(stringVal)) {
          return `"\t${stringVal}"`;
        }

        const escaped = stringVal.replace(/"/g, '""');
        return `"${escaped}"`;
      });
      csvRows.push(values.join(','));
    }

    const csvString = csvRows.join('\n');
    const blob = new Blob([csvString], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.setAttribute('hidden', '');
    a.setAttribute('href', url);
    a.setAttribute('download', filename);
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  }

   runMigration() {
    if (this.hasPendingEdits) {
      this.toastr.warning(
        'You have un-validated fixes in the grid. Please click "Re-Validate Fixes" before running the migration.',
        'Action Required'
      );
      const revalBtn = document.querySelector('.btn-danger.fw-bold') as HTMLElement;
      if (revalBtn) {
        revalBtn.classList.add('animate__animated', 'animate__headShake');
        setTimeout(() => revalBtn.classList.remove('animate__headShake'), 1000);
      }
      return;
    }

    const { errors, warnings } = this.getValidationIssues();

    if (errors.length > 0) {
      this.jobStatus = 'Failed';
      this.toastr.error(errors[0], 'Migration Error');
      return;
    }

    const activeMappings = this.restrictToQueryFields(this.mappings.filter((m) => m.targetField !== ''));

    if (warnings.missingFields.length > 0 || warnings.incompleteRefs.length > 0) {
      this.show_warning_modal(warnings);
    } else {
      this.show_confirmation_modal(activeMappings);
    }
  }

  private getValidationIssues() {
    const errors: string[] = [];
    const warnings = {
      missingFields: this.getMissingRequiredFields(),
      incompleteRefs: this.getIncompleteReferenceMappings()
    };

    const accessViolations = this.getFieldAccessViolations();
    if (accessViolations.length > 0) {
      const fieldList = accessViolations.map((v) => v.label).join(', ');
      errors.push(
        `The connected ${this.targetSystem} user doesn't have write access to: ${fieldList}. Remove ` +
        `${accessViolations.length > 1 ? 'these fields' : 'this field'} from the mapping before running.`
      );
    }

    if (this.customQuery && !this.validateQuery()) {
      errors.push('Please fix your query criteria before running.');
    }

    if (!this.validateDateRange()) {
      errors.push(this.dateRangeError!);
    }

    const activeMappings = this.restrictToQueryFields(this.mappings.filter((m) => m.targetField !== ''));
    if (activeMappings.length === 0) {
      errors.push('Please map at least one field before running the migration.');
    }

    const isUpdateMode = this.operationMode === 'update' || this.operationMode === 'upsert';
    if (isUpdateMode) {
      if (!this.externalIdField) {
        errors.push(`Please select an External ID field to match existing ${this.targetSystem} records for ${this.operationMode.toUpperCase()}.`);
      } else {
        const targetFieldMeta = this.targetFields.find((f) => f.name === this.externalIdField);
        const isEligible = this.isExternalIdEligible(targetFieldMeta || { name: '', label: '' });

        if (!isEligible) {
          errors.push(`"${this.getTargetFieldLabel(this.externalIdField)}" isn't marked as an External ID in ${this.targetSystem}. Mark it in Setup or choose another field.`);
        } else if (!activeMappings.some((m) => m.targetField === this.externalIdField)) {
          const rawMapping = this.mappings.find((m) => m.targetField === this.externalIdField);
          if (rawMapping && this.getQueryFieldFilterSet()) {
            errors.push(`Your External ID source field "${rawMapping.sourceField}" isn't in your query's SELECT list, so it won't be extracted. Add it to your query, or unmap it and choose a field the query actually selects.`);
          } else {
            errors.push(`The External ID field "${this.getTargetFieldLabel(this.externalIdField)}" isn't mapped to a source column.`);
          }
        }
      }
    }

    if (!this.selectedSourceObject || !this.selectedTargetObject) {
      errors.push('Source and Target objects must be selected.');
    }

    return { errors, warnings };
  }

  private show_warning_modal(warnings: { missingFields: string[], incompleteRefs: string[] }) {
    let warningHtml = '<div class="text-start mt-2">';

    if (warnings.missingFields.length > 0) {
      warningHtml += `<p class="text-danger fw-bold mb-1"><i class="feather icon-alert-triangle"></i> Missing Required Fields:</p>
                       <ul class="small mb-3 text-muted"><li class="mb-0">${warnings.missingFields.join('</li><li class="mb-0">')}</li></ul>`;
    }

    if (warnings.incompleteRefs.length > 0) {
      warningHtml += `<p class="text-warning text-dark fw-bold mb-1"><i class="feather icon-link"></i> Incomplete Lookups:</p>
                       <p class="small mb-1 text-muted">You mapped these relational fields but left the <strong>Parent Ext ID</strong> blank (Defaults to 'Id'):</p>
                       <ul class="small mb-0 text-muted"><li class="mb-0">${warnings.incompleteRefs.join('</li><li class="mb-0">')}</li></ul>`;
    }
    warningHtml += '</div>';

    Swal.fire({
      title: 'Mapping Warnings',
      html: warningHtml,
      icon: 'warning',
      showCancelButton: true,
      confirmButtonText: 'Run Anyway',
      cancelButtonText: 'Fix Mapping',
      confirmButtonColor: '#dc3545',
      customClass: { popup: 'rounded-4 shadow-lg border-0' }
    }).then((result) => {
      if (result.isConfirmed) {
        const activeMappings = this.restrictToQueryFields(this.mappings.filter((m) => m.targetField !== ''));
        this.executeMigrationJob(activeMappings);
      } else {
        this.jobStatus = 'Idle';
      }
    });
  }

  private show_confirmation_modal(activeMappings: any[]) {
    const filesInScope = this.fileMigrationSupported && (this.migrateAttachments || this.migrateFiles);
    const preview = this.fileMigrationBudgetPreview;

    let fileBudgetHtml = '';
    if (filesInScope && preview && !preview.fitsInBudget) {
      fileBudgetHtml = `
        <div class="alert alert-warning text-start small mt-3 mb-0">
          <strong><i class="feather icon-alert-triangle"></i> File migration budget notice:</strong>
          ${preview.message}
          You'll be asked to confirm the exact scope again once the migration reaches the file
          transfer step, with live numbers at that moment.
        </div>`;
    } else if (filesInScope && preview && preview.fitsInBudget) {
      fileBudgetHtml = `
        <div class="alert alert-success text-start small mt-3 mb-0">
          <i class="feather icon-check-circle"></i> File migration limit check passed —
          ~${preview.estimatedTotalCalls.toLocaleString()} API call(s) needed, within today's allowance for Data Migration.
        </div>`;
    } else if (filesInScope && this.isCheckingFileMigrationBudget) {
      fileBudgetHtml = `
        <div class="alert alert-secondary text-start small mt-3 mb-0">
          Still checking the file migration API Limit in the background — this run will still
          be guarded live during the file transfer step regardless.
        </div>`;
    }

    Swal.fire({
      title: 'Ready to Migrate!',
      html: `
        <p class="mb-0">Are you sure you want to execute this ${this.operationMode.toUpperCase()} job? This will push live data into ${this.selectedTargetObject}.</p>
        ${fileBudgetHtml}
      `,
      icon: 'info',
      showCancelButton: true,
      confirmButtonColor: '#198754',
      cancelButtonColor: '#6c757d',
      confirmButtonText: 'Yes, Run Job!'
    }).then((result) => {
      if (result.isConfirmed) {
        this.executeMigrationJob(activeMappings);
      } else {
        this.jobStatus = 'Idle';
      }
    });
  }

  downloadAudit(type: 'valid' | 'invalid') {
  if (!this.currentSessionId) {
    this.toastr.error('Session expired. Please run the validation stream again to generate a new report.', 'No Session');
    return;
  }

  if (type === 'invalid' && this.validationResults?.invalidRecords) {
    this.toastr.info(`Generating error audit report...`, 'Downloading');
    this.downloadCSV(this.validationResults.invalidRecords, 'validation_errors.csv');
    return;
  }

  this.toastr.info(`Generating valid audit report...`, 'Downloading');
  const url = `${environment.apiUrl}/api/audit/download/${this.currentSessionId}?type=${type}`;
  const token = localStorage.getItem('supabase_token') || '';

  this.http.get(url, {
    responseType: 'blob',
    headers: { Authorization: `Bearer ${token}` }
  }).subscribe({
    next: (blob) => {
      const objectUrl = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = objectUrl;
      a.download = `Validation_Audit_${type}_${this.currentSessionId}.csv`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(objectUrl);
    },
    error: () => this.toastr.error('Failed to download audit report.', 'Download Failed')
  });
}

  onFileMigrationCheckboxToggle(): void {
    this.scheduleFileMigrationBudgetRecheck();
  }

  private scheduleFileMigrationBudgetRecheck(): void {
    if (this.fileMigrationBudgetPreviewDebounce) {
      clearTimeout(this.fileMigrationBudgetPreviewDebounce);
    }

    if (!this.fileMigrationSupported || (!this.migrateAttachments && !this.migrateFiles)) {
      this.fileMigrationBudgetPreview = null;
      this.fileMigrationBudgetPreviewError = null;
      this.isCheckingFileMigrationBudget = false;
      return;
    }


    this.fileMigrationBudgetPreview = null;

    this.fileMigrationBudgetPreviewDebounce = setTimeout(() => {
      this.checkFileMigrationBudgetPreview();
    }, 600);
  }

  private checkFileMigrationBudgetPreview(): void {
    if (!this.selectedSourceObject) {
      return;
    }

    this.isCheckingFileMigrationBudget = true;
    this.fileMigrationBudgetPreviewError = null;
    const token = localStorage.getItem('supabase_token') || '';

    const body = {
      sourceObject: this.selectedSourceObject,
      query: this.customQuery?.trim() || '',
      sourceCrm: this.sourceCrmId.toLowerCase(),
      targetCrm: this.targetCrmId.toLowerCase(),
      migrationTimeFilter: this.migrationTimeFilter,
      migrateAttachments: this.migrateAttachments,
      migrateFiles: this.migrateFiles
    };

    this.http.post<FileMigrationBudgetPreview>(
      `${environment.apiUrl}/api/migration/files/precheck`,
      body,
      { headers: { Authorization: `Bearer ${token}` } }
    ).subscribe({
      next: (result) => {
        this.fileMigrationBudgetPreview = result;
        this.isCheckingFileMigrationBudget = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        if (isAuthExpiredError(err)) { this.handleSessionExpired(getExpiredSidesFromError(err)); return; }
        this.fileMigrationBudgetPreview = null;
        this.fileMigrationBudgetPreviewError =
          err?.error?.detail || 'Could not check the API Limit for this migration. You can still proceed -- the live guard during the run will catch this if needed.';
        this.isCheckingFileMigrationBudget = false;
        this.cdr.detectChanges();
      }
    });
  }

  private showFileMigrationBudgetPrompt(budget: FileMigrationBudget, ws: WebSocket): void {
    this.pendingFileMigrationBudget = budget;

    const safePercent = budget.totalRecordCount
      ? Math.round((budget.safeRecordCount / budget.totalRecordCount) * 100)
      : 0;
    const deferredCount = budget.totalRecordCount - budget.safeRecordCount;

    Swal.fire({
      title: `File Migration — ${budget.targetObject}`,
      html: `
        <div class="text-start">
          <p class="text-muted small mb-3">
            ${budget.totalFileCount.toLocaleString()} file(s) attached to
            ${budget.totalRecordCount.toLocaleString()} record(s) need to move, but today's Salesforce
            API allowance on the <strong>${budget.bindingOrg}</strong> org can't cover all of them safely.
          </p>
          <div class="d-flex justify-content-around gap-3 mb-3">
            <div class="p-2 border rounded bg-light w-100">
              <div class="small fw-bold text-uppercase text-muted">Source (downloads)</div>
              <div class="fw-bold">${budget.estimatedDownloadCalls.toLocaleString()} needed</div>
              <div class="small text-muted">${budget.sourceAvailable.toLocaleString()} available today</div>
            </div>
            <div class="p-2 border rounded bg-light w-100">
              <div class="small fw-bold text-uppercase text-muted">Target (uploads)</div>
              <div class="fw-bold">${budget.estimatedUploadCalls.toLocaleString()} needed</div>
              <div class="small text-muted">${budget.targetAvailable.toLocaleString()} available today</div>
            </div>
          </div>
          <div class="alert alert-primary py-2 mb-0">
            <strong>Recommended:</strong> migrate files for
            <strong>${budget.safeRecordCount.toLocaleString()} of ${budget.totalRecordCount.toLocaleString()}</strong>
            record(s) now (~${budget.safeFileCount.toLocaleString()} files, ${safePercent}%).
            The remaining ${deferredCount.toLocaleString()} record(s) keep their field data — just no
            files this pass — and can be re-run once the ${budget.bindingOrg} org's daily limit resets.
          </div>
        </div>
      `,
      icon: 'warning',
      showConfirmButton: true,
      confirmButtonText: `Migrate ${budget.safeRecordCount.toLocaleString()} now (recommended)`,
      confirmButtonColor: '#0d6efd',
      showDenyButton: true,
      denyButtonText: 'Skip files this pass',
      showCancelButton: true,
      cancelButtonText: 'Migrate all anyway',
      reverseButtons: true,
      allowOutsideClick: false,
      allowEscapeKey: false,
      customClass: { popup: 'rounded-4 shadow-lg border-0' }
    }).then((result) => {
      if (result.isConfirmed) {
        this.sendFileMigrationDecision(ws, budget.jobRef, 'proceed_limited');
      } else if (result.isDenied) {
        this.sendFileMigrationDecision(ws, budget.jobRef, 'skip');
      } else if (result.dismiss === Swal.DismissReason.cancel) {
        this.confirmFullFileMigrationOverride(budget, ws);
      } else {

        this.sendFileMigrationDecision(ws, budget.jobRef, 'skip');
      }
    });
  }

  private confirmFullFileMigrationOverride(budget: FileMigrationBudget, ws: WebSocket): void {
    Swal.fire({
      title: 'Migrate all records anyway?',
      html: `
        <p class="text-muted small mb-0">
          This may exhaust the <strong>${budget.bindingOrg}</strong> org's daily API limit mid-run,
          which can also affect other tools or integrations sharing that org today.
        </p>
      `,
      icon: 'warning',
      showConfirmButton: true,
      confirmButtonText: `Yes, migrate all ${budget.totalRecordCount.toLocaleString()}`,
      confirmButtonColor: '#d93025',
      showCancelButton: true,
      cancelButtonText: 'Go back',
      reverseButtons: true,
      customClass: { popup: 'rounded-4 shadow-lg border-0' }
    }).then((result) => {
      if (result.isConfirmed) {
        this.sendFileMigrationDecision(ws, budget.jobRef, 'proceed_full');
      } else {

        this.showFileMigrationBudgetPrompt(budget, ws);
      }
    });
  }

  private sendFileMigrationDecision(ws: WebSocket, jobRef: string, action: FileMigrationDecisionAction): void {
    if (ws.readyState !== WebSocket.OPEN) {
      this.toastr.error('Connection to migration engine was lost before a file migration decision could be sent.', 'Engine Error');
      this.pendingFileMigrationBudget = null;
      return;
    }
    ws.send(JSON.stringify({ fileMigrationDecision: { jobRef, action } }));
    this.pendingFileMigrationBudget = null;

    const actionLabel =
      action === 'proceed_limited' ? 'Proceeding with the recommended reduced scope...' :
      action === 'proceed_full' ? 'Proceeding with the full scope...' :
      'Skipping files for this pass...';
    this.logMessages = [...this.logMessages, `[File Migration] ${actionLabel}`];
    this.cdr.detectChanges();
  }

  private executeMigrationJob(activeMappings: any[]) {
    this.successData = [];
    this.errorData = [];
    this.skippedData = [];
    this.jobStatus = 'Initializing...';
    this.logMessages = [];
    this.isGlobalLoading = true;
    this.globalLoadingText = `Migrating to ${this.targetSystem}...`;
    this.globalLoadingSubText = `Pushing data into ${this.selectedTargetObject}. Please do not close this window.`;
    this.cdr.detectChanges();
    this.toastr.info('Connecting to Migration Engine...', 'Job Started');

    const enhancedMappings = activeMappings.map((m) => {
      const fieldMeta = this.targetFields.find((t) => t.name === m.targetField);
      const isRef = this.isReferenceField(m.targetField);
      return {
        sourceField: m.sourceField,
        targetField: m.targetField,
        type: isRef ? 'reference' : fieldMeta?.type,
        syncPicklistValues: this.canSyncPicklist(m) && !!m.syncPicklistValues,
        referenceTo: fieldMeta?.referenceTo,
        relationshipName: fieldMeta?.relationshipName,
        relationalExtIdField: m.relationalExtIdField || (isRef ? 'Id' : undefined),
        parentObjectName: m.parentObjectName || (fieldMeta?.referenceTo ? fieldMeta.referenceTo[0] : undefined)
      };
    });

    let safeQuery = this.customQuery.trim();
    if (this.sourceCrmId.toLowerCase() === 'zendesk' && !this.isStandardZendeskObject(this.selectedSourceObject)) {
      safeQuery = safeQuery.replace(/\/\*[\s\S]*?\*\//g, '').trim();

      const cleanBlankTemplate = this.ZENDESK_CUSTOM_OBJECT_TEMPLATE.replace(/\/\*[\s\S]*?\*\//g, '').trim();
      if (safeQuery === cleanBlankTemplate) {
        safeQuery = '';
      }
    }

    if (this.sourceCrmId.toLowerCase() === 'hubspot') {
      safeQuery = safeQuery.replace(/\/\*[\s\S]*?\*\//g, '').trim();
      const cleanBlankTemplate = this.HUBSPOT_SEARCH_TEMPLATE.replace(/\/\*[\s\S]*?\*\//g, '').trim();
      if (safeQuery === cleanBlankTemplate) {
        safeQuery = '';
      }
    }

    const fixedRecords =
      this.validationResults?.invalidRecords?.filter((rec: any) => rec._editedFields)?.map((rec: any) => rec.originalRow) || [];

    const sfRules: any = {};
    this.targetFields.forEach((field) => {
      sfRules[field.name] = field;
    });

    const job = {
      sessionId: this.currentSessionId,
      fixedRecords: fixedRecords,
      sourceObject: this.selectedSourceObject,
      targetObject: this.selectedTargetObject,
      sourceCrmId: this.sourceCrmId,
      targetCrmId: this.targetCrmId,
      extractionQuery: safeQuery,
      mappings: enhancedMappings,
      sfRules: sfRules,
      operationMode: this.operationMode,
      batchSize: this.batchSize,
      externalIdField: this.externalIdField,
      migrateAttachments: this.fileMigrationSupported ? this.migrateAttachments : false,
      migrateFiles: this.fileMigrationSupported && !this.isSourceSingleFileType ? this.migrateFiles : false,
      migrationTimeFilter: this.migrationTimeFilter,

      authToken: localStorage.getItem('supabase_token') || ''
    };

    this.openMigrationSocket({ queue: [job] });
  }

  private openMigrationSocket(payload: any): void {
    this.canResume = false;
    this.canRetryFailed = false;
    if (!payload.mode) {
      // a brand-new run: Resume / Retry must never target the previous one
      this.currentRunId = null;
      this.forgetRun();
    }
    this.closeSocket(this.migrationSocket);

    const ws = new WebSocket(`${environment.wsUrl}/ws/migrate`);
    this.migrationSocket = ws;

    ws.onopen = () => {
      ws.send(JSON.stringify(payload));
    };

    ws.onmessage = (event) => {
      this.zone.run(() => {
        const data = JSON.parse(event.data);

        if (isSocketAuthExpired(data)) {
          this.handleSessionExpired(getSocketExpiredSides(data));
          return;
        }

        if (data.runId) {
          this.currentRunId = data.runId;
          this.rememberRun(data.runId);
        }

        if (data.status === 'Interrupted' || data.status === 'Cancelled') {
          this.isGlobalLoading = false;
          this.canResume = data.status === 'Interrupted' && !!data.resumable;
          if (data.status === 'Cancelled') this.forgetRun();
          Swal.fire({
            title: data.status === 'Cancelled' ? 'Migration cancelled' : 'Migration paused',
            text: data.log,
            icon: 'info',
            confirmButtonColor: '#0d6efd',
            customClass: { popup: 'rounded-4 shadow-lg border-0' }
          });
        }

        if (data.log) {
          this.logMessages = [...this.logMessages, data.log];
        }

        if (data.errorCode === 'ORG_MISMATCH') {
          this.isGlobalLoading = false;
          Swal.fire({
            title: 'Different org connected',
            text: data.log,
            icon: 'error',
            confirmButtonColor: '#0d6efd',
            customClass: { popup: 'rounded-4 shadow-lg border-0' }
          });
        }

        if (data.status === 'Failed' && /Run not found/i.test(data.log || '')) {
          this.isGlobalLoading = false;
          this.handleStaleRun(this.currentRunId || undefined);
        }

        if (data.status === 'AwaitingFileMigrationApproval' && data.fileMigrationBudget) {
          this.showFileMigrationBudgetPrompt(data.fileMigrationBudget, ws);
        }

        if (data.fileMigrationJobResult) {
          const r = data.fileMigrationJobResult;
          const parts: string[] = [];
          if (r.attachmentsSuccess || r.attachmentsError) {
            parts.push(`Attachments: ${r.attachmentsSuccess} ok / ${r.attachmentsError} failed`);
          }
          if (r.filesSuccess || r.filesError) {
            parts.push(`Files: ${r.filesSuccess} ok / ${r.filesError} failed`);
          }
          if (r.deferredRecordCount > 0) {
            parts.push(`${r.deferredRecordCount} record(s) deferred (API Limit)`);
          }
          if (parts.length) {
            this.logMessages = [...this.logMessages, `[${r.targetObject}] ${parts.join(' \u00b7 ')}`];
          }
        }

        if (data.status) {
          this.jobStatus = data.status;

          if (data.status === 'Finished') {
            this.isGlobalLoading = false;
            const successCount = data.successData ? data.successData.length : 0;
            const errorCount = data.errorData ? data.errorData.length : 0;
            const skippedCount = data.skippedData ? data.skippedData.length : 0;

            this.canResume = false;
            this.canRetryFailed = !!data.retryable;
            this.retryCount = errorCount;
            if (!data.retryable) this.forgetRun();

            let swalIcon: 'success' | 'warning' | 'error' = 'success';
            let swalTitle = 'Migration Complete!';

            if (errorCount > 0 && successCount > 0) {
              swalIcon = 'warning';
              swalTitle = 'Migration Finished with Errors';
            } else if (errorCount > 0 && successCount === 0) {
              swalIcon = 'error';
              swalTitle = 'Migration Failed';
            } else if (skippedCount > 0) {
              swalTitle = 'Migration Complete (Some Records Skipped)';
            }

            Swal.fire({
              title: swalTitle,
              html: `
                <div class="text-center mt-3">
                  <div class="d-flex justify-content-around mb-4 gap-3">
                    <div class="p-3 border rounded border-success-subtle bg-success-subtle w-100 shadow-sm">
                      <h2 class="text-success mb-0 fw-bold">${successCount}</h2>
                      <span class="small fw-bold text-success-emphasis text-uppercase">Successful</span>
                    </div>
                    ${skippedCount > 0 ? `
                    <div class="p-3 border rounded border-warning-subtle bg-warning-subtle w-100 shadow-sm">
                      <h2 class="text-warning-emphasis mb-0 fw-bold">${skippedCount}</h2>
                      <span class="small fw-bold text-warning-emphasis text-uppercase">Skipped (No Match)</span>
                    </div>` : ''}
                    <div class="p-3 border rounded border-danger-subtle bg-danger-subtle w-100 shadow-sm">
                      <h2 class="text-danger mb-0 fw-bold">${errorCount}</h2>
                      <span class="small fw-bold text-danger-emphasis text-uppercase">Rejected</span>
                    </div>
                  </div>
                  <p class="text-muted small mb-0">Check the terminal logs or download the reports below to review the details.</p>
                </div>
              `,
              icon: swalIcon,
              confirmButtonText: 'View Logs',
              confirmButtonColor: '#0d6efd',
              customClass: { popup: 'rounded-4 shadow-lg border-0' }
            });
          }
        }

        if (data.successData) this.successData = data.successData;
        if (data.errorData) this.errorData = data.errorData;
        if (data.skippedData) this.skippedData = data.skippedData;

        // Failed records / stop details open in the recovery panel (loaded from the server, paged)
        if (data.runId && data.status === 'Finished' && (data.errorData?.length || 0) > 0) {
          void this.openRunRecovery(data.runId);
        }
        if (data.runId && (data.status === 'Interrupted' || data.status === 'Cancelled')) {
          void this.openRunRecovery(data.runId);
        }

        this.cdr.detectChanges();
        setTimeout(() => {
          const logContainer = document.querySelector('#terminal-window');
          if (logContainer) logContainer.scrollTop = logContainer.scrollHeight;
        }, 10);
      });
    };

    ws.onerror = () => {
      this.zone.run(() => {
        if (this.migrationSocket === ws) this.migrationSocket = null;
        this.isGlobalLoading = false;
        this.logMessages.push('FATAL: Connection to migration engine lost or refused.');
        this.jobStatus = 'Failed';
        this.canResume = !!this.currentRunId;
        this.toastr.error('WebSocket connection failed.', 'Engine Error');
        Swal.fire({
          title: 'Connection Lost',
          text: 'The server disconnected unexpectedly while streaming data. Please check your network or server logs and try again.',
          icon: 'error',
          confirmButtonColor: '#0d6efd',
          customClass: { popup: 'rounded-4 shadow-lg border-0' }
        });
        this.cdr.detectChanges();
      });
    };

    ws.onclose = () => {
      this.zone.run(() => {
        if (this.migrationSocket === ws) this.migrationSocket = null;
        this.isGlobalLoading = false;
        if (this.jobStatus === 'Running' || this.jobStatus === 'Initializing...') {
          this.jobStatus = 'Disconnected';
          this.canResume = !!this.currentRunId;
        }
        this.cdr.detectChanges();
      });
    };
  }
}