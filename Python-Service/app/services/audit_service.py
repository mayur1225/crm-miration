import os
import json
import tempfile
import csv
import logging
from datetime import datetime, timezone
from collections import Counter
from typing import Callable, Iterable, Iterator, Optional
from fpdf import FPDF
from supabase import create_client
from app.utils.config import supabase, SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger(__name__)

LOGO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets/images")
LOGO_CANDIDATES = ["gemini-svg.svg", "logo.png"]


def _resolve_logo_path():
    for name in LOGO_CANDIDATES:
        candidate = os.path.join(LOGO_DIR, name)
        if os.path.exists(candidate):
            return candidate
    return None


ERROR_CATEGORIES = [
    ("Duplicate Record", ["duplicate", "already exists", "duplicate_value"]),
    ("Required Field Missing", ["required", "cannot be blank", "is required", "required_field_missing"]),
    ("Invalid Reference / Lookup", ["invalid cross reference", "no matching", "foreign key", "invalid lookup", "reference to a record"]),
    ("Permission / Field-Level Security", ["insufficient access", "permission", "not authorized", "field_custom_validation", "fls", "unable to access"]),
    ("Validation Rule Failed", ["validation rule", "field_custom_validation_exception"]),
    ("Invalid Value / Format", ["invalid", "malformed", "picklist", "invalid_field", "bad format", "value provided"]),
    ("Rate Limit / API Budget", ["rate limit", "request_limit_exceeded", "too many requests", "api limit"]),
    ("Authentication / Session", ["invalid_session_id", "unauthorized", "expired token", "invalid token", "session expired"]),
]


class SureShiftPDF(FPDF):
    """FPDF subclass that draws the SureShift Migration branded header
    (logo + tool name) and footer (page number + copyright) on every page
    automatically, instead of that being drawn by hand once per report."""

    report_title = "Migration Audit Report"

    NAVY = (15, 23, 42)      # matches logo card background (#0F172A)
    CYAN = (6, 182, 212)     # matches logo accent (#06B6D4)
    GRAY = (148, 163, 184)   # matches logo tagline gray (#94A3B8)

    def header(self):
        s = AuditService._sanitize_pdf_text

        # --- Banner background (navy, so the logo drops in seamlessly) ---
        self.set_fill_color(*self.NAVY)
        self.rect(0, 0, self.w, 26, style="F")

        # --- Logo (falls back to a text wordmark if no file is present) ---
        logo_path = _resolve_logo_path()
        logo_drawn = False
        logo_w = 0
        if logo_path:
            try:
                logo_h = 18
                logo_w = logo_h * (800 / 300)  # source logo's aspect ratio
                self.image(logo_path, x=8, y=4, h=logo_h)
                logo_drawn = True
            except Exception:
                logger.warning("Could not embed logo at %s -- falling back to text.", logo_path, exc_info=True)
                logo_drawn = False

        title_x = 8 + logo_w + 6 if logo_drawn else 10

        # --- Report title (logo already carries the SureShift wordmark) ---
        self.set_text_color(255, 255, 255)
        if not logo_drawn:
            self.set_xy(title_x, 5)
            self.set_font("Arial", "B", 16)
            self.cell(0, 8, txt=s("SureShift Migration"), ln=True)
            self.set_x(title_x)

        self.set_xy(title_x, 13)
        self.set_font("Arial", "", 11)
        self.cell(0, 6, txt=s(self.report_title))

        self.set_text_color(0, 0, 0)
        self.set_y(31)

    def footer(self):
        s = AuditService._sanitize_pdf_text
        self.set_y(-18)

        # --- Accent rule ---
        self.set_draw_color(*self.CYAN)
        self.set_line_width(0.6)
        self.line(10, self.get_y(), self.w - 10, self.get_y())
        self.set_line_width(0.2)

        self.set_y(-14)
        self.set_font("Arial", "", 8)
        self.set_text_color(*self.GRAY)

        self.cell(0, 6, txt=s(f"Page {self.page_no()} of {{nb}}"), align="C")

        self.set_y(-9)
        year = datetime.now().year
        self.cell(
            0, 5,
            txt=s(f"\u00a9 {year} SureShift Migration. All rights reserved. | Confidential audit report."),
            align="C",
        )
        self.set_text_color(0, 0, 0)


class AuditService:

    # ==========================================
    # ERROR CATEGORIZATION
    # ==========================================
    @staticmethod
    def categorize_error(error_message: str) -> str:
        msg = (error_message or "").lower()
        for category, keywords in ERROR_CATEGORIES:
            if any(kw in msg for kw in keywords):
                return category
        return "Other API Error"

    @staticmethod
    def build_error_summary(error_data: list, top_n: int = 8) -> list:
        if not error_data:
            return []

        counts = Counter()
        samples = {}
        for err in error_data:
            message = err.get("error") if isinstance(err, dict) else str(err)
            category = AuditService.categorize_error(message)
            counts[category] += 1
            if category not in samples:
                samples[category] = message

        ranked = counts.most_common()
        top = ranked[:top_n]
        rest_count = sum(c for _, c in ranked[top_n:])

        summary = [
            {"category": cat, "count": count, "sample": samples[cat]}
            for cat, count in top
        ]
        if rest_count:
            summary.append({"category": "Other", "count": rest_count, "sample": None})
        return summary

    # ==========================================
    # PDF TEXT SANITIZATION
    # ==========================================
    @staticmethod
    def _sanitize_pdf_text(text) -> str:
        if text is None:
            return ""
        s = str(text)
        replacements = {
            "\u2018": "'", "\u2019": "'",   
            "\u201c": '"', "\u201d": '"',   
            "\u2013": "-", "\u2014": "-",   
            "\u2026": "...",                 
            "\u00a0": " ",                   
        }
        for src, dst in replacements.items():
            s = s.replace(src, dst)
        return s.encode("latin-1", "replace").decode("latin-1")

    # ==========================================
    # EFFECTIVE QUERY RECONSTRUCTION 
    # ==========================================
    @staticmethod
    def _build_effective_query(source_crm: str, target_object: str, extraction_query: str, time_filter: dict = None) -> str:
        query = (extraction_query or "").strip()
        no_query_label = "(no query filter -- CSV export)"

        if not time_filter:
            return query or no_query_label

        try:
            from app.services.time_filter_service import (
                build_salesforce_time_clause, build_zoho_time_clause,
                build_zendesk_time_clause, build_hubspot_time_filters,
                merge_time_clause,
            )

            crm = (source_crm or "").lower()

            if crm == "salesforce":
                time_clause = build_salesforce_time_clause(time_filter)
                if not time_clause:
                    return query or no_query_label
                if query.lower().startswith("select "):
                    return merge_time_clause(query, time_clause, where_kw="WHERE", and_kw="AND")
                where_parts = [p for p in [f"({query})" if query else None, time_clause] if p]
                return f"SELECT * FROM {target_object} WHERE {' AND '.join(where_parts)}"

            elif crm == "zoho":
                time_clause = build_zoho_time_clause(time_filter)
                if not time_clause:
                    return query or no_query_label
                if query.lower().startswith("select "):
                    return merge_time_clause(query, time_clause, where_kw="where", and_kw="and")
                where_parts = [p for p in [f"({query})" if query else None, time_clause] if p]
                return f"select * from {target_object} where {' and '.join(where_parts)}"

            elif crm == "zendesk":
                time_clause = build_zendesk_time_clause(time_filter)
                parts = [p for p in [query, time_clause] if p]
                return " ".join(parts) if parts else no_query_label

            elif crm == "hubspot":
                time_filters = build_hubspot_time_filters(time_filter)
                base = query or "{}"
                return f"{base}  |  time filters: {json.dumps(time_filters)}" if time_filters else base

            else:
                return query or no_query_label

        except Exception:
            logger.warning(
                "[AUDIT] Could not reconstruct effective query for source_crm=%s -- "
                "falling back to raw extraction_query.", source_crm, exc_info=True
            )
            return query or no_query_label

    # ==========================================
    # SHARED STORAGE UPLOAD HELPER
    # ==========================================
    @staticmethod
    def _upload_file(local_path: str, storage_path: str, content_type: str, disposition: str, bucket: str = "migration_reports") -> str:
        filename = os.path.basename(storage_path)
        with open(local_path, "rb") as f:
            supabase.storage.from_(bucket).upload(
                storage_path,
                f,
                file_options={
                    "x-upsert": "true",
                    "content-type": content_type,
                    "content-disposition": f'{disposition}; filename="{filename}"',
                }
            )
        return supabase.storage.from_(bucket).get_public_url(storage_path)

    @staticmethod
    def _upload_csv(rows: list, fieldnames: list, storage_path: str, bucket: str = "migration_reports") -> str:
        filename = os.path.basename(storage_path)
        tmp_path = os.path.join(tempfile.gettempdir(), filename)

        with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(rows)

        url = AuditService._upload_file(
            tmp_path, storage_path,
            content_type="text/csv; charset=utf-8",
            disposition="attachment",
            bucket=bucket,
        )
        os.remove(tmp_path)
        return url

    # ==========================================
    # SCOPED SUPABASE CLIENT
    # ==========================================
    @staticmethod
    def _scoped_client(auth_token: str):
        """Client that sends the user's JWT to PostgREST so RLS policies on the
        history tables see auth.uid(). Uses postgrest.auth() instead of
        auth.set_session(refresh_token=""), which validates/refreshes the session
        client-side and can raise on an empty refresh token."""
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        client.postgrest.auth(auth_token)
        return client

    @staticmethod
    def _upload_csv_stream(
        rows_factory: Callable[[], Iterable[dict]],
        storage_path: str,
        leading_columns: Optional[list] = None,
        bucket: str = "migration_reports",
    ) -> Optional[str]:
        """Two-pass streaming CSV export: pass 1 collects the column union,
        pass 2 writes. rows_factory must return a fresh iterator on each call
        so large staged sessions never have to be fully held in memory.
        Returns None when there are no rows."""
        leading = list(leading_columns or [])
        columns = list(leading)
        seen = set(columns)
        has_rows = False
        for row in rows_factory():
            has_rows = True
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    columns.append(key)
        if not has_rows:
            return None

        filename = os.path.basename(storage_path)
        tmp_path = os.path.join(tempfile.gettempdir(), filename)
        try:
            with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=columns, quoting=csv.QUOTE_MINIMAL, restval="")
                writer.writeheader()
                for row in rows_factory():
                    writer.writerow(row)
            return AuditService._upload_file(
                tmp_path, storage_path,
                content_type="text/csv; charset=utf-8",
                disposition="attachment",
                bucket=bucket,
            )
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    # ==========================================
    # MIGRATION REPORTS
    # ==========================================
    @staticmethod
    def generate_and_save_reports(user_id: str, session_id: str, source_crm: str, target_crm: str, target_object: str, success_data: list, error_data: list, auth_token: str, extraction_query: str = "", time_filter: dict = None, op_mode: str = "", user_email: str = None, user_name: str = None, migration_mode: str = None, source_mode: str = None, actual_query_used: str = None):
        success_count = len(success_data)
        error_count = len(error_data)
        total = success_count + error_count

        urls = {"pdf": None, "success_csv": None, "error_csv": None}
        error_summary = AuditService.build_error_summary(error_data)

        run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %I:%M:%S %p UTC")
        migration_mode_display = migration_mode or ("CSV / Excel Upload" if (source_crm or "").lower() == "csv" else "Direct API Sync")

        effective_query = actual_query_used or AuditService._build_effective_query(source_crm, target_object, extraction_query, time_filter)

        # ==========================================
        # 1. GENERATE & UPLOAD PDF SUMMARY
        # ==========================================
        try:
            s = AuditService._sanitize_pdf_text

            pdf = SureShiftPDF()
            pdf.alias_nb_pages()
            pdf.set_auto_page_break(auto=True, margin=22)
            pdf.add_page()

            # --- WATERMARK ---
            pdf.set_font("Arial", "B", 40)
            pdf.set_text_color(240, 240, 240)  # Very light gray
            pdf.text(25, 165, s("SureShift Migration"))
            pdf.set_text_color(0, 0, 0)

            # --- RUN DETAILS ---
            pdf.set_fill_color(245, 245, 245) # Light grey section header
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 8, txt=s(" Migration Run Details"), ln=True, fill=True)
            pdf.set_font("Arial", "", 10)
            pdf.cell(95, 7, txt=s(f"  User Name: {user_name or 'N/A'}"))
            pdf.cell(95, 7, txt=s(f"  User Email: {user_email or 'N/A'}"), ln=True)
            pdf.cell(95, 7, txt=s(f"  Generated At: {run_timestamp}"))
            pdf.cell(95, 7, txt=s(f"  Operation Mode: {op_mode}"), ln=True)
            pdf.ln(5)

            # --- MIGRATION SCOPE ---
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 8, txt=s(" Migration Scope"), ln=True, fill=True)
            pdf.set_font("Arial", "", 10)
            pdf.cell(95, 7, txt=s(f"  Source: {source_crm.capitalize()}"))
            pdf.cell(95, 7, txt=s(f"  Target: {target_crm.capitalize()}"), ln=True)
            pdf.cell(95, 7, txt=s(f"  Object: {target_object}"))
            pdf.cell(95, 7, txt=s(f"  Migration Mode: {migration_mode_display}"), ln=True)
            pdf.ln(5)

            # --- EXECUTION RESULTS ---
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 8, txt=s(" Execution Results"), ln=True, fill=True)
            pdf.set_font("Arial", "B", 10)
            pdf.cell(60, 7, txt=s(f"  Total Records: {total}"))
            
            pdf.set_text_color(39, 174, 96) # Green
            pdf.cell(60, 7, txt=s(f"  Successful: {success_count}"))
            
            pdf.set_text_color(192, 57, 43) # Red
            pdf.cell(60, 7, txt=s(f"  Failed: {error_count}"), ln=True)
            
            pdf.set_text_color(0, 0, 0) # Reset
            pdf.ln(5)

            # --- QUERY SECTION ---
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 8, txt=s(" Effective Query (incl. filters)"), ln=True, fill=True)
            pdf.set_font("Courier", "", 9)
            pdf.multi_cell(0, 6, txt=s(effective_query), border=1)
            pdf.ln(5)

            # --- ERROR SUMMARY TABLE ---
            if error_summary:
                pdf.set_font("Arial", "B", 12)
                pdf.cell(0, 8, txt=s(" Top Error Categories"), ln=True, fill=True)
                
                # Table Header
                pdf.set_font("Arial", "B", 10)
                pdf.set_fill_color(220, 220, 220)
                pdf.cell(140, 7, txt=s("Error Category"), border=1, fill=True)
                pdf.cell(50, 7, txt=s("Count"), border=1, ln=True, align="C", fill=True)
                
                # Table Rows
                pdf.set_font("Arial", "", 10)
                for item in error_summary:
                    pdf.cell(140, 7, txt=s(item['category']), border=1)
                    pdf.cell(50, 7, txt=s(str(item['count'])), border=1, ln=True, align="C")

            temp_pdf = os.path.join(tempfile.gettempdir(), f"{session_id}.pdf")
            pdf.output(temp_pdf)

            pdf_filename = f"{session_id}.pdf"
            urls["pdf"] = AuditService._upload_file(
                temp_pdf, f"{user_id}/{pdf_filename}",
                content_type="application/pdf",
                disposition="inline",
            )
            os.remove(temp_pdf)
        except Exception:
            logger.exception(
                "[AUDIT] PDF summary generation/upload failed for session %s -- "
                "continuing without it so the history row still gets saved.", session_id
            )

        # ==========================================
        # 2. GENERATE & UPLOAD SUCCESS CSV
        # ==========================================
        try:
            if success_count > 0:
                fieldnames = list(success_data[0].keys())
                urls["success_csv"] = AuditService._upload_csv(
                    success_data, fieldnames, f"{user_id}/{session_id}_success.csv"
                )
        except Exception:
            logger.exception(
                "[AUDIT] Success CSV generation/upload failed for session %s -- "
                "continuing without it so the history row still gets saved.", session_id
            )

        # ==========================================
        # 3. GENERATE & UPLOAD ERROR CSV
        # ==========================================
        try:
            if error_count > 0:
                flat_errors = []
                for err in error_data:
                    flat_rec = dict(err.get("record", {}))
                    flat_rec["Migration_Error_Message"] = err.get("error", "Unknown Error")
                    flat_errors.append(flat_rec)

                fieldnames = ["Migration_Error_Message"] + [k for k in flat_errors[0].keys() if k != "Migration_Error_Message"]
                urls["error_csv"] = AuditService._upload_csv(
                    flat_errors, fieldnames, f"{user_id}/{session_id}_error.csv"
                )
        except Exception:
            logger.exception(
                "[AUDIT] Error CSV generation/upload failed for session %s -- "
                "continuing without it so the history row still gets saved.", session_id
            )

        # ==========================================
        # 4. SAVE TO DATABASE
        # ==========================================
        scoped_client = AuditService._scoped_client(auth_token)

        scoped_client.table("migration_history").insert({
            "user_id": user_id,
            "session_id": session_id,
            "source_crm": source_crm,
            "target_crm": target_crm,
            "target_object": target_object,
            "total_records": total,
            "success_count": success_count,
            "error_count": error_count,
            "pdf_url": urls["pdf"],
            "success_csv_url": urls["success_csv"],
            "error_csv_url": urls["error_csv"],
            "error_summary": error_summary,
            "migration_mode": migration_mode_display,
            "source_mode": source_mode,
        }).execute()

        return urls

    # ==========================================
    # VALIDATION REPORTS
    # ==========================================
    @staticmethod
    def generate_and_save_validation_report(
        user_id: str,
        session_id: str,
        source_crm: str,
        target_crm: str,
        target_object: str,
        stats: dict,
        invalid_records: Iterable[dict],
        auth_token: str,
        valid_records_factory: Optional[Callable[[], Iterable[dict]]] = None,
    ):
        """Persists one validation_history row plus both log files:
        - invalid_csv_url: every failed row + Validation_Errors column
        - valid_csv_url:   every passed row (previously never generated)
        invalid_records is a list (needed for error_summary); valid_records_factory
        is a zero-arg callable returning a fresh iterator of valid row dicts."""
        invalid_csv_url = None
        valid_csv_url = None
        invalid_records = list(invalid_records or [])
        error_like = [{"record": rec.get("originalRow", {}), "error": rec.get("errors", "")} for rec in invalid_records]
        error_summary = AuditService.build_error_summary(error_like)

        try:
            if invalid_records:
                def _invalid_rows() -> Iterator[dict]:
                    for rec in invalid_records:
                        flat = dict(rec.get("originalRow", {}))
                        flat.pop("_db_id", None)
                        flat["Validation_Errors"] = rec.get("errors", "")
                        yield flat

                invalid_csv_url = AuditService._upload_csv_stream(
                    _invalid_rows,
                    f"{user_id}/{session_id}_validation_invalid.csv",
                    leading_columns=["Validation_Errors"],
                )
        except Exception:
            logger.exception(
                "[AUDIT] Invalid-records CSV generation/upload failed for session %s -- "
                "continuing without it so the history row still gets saved.", session_id
            )

        try:
            if valid_records_factory is not None and stats.get("valid", 0) > 0:
                def _valid_rows() -> Iterator[dict]:
                    for rec in valid_records_factory():
                        flat = dict(rec)
                        flat.pop("_db_id", None)
                        yield flat

                valid_csv_url = AuditService._upload_csv_stream(
                    _valid_rows,
                    f"{user_id}/{session_id}_validation_valid.csv",
                )
        except Exception:
            logger.exception(
                "[AUDIT] Valid-records CSV generation/upload failed for session %s -- "
                "continuing without it so the history row still gets saved.", session_id
            )

        row = {
            "user_id": user_id,
            "session_id": session_id,
            "source_crm": (source_crm or "").lower(),
            "target_crm": (target_crm or "").lower(),
            "target_object": target_object,
            "total_records": stats.get("total", 0),
            "valid_count": stats.get("valid", 0),
            "invalid_count": stats.get("invalid", 0),
            "duplicate_count": stats.get("duplicates", 0),
            "invalid_csv_url": invalid_csv_url,
            "valid_csv_url": valid_csv_url,
            "error_summary": error_summary,
        }

        scoped_client = AuditService._scoped_client(auth_token)
        AuditService._save_validation_row(scoped_client, session_id, row)

        return {"invalid_csv_url": invalid_csv_url, "valid_csv_url": valid_csv_url, "error_summary": error_summary}

    @staticmethod
    def _save_validation_row(scoped_client, session_id: str, row: dict) -> None:
        try:
            scoped_client.table("validation_history").upsert(row, on_conflict="session_id").execute()
            return
        except Exception as e:
            error_code = getattr(e, "code", None)
            if error_code != "42P10":
                logger.exception(
                    "[AUDIT] validation_history upsert failed for session %s: %s", session_id, e
                )
                raise
            logger.warning(
                "[AUDIT] validation_history.session_id has no UNIQUE constraint (Postgrest 42P10) -- "
                "falling back to manual select+insert/update for session %s. Add "
                "`ALTER TABLE validation_history ADD CONSTRAINT validation_history_session_id_key "
                "UNIQUE (session_id);` to restore the fast upsert path.",
                session_id,
            )

        existing = (
            scoped_client.table("validation_history")
            .select("id")
            .eq("session_id", session_id)
            .limit(1)
            .execute()
        )
        if existing.data:
            scoped_client.table("validation_history").update(row).eq("session_id", session_id).execute()
        else:
            scoped_client.table("validation_history").insert(row).execute()