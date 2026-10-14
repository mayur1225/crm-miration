"""
Pure helpers for resuming / retrying runs safely (no web framework imports, unit-testable).

  * org_mismatch(...)       refuse to resume a run against a different org than it was started on
  * capture_reference_raw / refresh_references
        Child rows are frozen with reference fields already remapped to target ids. If the parent failed, that
        remap had nothing to map to. When the parent is retried and succeeds, the child's reference must be
        remapped AGAIN from the original source value, or the retried child fails the same way. These helpers
        keep the original reference values and redo the remap for re-queued rows, never touching fields a
        user edited by hand.
"""
from typing import Any, Awaitable, Callable, Dict, List, Optional

from app.services.org_identity import org_key, org_label


def org_mismatch(meta: Dict[str, Any], source_crm: str, source_creds: Optional[Dict[str, Any]],
                 target_crm: str, target_creds: Dict[str, Any]) -> Optional[str]:
    """Message if the CURRENT connections are not the orgs the run was started on, else None."""
    checks = [("target", meta.get("targetOrg"), org_key(target_crm, target_creds))]
    if meta.get("sourceOrg") and source_creds is not None:
        checks.append(("source", meta.get("sourceOrg"), org_key(source_crm, source_creds)))
    for side, saved, current in checks:
        if saved and current != saved:
            return (
                f"You are connected to a different {side} org than this run was started on "
                f"(run: {org_label(saved)}; now: {org_label(current)}). Log in to the original {side} org and try again."
            )
    return None


def reference_source_fields(mappings: List[Dict[str, Any]]) -> List[str]:
    return [
        (m.get("sourceField") or m.get("csvField"))
        for m in mappings
        if m.get("type") == "reference" and (m.get("sourceField") or m.get("csvField"))
    ]


def capture_reference_raw(source_records: List[Dict[str, Any]], mappings: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Original (pre-remap) reference values per row. Call BEFORE the id remap."""
    fields = reference_source_fields(mappings)
    if not fields:
        return {}
    out: Dict[int, Dict[str, Any]] = {}
    for i, rec in enumerate(source_records):
        kept = {f: rec[f] for f in fields if f in rec}
        if kept:
            out[i] = kept
    return out


async def refresh_references(
    store: Any, job_index: int, mappings: List[Dict[str, Any]], source_records: List[Dict[str, Any]],
    remap: Callable[[List[Dict[str, Any]]], Awaitable[None]],
) -> int:
    """Re-run the id remap for this job's re-queued (pending) rows. Returns how many rows changed."""
    raw = store.load_ref_raw(job_index)
    if not raw:
        return 0
    fields = reference_source_fields(mappings)
    edited = store.edited_fields(job_index)
    idxs = [i for i in store.pending_row_idxs(job_index) if i in raw and i < len(source_records)]
    if not idxs:
        return 0

    probe: List[Dict[str, Any]] = []
    for i in idxs:
        row = dict(raw[i])
        for key in ("Id", "id"):
            if key in source_records[i]:
                row[key] = source_records[i][key]
        probe.append(row)
    await remap(probe)

    updates: Dict[int, Dict[str, Any]] = {}
    for i, row in zip(idxs, probe):
        changed = False
        for f in fields:
            if f in edited.get(i, ()):
                continue  # a human fixed this value; keep it
            if f in row and row[f] != source_records[i].get(f):
                source_records[i][f] = row[f]
                changed = True
        if changed:
            updates[i] = source_records[i]
    store.update_extracted_rows(job_index, updates)
    return len(updates)