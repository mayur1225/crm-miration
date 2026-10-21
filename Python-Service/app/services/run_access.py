"""
Who may see / resume / edit a run.

  * the user who started it (always, while they are still connected to that org pair), and
  * ANY other user whose CURRENT source+target connections point at the same orgs the run was started on.

Run files live in runs/<owner_id>/<run_id>.db, so a colleague is matched by org identity, never by
guessing ids. A run with no stored org identity (started before this feature) stays owner-only.
"""
import logging
import os
from typing import Any, Dict, List

from app.services.crm_service import CrmService
from app.services.org_identity import org_aliases
from app.services.store import BASE_RUNS_DIR, RunForbidden, RunNotFound, RunStore

logger = logging.getLogger(__name__)


class RunAccess:
    @staticmethod
    def user_orgs(user_id: str) -> Dict[str, Dict[str, Any]]:
        """{'source': {'crm','key'}, 'target': {...}} from the user's saved connections."""
        out: Dict[str, Dict[str, Any]] = {}
        for row in CrmService.get_user_connections(user_id) or []:
            role = (row.get("connection_role") or "").lower()
            aliases = org_aliases(row.get("crm_type"), row)
            out[role] = {
                "crm": (row.get("crm_type") or "").lower(),
                "key": aliases[0] if aliases else None,
                "aliases": aliases,
            }
        return out

    @staticmethod
    def _run_aliases(meta: Dict[str, Any], side: str) -> set:
        keys = set(meta.get(f"{side}OrgAliases") or [])
        if meta.get(f"{side}Org"):
            keys.add(meta[f"{side}Org"])
        return keys

    @classmethod
    def _matches(cls, meta: Dict[str, Any], orgs: Dict[str, Dict[str, Any]]) -> bool:
        """Same org = the run and the user's connection share at least one identity alias."""
        tgt = cls._run_aliases(meta, "target")
        if not tgt or not (tgt & set((orgs.get("target") or {}).get("aliases") or [])):
            return False
        src = cls._run_aliases(meta, "source")  # empty for CSV sources
        if src and not (src & set((orgs.get("source") or {}).get("aliases") or [])):
            return False
        return True

    @classmethod
    def open(cls, user_id: str, run_id: str) -> RunStore:
        """Owner first; otherwise a colleague connected to the same org(s). Raises RunNotFound / RunForbidden."""
        try:
            return RunStore.open(user_id, run_id)
        except (RunNotFound, RunForbidden):
            pass
        store = RunStore.open_shared(run_id, user_id)
        try:
            if not cls._matches(store.run_meta(), cls.user_orgs(user_id)):
                raise RunForbidden(run_id)
        except BaseException:
            store.close()
            raise
        return store

    @classmethod
    def recoverable(cls, user_id: str, max_age_days: int = 14, limit: int = 20) -> List[Dict[str, Any]]:
        """Runs on this user's connected org pair that still need attention (own runs and colleagues')."""
        orgs = cls.user_orgs(user_id)
        if not orgs.get("target"):
            logger.info("[RECOVERY] user %s has no saved target connection, so no runs can be matched yet.", user_id)
            return []
        logger.info("[RECOVERY] user %s connected to target=%s source=%s; scanning %s",
                    user_id, (orgs.get("target") or {}).get("key"), (orgs.get("source") or {}).get("key"), BASE_RUNS_DIR)

        candidates: List[Dict[str, Any]] = []
        replaced: set = set()  
        for path in RunStore.recent_run_paths(max_age_days):
            run_id = os.path.basename(path)[:-3]
            try:
                store = RunStore.open_shared(run_id, user_id)
            except Exception:
                logger.warning("[RECOVERY] cannot open %s", path, exc_info=True)
                continue
            try:
                meta = store.run_meta()
                mine = store.user_id == str(user_id)
                if cls._run_aliases(meta, "target"):
                    if not cls._matches(meta, orgs):
                        logger.info("[RECOVERY] skip %s: started on %s / %s, you are connected to %s / %s",
                                    run_id, meta.get("sourceOrg"), meta.get("targetOrg"),
                                    (orgs.get("source") or {}).get("key"), (orgs.get("target") or {}).get("key"))
                        continue
                elif not mine:
                    logger.info("[RECOVERY] skip %s: no org identity stored and it belongs to another user.", run_id)
                    continue  # legacy run / CRM without a stable org id: owner only
                summary = store.summary()
                if summary.get("parentRunId"):
                    replaced.add(summary["parentRunId"])
                actionable = summary["resumable"] or summary["retryable"] or summary["status"] == "running"
                if not actionable or not (summary["counts"] or summary["status"] == "running"):
                    logger.debug("[RECOVERY] skip %s: status=%s nothing to resume or retry.", run_id, summary["status"])
                    continue
                summary["isMine"] = mine
                candidates.append(summary)
            except Exception:
                logger.exception("Skipping unreadable run %s", run_id)
            finally:
                store.close()

        out = [r for r in candidates if r["runId"] not in replaced]
        out.sort(key=lambda r: r["updatedAt"] or "", reverse=True)
        logger.info("[RECOVERY] user %s: %d recoverable run(s).", user_id, len(out))
        return out[:limit]