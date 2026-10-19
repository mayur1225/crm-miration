import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)

BASE_STAGING_DIR = os.path.join(os.getcwd(), "SureShift_staging_databases")
DEFAULT_MAX_AGE_HOURS = 24 * 7
DEFAULT_SWEEP_INTERVAL_SECONDS = 3600  # sweep hourly; deletions still respect max_age_hours


def cleanup_stale_staging_databases(
    base_dir: str = BASE_STAGING_DIR,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
) -> dict:
    """
    Deletes staging .db files older than `max_age_hours` and prunes any
    now-empty {crm}/{object} folders left behind. Safe to call repeatedly or
    concurrently -- a file that disappears mid-scan (e.g. removed by another
    sweep, or downloaded/finished elsewhere) is treated as already-cleaned,
    not an error.
    """
    if not os.path.isdir(base_dir):
        return {"scanned": 0, "deleted": 0, "errors": 0}

    cutoff = time.time() - (max_age_hours * 3600)
    scanned = deleted = errors = 0

    with os.scandir(base_dir) as crm_folders:
        for crm_folder in crm_folders:
            if not crm_folder.is_dir():
                continue

            with os.scandir(crm_folder.path) as obj_folders:
                for obj_folder in obj_folders:
                    if not obj_folder.is_dir():
                        continue

                    with os.scandir(obj_folder.path) as entries:
                        for entry in entries:
                            if not entry.is_file() or not entry.name.endswith(".db"):
                                continue
                            scanned += 1
                            try:

                                stat = entry.stat()
                                created_at = min(stat.st_ctime, stat.st_mtime)
                                if created_at < cutoff:
                                    os.remove(entry.path)
                                    deleted += 1
                                    logger.info("Removed stale staging DB: %s", entry.path)
                            except FileNotFoundError:
                                continue
                            except OSError as exc:
                                errors += 1
                                logger.warning("Failed to remove %s: %s", entry.path, exc)

                    _prune_if_empty(obj_folder.path)

            _prune_if_empty(crm_folder.path)

    return {"scanned": scanned, "deleted": deleted, "errors": errors}


def _prune_if_empty(dir_path: str) -> None:
    try:
        if not os.listdir(dir_path):
            os.rmdir(dir_path)
    except OSError:
        pass


async def run_staging_cleanup_loop(
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    interval_seconds: float = DEFAULT_SWEEP_INTERVAL_SECONDS,
) -> None:
    """
    Background task: sweeps the staging dir every `interval_seconds` for the
    lifetime of the process, deleting anything older than `max_age_hours`.
    Register as an asyncio task from the app's lifespan/startup hook -- see
    main.py wiring below. A sweep failure is logged and never kills the loop.
    """
    while True:
        try:
            result = cleanup_stale_staging_databases(max_age_hours=max_age_hours)
            if result["deleted"] or result["errors"]:
                logger.info(
                    "Staging cleanup sweep: scanned=%d deleted=%d errors=%d",
                    result["scanned"], result["deleted"], result["errors"],
                )
        except Exception:
            logger.exception("Staging cleanup sweep crashed")
        await asyncio.sleep(interval_seconds)