"""
JSONL checkpoint logging, backup, and resume utilities.
Ported from the ECHR Lawyers pipeline pattern.
"""
import json
import os
import shutil
import logging
from datetime import datetime
from typing import Set, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_BACKUP_DIR = "backups"


def backup_file_if_exists(
    file_path: str,
    backup_dir: Optional[str] = None,
    max_backups: int = 10,
    min_size_bytes: int = 100,
) -> Optional[str]:
    """
    Create a timestamped backup of a file before modifying it.
    Returns path to backup file, or None if no backup was made.
    """
    if not os.path.exists(file_path):
        return None

    file_size = os.path.getsize(file_path)
    if file_size < min_size_bytes:
        return None

    if backup_dir is None:
        file_dir = os.path.dirname(file_path) or "."
        backup_dir = os.path.join(file_dir, DEFAULT_BACKUP_DIR)

    os.makedirs(backup_dir, exist_ok=True)

    base_name = os.path.basename(file_path)
    name_part, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{name_part}_{timestamp}{ext}"
    backup_path = os.path.join(backup_dir, backup_name)

    try:
        shutil.copy2(file_path, backup_path)
        logger.info(f"Created backup: {backup_path} ({file_size:,} bytes)")
    except Exception as e:
        logger.warning(f"Failed to create backup of {file_path}: {e}")
        return None

    _cleanup_old_backups(backup_dir, name_part, ext, max_backups)
    return backup_path


def _cleanup_old_backups(backup_dir: str, name_prefix: str, extension: str, max_backups: int):
    """Remove oldest backups if we exceed max_backups count."""
    try:
        pattern_prefix = f"{name_prefix}_"
        backups = []
        for f in os.listdir(backup_dir):
            if f.startswith(pattern_prefix) and f.endswith(extension):
                full_path = os.path.join(backup_dir, f)
                if os.path.isfile(full_path):
                    backups.append((os.path.getmtime(full_path), full_path))
        backups.sort()
        while len(backups) > max_backups:
            _, oldest_path = backups.pop(0)
            try:
                os.remove(oldest_path)
            except Exception:
                pass
    except Exception:
        pass


def load_existing_log(log_path: str, id_field: str = "celex") -> Set[str]:
    """
    Load processed item IDs from a JSONL log file.
    Returns a set of IDs that have already been processed (for resume).
    """
    processed_ids = set()
    if not os.path.exists(log_path):
        return processed_ids

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    iid = record.get(id_field) or record.get("id") or record.get("celex")
                    if iid:
                        processed_ids.add(str(iid))
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.warning(f"Error reading log file {log_path}: {e}")

    logger.info(f"Loaded {len(processed_ids)} processed IDs from {log_path}")
    return processed_ids


def append_log(log_path: str, record: Dict):
    """Append a single record to the JSONL log file."""
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except Exception as e:
        logger.error(f"Failed to append to log {log_path}: {e}")


def batch_append_log(log_path: str, records: list):
    """Append a batch of records to the JSONL log file."""
    if not records:
        return
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            lines = [json.dumps(r, ensure_ascii=False, default=str) for r in records]
            f.write("\n".join(lines) + "\n")
    except Exception as e:
        logger.error(f"Failed to batch append to log {log_path}: {e}")
