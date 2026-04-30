from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from db import (
    PERSISTENT_DB_BACKUPS_DIR,
    PERSISTENT_OZON_BACKUPS_DIR,
    get_active_db_path,
)


def _timestamp_slug() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_metadata(path: Path, metadata: dict[str, Any]) -> None:
    meta_path = path.with_suffix(path.suffix + ".json")
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")


def backup_database_file(reason: str = "manual") -> dict[str, Any]:
    source_path = get_active_db_path()
    if not source_path:
        return {"ok": False, "message": "active db path is not defined"}
    source = Path(str(source_path))
    if not source.exists():
        return {"ok": False, "message": f"db file not found: {source}"}

    backup_dir = _ensure_dir(PERSISTENT_DB_BACKUPS_DIR)
    stamp = _timestamp_slug()
    safe_reason = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(reason or "manual")).strip("_") or "manual"
    target = backup_dir / f"catalog_{stamp}_{safe_reason}.db"
    latest = backup_dir / "catalog_latest.db"
    shutil.copy2(source, target)
    shutil.copy2(source, latest)
    metadata = {
        "kind": "database",
        "reason": safe_reason,
        "source_path": str(source),
        "created_at": datetime.utcnow().isoformat(timespec="seconds"),
        "size_bytes": int(target.stat().st_size),
    }
    _write_metadata(target, metadata)
    _write_metadata(latest, metadata)
    return {
        "ok": True,
        "path": str(target),
        "latest_path": str(latest),
        "size_bytes": int(target.stat().st_size),
    }


def backup_ozon_snapshot_bytes(
    snapshot_bytes: bytes,
    *,
    include_value_cache: bool = False,
    source: str = "manual",
) -> dict[str, Any]:
    payload = bytes(snapshot_bytes or b"")
    if not payload:
        return {"ok": False, "message": "snapshot payload is empty"}

    backup_dir = _ensure_dir(PERSISTENT_OZON_BACKUPS_DIR)
    stamp = _timestamp_slug()
    safe_source = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(source or "manual")).strip("_") or "manual"
    target = backup_dir / f"ozon_cache_{stamp}_{safe_source}.xlsx"
    latest = backup_dir / "ozon_cache_latest.xlsx"
    target.write_bytes(payload)
    latest.write_bytes(payload)
    metadata = {
        "kind": "ozon_cache",
        "source": safe_source,
        "include_value_cache": 1 if bool(include_value_cache) else 0,
        "created_at": datetime.utcnow().isoformat(timespec="seconds"),
        "size_bytes": int(target.stat().st_size),
    }
    _write_metadata(target, metadata)
    _write_metadata(latest, metadata)
    return {
        "ok": True,
        "path": str(target),
        "latest_path": str(latest),
        "size_bytes": int(target.stat().st_size),
        "include_value_cache": bool(include_value_cache),
    }


def list_ozon_snapshot_backups(limit: int = 12) -> list[dict[str, Any]]:
    backup_dir = _ensure_dir(PERSISTENT_OZON_BACKUPS_DIR)
    rows: list[dict[str, Any]] = []
    files = sorted(
        [p for p in backup_dir.glob("ozon_cache_*.xlsx") if p.is_file() and p.name != "ozon_cache_latest.xlsx"],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in files[: max(1, int(limit))]:
        meta_path = path.with_suffix(path.suffix + ".json")
        meta: dict[str, Any] = {}
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
        rows.append(
            {
                "file_name": path.name,
                "file_path": str(path),
                "created_at": meta.get("created_at"),
                "include_value_cache": bool(meta.get("include_value_cache")),
                "source": meta.get("source"),
                "size_bytes": int(meta.get("size_bytes") or path.stat().st_size),
            }
        )
    return rows


def read_backup_bytes(file_path: str | Path) -> bytes | None:
    path = Path(str(file_path))
    if not path.exists() or not path.is_file():
        return None
    try:
        return path.read_bytes()
    except Exception:
        return None
