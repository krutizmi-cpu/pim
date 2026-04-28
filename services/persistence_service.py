from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from db import PERSISTENT_IMPORTS_DIR, PERSISTENT_TEMPLATES_DIR, PERSISTENT_UPLOADS_DIR


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _sanitize_name(name: str | None, fallback: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        raw = fallback
    raw = re.sub(r"[^\w\.\-]+", "_", raw, flags=re.UNICODE).strip("._")
    return raw or fallback


def _ensure_upload_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS uploaded_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            storage_kind TEXT NOT NULL,
            file_hash TEXT NOT NULL,
            original_file_name TEXT,
            stored_rel_path TEXT NOT NULL,
            channel_code TEXT,
            category_code TEXT,
            batch_id TEXT,
            metadata_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_uploaded_files_unique
        ON uploaded_files(storage_kind, file_hash)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_import_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            uploaded_file_id INTEGER,
            original_file_name TEXT,
            supplier_name TEXT,
            supplier_url_template TEXT,
            selected_sheet TEXT,
            header_row INTEGER,
            imported_count INTEGER DEFAULT 0,
            created_count INTEGER DEFAULT 0,
            updated_count INTEGER DEFAULT 0,
            duplicates_count INTEGER DEFAULT 0,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_import_batch_unique ON catalog_import_history(batch_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_connection_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_name TEXT NOT NULL,
            provider TEXT NOT NULL,
            base_url TEXT,
            chat_model TEXT,
            image_model TEXT,
            api_key TEXT,
            use_env_api_key INTEGER DEFAULT 1,
            temperature REAL DEFAULT 0.3,
            max_tokens INTEGER DEFAULT 1800,
            image_size TEXT,
            openrouter_referer TEXT,
            openrouter_title TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_profiles_name_unique ON ai_connection_profiles(profile_name)")
    conn.commit()


def _storage_dir_for_kind(storage_kind: str) -> Path:
    kind = str(storage_kind or "").strip().lower()
    if kind == "client_template":
        return PERSISTENT_TEMPLATES_DIR
    if kind == "supplier_catalog":
        return PERSISTENT_IMPORTS_DIR
    return PERSISTENT_UPLOADS_DIR / kind


def persist_uploaded_file(
    conn: sqlite3.Connection,
    storage_kind: str,
    original_file_name: str | None,
    file_bytes: bytes,
    *,
    channel_code: str | None = None,
    category_code: str | None = None,
    batch_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _ensure_upload_tables(conn)
    payload = bytes(file_bytes or b"")
    if not payload:
        raise ValueError("empty upload payload")
    file_hash = hashlib.sha256(payload).hexdigest()
    storage_dir = _storage_dir_for_kind(storage_kind)
    storage_dir.mkdir(parents=True, exist_ok=True)
    safe_name = _sanitize_name(original_file_name, f"{storage_kind}.bin")
    stored_name = f"{file_hash[:16]}_{safe_name}"
    stored_path = storage_dir / stored_name
    if not stored_path.exists():
        stored_path.write_bytes(payload)

    rel_path = str(stored_path)
    now = _now()
    metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
    existing = conn.execute(
        """
        SELECT *
        FROM uploaded_files
        WHERE storage_kind = ?
          AND file_hash = ?
        LIMIT 1
        """,
        (str(storage_kind), file_hash),
    ).fetchone()

    if existing:
        row_id = int(existing["id"])
        conn.execute(
            """
            UPDATE uploaded_files
            SET original_file_name = ?,
                stored_rel_path = ?,
                channel_code = COALESCE(?, channel_code),
                category_code = COALESCE(?, category_code),
                batch_id = COALESCE(?, batch_id),
                metadata_json = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                original_file_name,
                rel_path,
                channel_code,
                category_code,
                batch_id,
                metadata_json,
                now,
                row_id,
            ),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO uploaded_files (
                storage_kind,
                file_hash,
                original_file_name,
                stored_rel_path,
                channel_code,
                category_code,
                batch_id,
                metadata_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(storage_kind),
                file_hash,
                original_file_name,
                rel_path,
                channel_code,
                category_code,
                batch_id,
                metadata_json,
                now,
                now,
            ),
        )
        row_id = int(cur.lastrowid)
    conn.commit()
    return {
        "id": row_id,
        "file_hash": file_hash,
        "stored_path": rel_path,
        "original_file_name": original_file_name,
    }


def list_uploaded_files(
    conn: sqlite3.Connection,
    *,
    storage_kind: str | None = None,
    channel_code: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    _ensure_upload_tables(conn)
    where: list[str] = []
    params: list[Any] = []
    if storage_kind:
        where.append("storage_kind = ?")
        params.append(str(storage_kind))
    if channel_code:
        where.append("channel_code = ?")
        params.append(str(channel_code))
    sql = "SELECT * FROM uploaded_files"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY COALESCE(updated_at, created_at) DESC, id DESC LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def record_catalog_import_history(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    uploaded_file_id: int | None,
    original_file_name: str | None,
    supplier_name: str | None,
    supplier_url_template: str | None,
    selected_sheet: str | None,
    header_row: int | None,
    imported_count: int,
    created_count: int,
    updated_count: int,
    duplicates_count: int,
    notes: str | None = None,
) -> int:
    _ensure_upload_tables(conn)
    now = _now()
    existing = conn.execute(
        "SELECT id FROM catalog_import_history WHERE batch_id = ? LIMIT 1",
        (str(batch_id),),
    ).fetchone()
    if existing:
        row_id = int(existing["id"])
        conn.execute(
            """
            UPDATE catalog_import_history
            SET uploaded_file_id = ?,
                original_file_name = ?,
                supplier_name = ?,
                supplier_url_template = ?,
                selected_sheet = ?,
                header_row = ?,
                imported_count = ?,
                created_count = ?,
                updated_count = ?,
                duplicates_count = ?,
                notes = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                uploaded_file_id,
                original_file_name,
                supplier_name,
                supplier_url_template,
                selected_sheet,
                header_row,
                int(imported_count),
                int(created_count),
                int(updated_count),
                int(duplicates_count),
                notes,
                now,
                row_id,
            ),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO catalog_import_history (
                batch_id,
                uploaded_file_id,
                original_file_name,
                supplier_name,
                supplier_url_template,
                selected_sheet,
                header_row,
                imported_count,
                created_count,
                updated_count,
                duplicates_count,
                notes,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(batch_id),
                uploaded_file_id,
                original_file_name,
                supplier_name,
                supplier_url_template,
                selected_sheet,
                header_row,
                int(imported_count),
                int(created_count),
                int(updated_count),
                int(duplicates_count),
                notes,
                now,
                now,
            ),
        )
        row_id = int(cur.lastrowid)
    conn.commit()
    return row_id


def list_catalog_import_history(conn: sqlite3.Connection, limit: int = 30) -> list[dict[str, Any]]:
    _ensure_upload_tables(conn)
    rows = conn.execute(
        """
        SELECT h.*, u.stored_rel_path
        FROM catalog_import_history h
        LEFT JOIN uploaded_files u
          ON u.id = h.uploaded_file_id
        ORDER BY COALESCE(h.updated_at, h.created_at) DESC, h.id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def save_ai_connection_profile(conn: sqlite3.Connection, profile_name: str, settings: dict[str, Any]) -> int:
    _ensure_upload_tables(conn)
    name = str(profile_name or "").strip()
    if not name:
        raise ValueError("profile_name is required")
    now = _now()
    existing = conn.execute(
        "SELECT id FROM ai_connection_profiles WHERE profile_name = ? LIMIT 1",
        (name,),
    ).fetchone()
    payload = (
        str(settings.get("provider") or "openai").strip().lower(),
        str(settings.get("base_url") or "").strip(),
        str(settings.get("chat_model") or "").strip(),
        str(settings.get("image_model") or "").strip(),
        str(settings.get("api_key") or "").strip(),
        1 if bool(settings.get("use_env_api_key", True)) else 0,
        float(settings.get("temperature", 0.3) or 0.3),
        int(settings.get("max_tokens", 1800) or 1800),
        str(settings.get("image_size") or "1024x1024").strip(),
        str(settings.get("openrouter_referer") or "").strip(),
        str(settings.get("openrouter_title") or "pim").strip(),
    )
    if existing:
        row_id = int(existing["id"])
        conn.execute(
            """
            UPDATE ai_connection_profiles
            SET provider = ?,
                base_url = ?,
                chat_model = ?,
                image_model = ?,
                api_key = ?,
                use_env_api_key = ?,
                temperature = ?,
                max_tokens = ?,
                image_size = ?,
                openrouter_referer = ?,
                openrouter_title = ?,
                updated_at = ?
            WHERE id = ?
            """,
            payload + (now, row_id),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO ai_connection_profiles (
                profile_name,
                provider,
                base_url,
                chat_model,
                image_model,
                api_key,
                use_env_api_key,
                temperature,
                max_tokens,
                image_size,
                openrouter_referer,
                openrouter_title,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name,) + payload + (now, now),
        )
        row_id = int(cur.lastrowid)
    conn.commit()
    return row_id


def list_ai_connection_profiles(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    _ensure_upload_tables(conn)
    rows = conn.execute(
        """
        SELECT *
        FROM ai_connection_profiles
        ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def get_ai_connection_profile(conn: sqlite3.Connection, profile_id: int) -> dict[str, Any] | None:
    _ensure_upload_tables(conn)
    row = conn.execute(
        "SELECT * FROM ai_connection_profiles WHERE id = ? LIMIT 1",
        (int(profile_id),),
    ).fetchone()
    return dict(row) if row else None
