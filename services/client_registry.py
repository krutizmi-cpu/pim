from __future__ import annotations

import sqlite3
from datetime import datetime


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _ensure_client_channels_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS client_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT NOT NULL,
            client_name TEXT,
            is_active INTEGER DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_client_channels_code_unique ON client_channels(client_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_client_channels_active ON client_channels(is_active)")
    conn.commit()


def upsert_client_channel(
    conn: sqlite3.Connection,
    *,
    client_code: str,
    client_name: str | None = None,
    notes: str | None = None,
    is_active: int = 1,
) -> int:
    _ensure_client_channels_table(conn)
    code = str(client_code or "").strip()
    if not code:
        raise ValueError("client_code is required")
    name = str(client_name or "").strip() or None
    note_text = str(notes or "").strip() or None
    now = _now()
    existing = conn.execute(
        "SELECT id, client_name, notes FROM client_channels WHERE client_code = ? LIMIT 1",
        (code,),
    ).fetchone()
    if existing:
        row_id = int(existing["id"])
        conn.execute(
            """
            UPDATE client_channels
            SET client_name = COALESCE(NULLIF(?, ''), client_name),
                notes = COALESCE(NULLIF(?, ''), notes),
                is_active = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (name, note_text, int(is_active), now, row_id),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO client_channels (client_code, client_name, is_active, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (code, name, int(is_active), note_text, now, now),
        )
        row_id = int(cur.lastrowid)
    conn.commit()
    return row_id


def list_client_channels(conn: sqlite3.Connection, include_inferred: bool = True) -> list[dict]:
    _ensure_client_channels_table(conn)
    rows = conn.execute(
        """
        SELECT *
        FROM client_channels
        WHERE COALESCE(is_active, 1) = 1
        ORDER BY COALESCE(client_name, client_code), client_code
        """
    ).fetchall()
    items = [dict(row) for row in rows]
    if not include_inferred:
        return items

    seen = {str(item.get("client_code") or "").strip() for item in items if str(item.get("client_code") or "").strip()}
    inferred_map: dict[str, dict] = {}
    sql_parts = [
        "SELECT DISTINCT channel_code AS client_code, NULL AS client_name, 'template_profiles' AS source_name FROM template_profiles WHERE TRIM(COALESCE(channel_code, '')) <> ''",
        "SELECT DISTINCT channel_code AS client_code, NULL AS client_name, 'uploaded_files' AS source_name FROM uploaded_files WHERE TRIM(COALESCE(channel_code, '')) <> ''",
        "SELECT DISTINCT channel_code AS client_code, NULL AS client_name, 'channel_mapping_rules' AS source_name FROM channel_mapping_rules WHERE TRIM(COALESCE(channel_code, '')) <> ''",
        "SELECT DISTINCT channel_code AS client_code, NULL AS client_name, 'channel_attribute_requirements' AS source_name FROM channel_attribute_requirements WHERE TRIM(COALESCE(channel_code, '')) <> ''",
    ]
    union_sql = " UNION ".join(sql_parts)
    try:
        inferred_rows = conn.execute(union_sql).fetchall()
    except Exception:
        inferred_rows = []

    for row in inferred_rows:
        code = str(row["client_code"] or "").strip()
        if not code or code in seen:
            continue
        inferred_map[code] = {
            "id": None,
            "client_code": code,
            "client_name": None,
            "is_active": 1,
            "notes": None,
            "created_at": None,
            "updated_at": None,
            "is_inferred": 1,
        }

    return sorted(
        items + list(inferred_map.values()),
        key=lambda item: (
            str(item.get("client_name") or item.get("client_code") or "").lower(),
            str(item.get("client_code") or "").lower(),
        ),
    )
