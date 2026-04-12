from __future__ import annotations

import sqlite3
from typing import Any


def list_supplier_profiles(conn: sqlite3.Connection, only_active: bool = True) -> list[dict[str, Any]]:
    if only_active:
        rows = conn.execute(
            """
            SELECT *
            FROM supplier_profiles
            WHERE IFNULL(is_active, 1) = 1
            ORDER BY supplier_name
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM supplier_profiles
            ORDER BY supplier_name
            """
        ).fetchall()
    return [dict(r) for r in rows]


def upsert_supplier_profile(
    conn: sqlite3.Connection,
    supplier_name: str,
    base_url: str | None = None,
    url_template: str | None = None,
    notes: str | None = None,
    is_active: int = 1,
) -> int:
    existing = conn.execute(
        "SELECT id FROM supplier_profiles WHERE supplier_name = ? LIMIT 1",
        (str(supplier_name).strip(),),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE supplier_profiles
            SET base_url = ?,
                url_template = ?,
                notes = ?,
                is_active = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                base_url or None,
                url_template or None,
                notes or None,
                int(is_active),
                int(existing["id"]),
            ),
        )
        conn.commit()
        return int(existing["id"])

    cur = conn.execute(
        """
        INSERT INTO supplier_profiles (
            supplier_name,
            base_url,
            url_template,
            notes,
            is_active,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (
            str(supplier_name).strip(),
            base_url or None,
            url_template or None,
            notes or None,
            int(is_active),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)

