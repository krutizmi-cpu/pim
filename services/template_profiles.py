from __future__ import annotations

import sqlite3
from datetime import datetime


def save_template_profile(
    conn: sqlite3.Connection,
    profile_name: str,
    channel_code: str,
    category_code: str | None,
    file_name: str | None,
    columns: list[dict],
) -> int:
    now = datetime.utcnow().isoformat(timespec="seconds")
    existing = conn.execute(
        """
        SELECT id FROM template_profiles
        WHERE profile_name = ? AND channel_code = ? AND IFNULL(category_code, '') = IFNULL(?, '')
        """,
        (profile_name, channel_code, category_code),
    ).fetchone()

    if existing:
        profile_id = int(existing["id"])
        conn.execute(
            """
            UPDATE template_profiles
            SET file_name = ?, updated_at = ?
            WHERE id = ?
            """,
            (file_name, now, profile_id),
        )
        conn.execute("DELETE FROM template_profile_columns WHERE profile_id = ?", (profile_id,))
    else:
        cur = conn.execute(
            """
            INSERT INTO template_profiles (profile_name, channel_code, category_code, file_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (profile_name, channel_code, category_code, file_name, now, now),
        )
        profile_id = int(cur.lastrowid)

    for idx, col in enumerate(columns, start=1):
        conn.execute(
            """
            INSERT INTO template_profile_columns
            (profile_id, template_column, source_type, source_name, matched_by, sort_order, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile_id,
                col.get("template_column"),
                col.get("source_type"),
                col.get("source_name"),
                col.get("matched_by"),
                idx,
                now,
                now,
            ),
        )

    conn.commit()
    return profile_id


def list_template_profiles(conn: sqlite3.Connection, channel_code: str | None = None) -> list[dict]:
    if channel_code:
        rows = conn.execute(
            "SELECT * FROM template_profiles WHERE channel_code = ? ORDER BY updated_at DESC, id DESC",
            (channel_code,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM template_profiles ORDER BY updated_at DESC, id DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_template_profile_columns(conn: sqlite3.Connection, profile_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM template_profile_columns WHERE profile_id = ? ORDER BY sort_order, id",
        (int(profile_id),),
    ).fetchall()
    return [dict(r) for r in rows]
