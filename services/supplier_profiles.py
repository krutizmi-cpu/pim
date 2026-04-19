from __future__ import annotations

import sqlite3
from typing import Any


DEFAULT_SUPPLIER_PROFILES: list[dict[str, Any]] = [
    {
        "supplier_name": "Рокви",
        "base_url": "https://velocitygroup.ru/catalog/",
        "url_template": "https://velocitygroup.ru/catalog/?q={supplier_article_q}",
        "notes": "Поиск по артикулу поставщика",
        "is_active": 1,
    },
]


def ensure_default_supplier_profiles(conn: sqlite3.Connection) -> int:
    created_or_updated = 0
    for item in DEFAULT_SUPPLIER_PROFILES:
        supplier_name = str(item.get("supplier_name") or "").strip()
        existing = conn.execute(
            "SELECT id, base_url, url_template, notes, is_active FROM supplier_profiles WHERE supplier_name = ? LIMIT 1",
            (supplier_name,),
        ).fetchone()
        desired_base = item.get("base_url") or None
        desired_template = item.get("url_template") or None
        desired_notes = item.get("notes") or None
        desired_active = int(item.get("is_active") or 1)
        if not existing:
            upsert_supplier_profile(
                conn=conn,
                supplier_name=supplier_name,
                base_url=desired_base,
                url_template=desired_template,
                notes=desired_notes,
                is_active=desired_active,
            )
            created_or_updated += 1
            continue
        need_update = (
            (existing["base_url"] or None) != desired_base
            or (existing["url_template"] or None) != desired_template
            or (existing["notes"] or None) != desired_notes
            or int(existing["is_active"] or 1) != desired_active
        )
        if need_update:
            upsert_supplier_profile(
                conn=conn,
                supplier_name=supplier_name,
                base_url=desired_base,
                url_template=desired_template,
                notes=desired_notes,
                is_active=desired_active,
            )
            created_or_updated += 1
    return created_or_updated


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
