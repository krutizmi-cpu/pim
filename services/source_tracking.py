from __future__ import annotations

import sqlite3
from typing import Any


def save_field_source(
    conn: sqlite3.Connection,
    product_id: int,
    field_name: str,
    source_type: str,
    source_value_raw: Any = None,
    source_url: str | None = None,
    confidence: float | None = None,
    is_manual: bool = False,
) -> None:
    conn.execute(
        """
        INSERT INTO product_data_sources (
            product_id,
            field_name,
            source_type,
            source_value_raw,
            source_url,
            confidence,
            is_manual
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(product_id),
            str(field_name),
            str(source_type),
            None if source_value_raw is None else str(source_value_raw),
            source_url,
            confidence,
            1 if is_manual else 0,
        ),
    )
    conn.commit()


def get_field_sources(conn: sqlite3.Connection, product_id: int) -> list[dict]:
    rows = conn.execute(
        """
        SELECT *
        FROM product_data_sources
        WHERE product_id = ?
        ORDER BY created_at DESC, id DESC
        """,
        (int(product_id),),
    ).fetchall()
    return [dict(r) for r in rows]


def get_latest_field_source(conn: sqlite3.Connection, product_id: int, field_name: str) -> dict | None:
    row = conn.execute(
        """
        SELECT *
        FROM product_data_sources
        WHERE product_id = ? AND field_name = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (int(product_id), str(field_name)),
    ).fetchone()
    return dict(row) if row else None


def field_is_manual(conn: sqlite3.Connection, product_id: int, field_name: str) -> bool:
    row = get_latest_field_source(conn, product_id, field_name)
    return bool(row and int(row.get("is_manual") or 0) == 1)
