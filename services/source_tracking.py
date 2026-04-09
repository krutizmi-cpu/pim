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
) -> None:
    conn.execute(
        """
        INSERT INTO product_data_sources (
            product_id,
            field_name,
            source_type,
            source_value_raw,
            source_url,
            confidence
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            int(product_id),
            str(field_name),
            str(source_type),
            None if source_value_raw is None else str(source_value_raw),
            source_url,
            confidence,
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
