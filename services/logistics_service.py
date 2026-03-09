from __future__ import annotations

import sqlite3
from datetime import datetime
from difflib import SequenceMatcher


def estimate_logistics(conn: sqlite3.Connection, product_id: int) -> dict | None:
    product = conn.execute(
        """
        SELECT id, name, normalized_name, category, package_length, package_width, package_height, gross_weight
        FROM products
        WHERE id = ?
        """,
        (product_id,),
    ).fetchone()
    if not product:
        return None

    rows = conn.execute(
        """
        SELECT id, name, normalized_name, package_length, package_width, package_height, gross_weight
        FROM products
        WHERE id != ?
          AND category = ?
          AND package_length IS NOT NULL
          AND package_width IS NOT NULL
          AND package_height IS NOT NULL
          AND gross_weight IS NOT NULL
        LIMIT 3000
        """,
        (product_id, product["category"]),
    ).fetchall()

    scored = []
    for row in rows:
        ratio = SequenceMatcher(None, product["normalized_name"] or "", row["normalized_name"] or "").ratio()
        if ratio >= 0.60:
            scored.append((ratio, row))

    if not scored:
        return None

    scored.sort(key=lambda x: x[0], reverse=True)
    top = [row for _, row in scored[:10]]

    avg = {
        "package_length": round(sum(float(r["package_length"]) for r in top) / len(top), 2),
        "package_width": round(sum(float(r["package_width"]) for r in top) / len(top), 2),
        "package_height": round(sum(float(r["package_height"]) for r in top) / len(top), 2),
        "gross_weight": round(sum(float(r["gross_weight"]) for r in top) / len(top), 3),
        "matched_count": len(top),
    }

    now = datetime.utcnow().isoformat(timespec="seconds")
    conn.execute(
        """
        UPDATE products
        SET package_length = COALESCE(package_length, ?),
            package_width = COALESCE(package_width, ?),
            package_height = COALESCE(package_height, ?),
            gross_weight = COALESCE(gross_weight, ?),
            is_estimated_logistics = 1,
            updated_at = ?
        WHERE id = ?
        """,
        (
            avg["package_length"],
            avg["package_width"],
            avg["package_height"],
            avg["gross_weight"],
            now,
            product_id,
        ),
    )
    conn.commit()
    return avg
