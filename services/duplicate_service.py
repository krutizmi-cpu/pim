from __future__ import annotations

import sqlite3
from datetime import datetime
from difflib import SequenceMatcher


def _ratio(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def refresh_duplicates_for_product(conn: sqlite3.Connection, product_id: int, threshold: float = 0.86) -> list[dict]:
    product = conn.execute(
        """
        SELECT id, article, name, normalized_name, category
        FROM products
        WHERE id = ?
        """,
        (product_id,),
    ).fetchone()
    if not product:
        return []

    conn.execute(
        "DELETE FROM duplicate_candidates WHERE product_id_1 = ? OR product_id_2 = ?",
        (product_id, product_id),
    )

    rows = conn.execute(
        """
        SELECT id, article, name, normalized_name, category
        FROM products
        WHERE id != ?
        """,
        (product_id,),
    ).fetchall()

    candidates: list[dict] = []
    now = datetime.utcnow().isoformat(timespec="seconds")

    for row in rows:
        reason = None
        score = 0.0

        if product["article"] and row["article"] and product["article"].strip().lower() == row["article"].strip().lower():
            reason = "exact article match"
            score = 1.0
        elif product["article"] and row["name"] and product["article"].lower() in row["name"].lower():
            reason = "article mentioned in title"
            score = 0.93
        else:
            name_ratio = _ratio(product["normalized_name"], row["normalized_name"])
            if name_ratio >= threshold:
                reason = "similar normalized name"
                score = name_ratio
            elif product["category"] and row["category"] and product["category"] == row["category"]:
                name_ratio = _ratio(product["normalized_name"], row["normalized_name"])
                if name_ratio >= 0.78:
                    reason = "same category and similar title"
                    score = name_ratio

        if not reason:
            continue

        p1, p2 = sorted([int(product["id"]), int(row["id"])])
        conn.execute(
            """
            INSERT OR REPLACE INTO duplicate_candidates
            (product_id_1, product_id_2, similarity_score, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (p1, p2, round(score, 4), reason, now),
        )
        candidates.append(
            {
                "product_id_1": p1,
                "product_id_2": p2,
                "similarity_score": round(score * 100, 2),
                "reason": reason,
            }
        )

    duplicate_status = "suspected" if candidates else None
    conn.execute(
        "UPDATE products SET duplicate_status = ?, updated_at = ? WHERE id = ?",
        (duplicate_status, now, product_id),
    )
    conn.commit()
    return candidates
