from __future__ import annotations

import sqlite3
from datetime import datetime
from difflib import SequenceMatcher


def _normalize(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).strip().lower().split())


def _ratio(left: str | None, right: str | None) -> float:
    left_n = _normalize(left)
    right_n = _normalize(right)
    if not left_n or not right_n:
        return 0.0
    return SequenceMatcher(None, left_n, right_n).ratio()


def refresh_duplicates_for_product(
    conn: sqlite3.Connection,
    product_id: int,
    threshold: float = 0.86,
) -> list[dict]:
    product = conn.execute(
        """
        SELECT id, article, name, normalized_name, category, barcode
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
        SELECT id, article, name, normalized_name, category, barcode
        FROM products
        WHERE id != ?
        """,
        (product_id,),
    ).fetchall()

    candidates: list[dict] = []
    now = datetime.utcnow().isoformat(timespec="seconds")

    product_article = _normalize(product["article"])
    product_name = _normalize(product["name"])
    product_norm_name = _normalize(product["normalized_name"])
    product_category = _normalize(product["category"])
    product_barcode = _normalize(product["barcode"])

    for row in rows:
        reason = None
        score = 0.0

        row_article = _normalize(row["article"])
        row_name = _normalize(row["name"])
        row_norm_name = _normalize(row["normalized_name"])
        row_category = _normalize(row["category"])
        row_barcode = _normalize(row["barcode"])

        # 1. Штрихкод — самый сильный сигнал
        if product_barcode and row_barcode and product_barcode == row_barcode:
            reason = "exact barcode match"
            score = 1.0

        # 2. Полное совпадение артикула
        elif product_article and row_article and product_article == row_article:
            reason = "exact article match"
            score = 0.99

        # 3. Артикул встретился в названии
        elif product_article and row_name and product_article in row_name:
            reason = "article mentioned in title"
            score = 0.93

        # 4. Очень похожее нормализованное имя
        else:
            name_ratio = _ratio(product_norm_name or product_name, row_norm_name or row_name)
            if name_ratio >= threshold:
                reason = "similar normalized name"
                score = name_ratio
            elif product_category and row_category and product_category == row_category:
                # В одной категории можно снизить порог
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
        """
        UPDATE products
        SET duplicate_status = ?, updated_at = ?
        WHERE id = ?
        """,
        (duplicate_status, now, product_id),
    )

    conn.commit()
    return candidates
