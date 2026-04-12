from __future__ import annotations

import re
import sqlite3
from difflib import SequenceMatcher
from typing import Any

from services.source_tracking import save_field_source


def _normalize(text: str | None) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _tokenize(text: str | None) -> set[str]:
    tokens = re.findall(r"[a-zA-Zа-яА-Я0-9]+", _normalize(text))
    return {t for t in tokens if len(t) >= 2}


def _query_terms(product_row: dict[str, Any]) -> list[str]:
    candidates = [
        product_row.get("base_category"),
        product_row.get("subcategory"),
        product_row.get("category"),
        product_row.get("name"),
    ]
    out: list[str] = []
    seen = set()
    for value in candidates:
        norm = _normalize(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def _category_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT description_category_id, type_id, category_name, full_path, type_name
        FROM ozon_category_cache
        WHERE description_category_id IS NOT NULL
          AND type_id IS NOT NULL
          AND IFNULL(disabled, 0) = 0
        """
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (int(row["description_category_id"]), int(row["type_id"]))
        if key in seen:
            continue
        seen.add(key)
        text = _normalize(" ".join([str(row["full_path"] or ""), str(row["category_name"] or ""), str(row["type_name"] or "")]))
        candidates.append(
            {
                "description_category_id": int(row["description_category_id"]),
                "type_id": int(row["type_id"]),
                "full_path": row["full_path"] or row["category_name"] or "",
                "category_name": row["category_name"] or "",
                "text": text,
                "tokens": _tokenize(text),
            }
        )
    return candidates


def _score(query: str, query_tokens: set[str], candidate: dict[str, Any]) -> float:
    text = candidate["text"]
    if not text:
        return 0.0
    ratio = SequenceMatcher(None, query, text).ratio()
    overlap = 0.0
    if query_tokens:
        overlap = len(query_tokens & candidate["tokens"]) / max(len(query_tokens), 1)
    contains = 1.0 if query in text else 0.0
    return 0.55 * ratio + 0.35 * overlap + 0.10 * contains


def _best_match_for_product(product_row: dict[str, Any], categories: list[dict[str, Any]]) -> dict[str, Any] | None:
    terms = _query_terms(product_row)
    if not terms or not categories:
        return None
    best: dict[str, Any] | None = None
    best_score = 0.0
    best_term = None
    for term in terms:
        term_tokens = _tokenize(term)
        for category in categories:
            score = _score(term, term_tokens, category)
            if score > best_score:
                best_score = score
                best = category
                best_term = term
    if not best:
        return None
    return {
        "description_category_id": int(best["description_category_id"]),
        "type_id": int(best["type_id"]),
        "full_path": best["full_path"],
        "score": round(float(best_score), 4),
        "matched_by": best_term,
    }


def bulk_assign_ozon_categories(
    conn: sqlite3.Connection,
    product_ids: list[int],
    min_score: float = 0.28,
    force: bool = False,
) -> dict[str, Any]:
    categories = _category_candidates(conn)
    if not categories:
        return {"processed": 0, "assigned": 0, "skipped": 0, "message": "Кэш категорий Ozon пуст"}

    assigned = 0
    skipped = 0
    processed = 0

    for product_id in product_ids:
        row = conn.execute(
            """
            SELECT id, name, category, base_category, subcategory,
                   ozon_description_category_id, ozon_type_id, ozon_category_confidence
            FROM products
            WHERE id = ?
            LIMIT 1
            """,
            (int(product_id),),
        ).fetchone()
        if not row:
            continue
        processed += 1
        product = dict(row)
        existing_id = product.get("ozon_description_category_id")
        existing_type = product.get("ozon_type_id")
        existing_conf = float(product.get("ozon_category_confidence") or 0.0)
        if existing_id and existing_type and not force:
            skipped += 1
            continue

        match = _best_match_for_product(product, categories)
        if not match:
            skipped += 1
            continue
        if float(match["score"]) < float(min_score):
            skipped += 1
            continue
        if (existing_id and existing_type) and (not force) and existing_conf >= float(match["score"]):
            skipped += 1
            continue

        conn.execute(
            """
            UPDATE products
            SET ozon_description_category_id = ?,
                ozon_type_id = ?,
                ozon_category_path = ?,
                ozon_category_confidence = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                int(match["description_category_id"]),
                int(match["type_id"]),
                str(match["full_path"] or ""),
                float(match["score"]),
                int(product_id),
            ),
        )
        save_field_source(
            conn=conn,
            product_id=int(product_id),
            field_name="ozon_description_category_id",
            source_type="ozon_category_match",
            source_value_raw=int(match["description_category_id"]),
            source_url=str(match["full_path"] or ""),
            confidence=float(match["score"]),
            is_manual=False,
        )
        save_field_source(
            conn=conn,
            product_id=int(product_id),
            field_name="ozon_type_id",
            source_type="ozon_category_match",
            source_value_raw=int(match["type_id"]),
            source_url=str(match["full_path"] or ""),
            confidence=float(match["score"]),
            is_manual=False,
        )
        assigned += 1

    conn.commit()
    return {"processed": processed, "assigned": assigned, "skipped": skipped}

