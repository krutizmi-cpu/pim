from __future__ import annotations

import re
import sqlite3
from difflib import SequenceMatcher
from typing import Any

from services.source_tracking import save_field_source


STOP_TOKENS = {
    "и", "для", "с", "на", "в", "из", "по", "под", "или", "the", "for", "with", "to",
    "товар", "товары", "прочее", "другое", "разное", "аксессуар", "аксессуары",
}
GENERIC_CATEGORY_TOKENS = {"прочее", "товары", "другое", "разное", "other", "misc"}
WEAK_CATEGORY_VALUES = {
    "",
    "прочее",
    "другое",
    "разное",
    "товары",
    "товар",
    "каталог",
    "продукция",
    "other",
    "misc",
}


def _split_ozon_path(path: str | None) -> list[str]:
    text = " ".join(str(path or "").strip().split())
    if not text:
        return []
    parts = [x.strip() for x in re.split(r"\s*(?:/|>|»|→|\|)\s*", text) if str(x).strip()]
    return parts if parts else [text]


def _derive_catalog_categories_from_ozon_path(path: str | None) -> dict[str, str]:
    parts = _split_ozon_path(path)
    if not parts:
        return {"category": "", "base_category": "", "subcategory": ""}
    leaf = parts[-1]
    parent = parts[-2] if len(parts) >= 2 else leaf
    # Ozon leaf is canonical category; parent is base category context.
    return {
        "category": leaf,
        "base_category": parent,
        "subcategory": leaf,
    }


def _normalize_mapping_key_part(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().replace("ё", "е").split())


def _build_catalog_mapping_key(product_row: dict[str, Any]) -> str:
    supplier = _normalize_mapping_key_part(product_row.get("supplier_name"))
    base = _normalize_mapping_key_part(product_row.get("base_category"))
    sub = _normalize_mapping_key_part(product_row.get("subcategory"))
    cat = _normalize_mapping_key_part(product_row.get("category"))
    strong_tokens = _strong_anchor_tokens(product_row)
    if not any([supplier, base, sub, cat]) or not strong_tokens:
        return ""
    return f"{supplier}|{base}|{sub}|{cat}"


def _get_saved_catalog_mapping(conn: sqlite3.Connection, product_row: dict[str, Any]) -> dict[str, Any] | None:
    mapping_key = _build_catalog_mapping_key(product_row)
    if not mapping_key:
        return None
    row = conn.execute(
        """
        SELECT
            mapping_key,
            description_category_id,
            type_id,
            ozon_category_path,
            confidence
        FROM ozon_catalog_mapping_memory
        WHERE mapping_key = ?
        LIMIT 1
        """,
        (mapping_key,),
    ).fetchone()
    return dict(row) if row else None


def _save_catalog_mapping(conn: sqlite3.Connection, product_row: dict[str, Any], match: dict[str, Any]) -> None:
    mapping_key = _build_catalog_mapping_key(product_row)
    if not mapping_key:
        return
    conn.execute(
        """
        INSERT INTO ozon_catalog_mapping_memory (
            mapping_key,
            supplier_name,
            category,
            base_category,
            subcategory,
            description_category_id,
            type_id,
            ozon_category_path,
            confidence,
            hit_count,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(mapping_key) DO UPDATE SET
            supplier_name = excluded.supplier_name,
            category = excluded.category,
            base_category = excluded.base_category,
            subcategory = excluded.subcategory,
            description_category_id = excluded.description_category_id,
            type_id = excluded.type_id,
            ozon_category_path = excluded.ozon_category_path,
            confidence = CASE
                WHEN excluded.confidence > IFNULL(ozon_catalog_mapping_memory.confidence, 0)
                THEN excluded.confidence
                ELSE ozon_catalog_mapping_memory.confidence
            END,
            hit_count = IFNULL(ozon_catalog_mapping_memory.hit_count, 0) + 1,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            mapping_key,
            str(product_row.get("supplier_name") or ""),
            str(product_row.get("category") or ""),
            str(product_row.get("base_category") or ""),
            str(product_row.get("subcategory") or ""),
            int(match.get("description_category_id") or 0),
            int(match.get("type_id") or 0),
            str(match.get("full_path") or ""),
            float(match.get("score") or 0.0),
        ),
    )


def _normalize(text: str | None) -> str:
    return " ".join(str(text or "").strip().lower().replace("ё", "е").split())


def _tokenize(text: str | None) -> set[str]:
    tokens = re.findall(r"[a-zA-Zа-яА-Я0-9]+", _normalize(text))
    return {t for t in tokens if len(t) >= 2 and t not in STOP_TOKENS}


def _is_weak_category_value(value: str | None) -> bool:
    norm = _normalize(value)
    if norm in WEAK_CATEGORY_VALUES:
        return True
    tokens = _tokenize(norm)
    if not tokens:
        return True
    if tokens.issubset(GENERIC_CATEGORY_TOKENS):
        return True
    return False


def _strong_anchor_tokens(product_row: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for field_name in ("base_category", "subcategory", "category"):
        value = str(product_row.get(field_name) or "").strip()
        if _is_weak_category_value(value):
            continue
        field_tokens = {t for t in _tokenize(value) if t not in GENERIC_CATEGORY_TOKENS}
        tokens.update(field_tokens)
    return tokens


def _query_terms(product_row: dict[str, Any]) -> list[tuple[str, float, str]]:
    weighted_candidates = [
        (product_row.get("name"), 1.35, "name"),
        (product_row.get("subcategory"), 1.05, "subcategory"),
        (product_row.get("base_category"), 1.0, "base_category"),
        (product_row.get("category"), 0.95, "category"),
    ]
    out: list[tuple[str, float, str]] = []
    seen = set()
    for raw_value, weight, source in weighted_candidates:
        norm = _normalize(raw_value)
        if not norm or norm in seen:
            continue
        if source in {"base_category", "subcategory", "category"} and _is_weak_category_value(norm):
            continue
        seen.add(norm)
        out.append((norm, float(weight), source))
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
        tokens = _tokenize(text)
        generic_penalty = 0.12 if tokens and tokens.issubset(GENERIC_CATEGORY_TOKENS) else 0.0
        candidates.append(
            {
                "description_category_id": int(row["description_category_id"]),
                "type_id": int(row["type_id"]),
                "full_path": row["full_path"] or row["category_name"] or "",
                "category_name": row["category_name"] or "",
                "text": text,
                "tokens": tokens,
                "generic_penalty": generic_penalty,
            }
        )
    return candidates


def _score(query: str, query_tokens: set[str], candidate: dict[str, Any], weight: float) -> float:
    text = candidate["text"]
    if not text:
        return 0.0
    ratio = SequenceMatcher(None, query, text).ratio()
    overlap = 0.0
    if query_tokens:
        overlap = len(query_tokens & candidate["tokens"]) / max(len(query_tokens), 1)
    contains = 1.0 if query in text else 0.0
    token_contains = 0.0
    if query_tokens and any(tok in text for tok in query_tokens):
        token_contains = 0.5
    raw_score = (0.50 * ratio) + (0.35 * overlap) + (0.10 * contains) + (0.05 * token_contains)
    raw_score -= float(candidate.get("generic_penalty") or 0.0)
    return max(0.0, min(1.0, raw_score * weight))


def _best_match_for_product(product_row: dict[str, Any], categories: list[dict[str, Any]]) -> dict[str, Any] | None:
    terms = _query_terms(product_row)
    if not terms or not categories:
        return None

    anchor_tokens = _strong_anchor_tokens(product_row)

    best: dict[str, Any] | None = None
    best_score = 0.0
    best_term = None
    best_source = None

    for term, weight, source in terms:
        term_tokens = _tokenize(term)
        if not term_tokens:
            continue
        for category in categories:
            score = _score(term, term_tokens, category, weight=weight)
            if anchor_tokens and not (anchor_tokens & category["tokens"]):
                score *= 0.45
            elif anchor_tokens and (anchor_tokens & category["tokens"]):
                score *= 1.08
            if score > best_score:
                best_score = score
                best = category
                best_term = term
                best_source = source

    if not best:
        return None
    return {
        "description_category_id": int(best["description_category_id"]),
        "type_id": int(best["type_id"]),
        "full_path": best["full_path"],
        "score": round(float(best_score), 4),
        "matched_by": best_term,
        "matched_source": best_source,
    }


def bulk_assign_ozon_categories(
    conn: sqlite3.Connection,
    product_ids: list[int],
    min_score: float = 0.42,
    force: bool = False,
) -> dict[str, Any]:
    categories = _category_candidates(conn)
    if not categories:
        return {"processed": 0, "assigned": 0, "skipped": 0, "message": "Кэш категорий Ozon пуст"}
    category_by_key = {
        (int(c["description_category_id"]), int(c["type_id"])): c
        for c in categories
    }

    assigned = 0
    skipped = 0
    processed = 0

    for product_id in product_ids:
        row = conn.execute(
            """
            SELECT id, name, supplier_name, category, base_category, subcategory,
                   ozon_description_category_id, ozon_type_id, ozon_category_path, ozon_category_confidence
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
            key = (int(existing_id), int(existing_type))
            existing_cat = category_by_key.get(key)
            existing_path = str(product.get("ozon_category_path") or (existing_cat or {}).get("full_path") or "").strip()
            derived = _derive_catalog_categories_from_ozon_path(existing_path)
            if existing_path and (
                str(product.get("category") or "").strip() != str(derived.get("category") or "").strip()
                or not str(product.get("base_category") or "").strip()
                or not str(product.get("subcategory") or "").strip()
            ):
                conn.execute(
                    """
                    UPDATE products
                    SET category = ?,
                        base_category = CASE WHEN IFNULL(TRIM(base_category), '') = '' THEN ? ELSE base_category END,
                        subcategory = CASE WHEN IFNULL(TRIM(subcategory), '') = '' THEN ? ELSE subcategory END,
                        ozon_category_path = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        str(derived.get("category") or product.get("category") or ""),
                        str(derived.get("base_category") or ""),
                        str(derived.get("subcategory") or ""),
                        existing_path,
                        int(product_id),
                    ),
                )
                save_field_source(
                    conn=conn,
                    product_id=int(product_id),
                    field_name="category",
                    source_type="ozon_category_match",
                    source_value_raw=str(derived.get("category") or ""),
                    source_url=existing_path,
                    confidence=max(0.6, float(existing_conf)),
                    is_manual=False,
                )
            skipped += 1
            continue

        saved_match = _get_saved_catalog_mapping(conn, product)
        if saved_match:
            match = {
                "description_category_id": int(saved_match["description_category_id"]),
                "type_id": int(saved_match["type_id"]),
                "full_path": str(saved_match.get("ozon_category_path") or ""),
                "score": max(0.65, float(saved_match.get("confidence") or 0.0)),
                "matched_by": "saved_catalog_mapping",
                "matched_source": "mapping_memory",
            }
        else:
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

        derived = _derive_catalog_categories_from_ozon_path(str(match["full_path"] or ""))
        desired_category = str(derived.get("category") or product.get("category") or "")
        desired_base = str(derived.get("base_category") or "")
        desired_sub = str(derived.get("subcategory") or "")
        current_base = str(product.get("base_category") or "").strip()
        current_sub = str(product.get("subcategory") or "").strip()
        if not force:
            if current_base:
                desired_base = current_base
            if current_sub:
                desired_sub = current_sub

        conn.execute(
            """
            UPDATE products
            SET ozon_description_category_id = ?,
                ozon_type_id = ?,
                ozon_category_path = ?,
                category = ?,
                base_category = ?,
                subcategory = ?,
                ozon_category_confidence = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                int(match["description_category_id"]),
                int(match["type_id"]),
                str(match["full_path"] or ""),
                desired_category,
                desired_base,
                desired_sub,
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
        save_field_source(
            conn=conn,
            product_id=int(product_id),
            field_name="category",
            source_type="ozon_category_match",
            source_value_raw=desired_category,
            source_url=str(match["full_path"] or ""),
            confidence=float(match["score"]),
            is_manual=False,
        )
        if force or not current_base:
            save_field_source(
                conn=conn,
                product_id=int(product_id),
                field_name="base_category",
                source_type="ozon_category_match",
                source_value_raw=desired_base,
                source_url=str(match["full_path"] or ""),
                confidence=float(match["score"]),
                is_manual=False,
            )
        if force or not current_sub:
            save_field_source(
                conn=conn,
                product_id=int(product_id),
                field_name="subcategory",
                source_type="ozon_category_match",
                source_value_raw=desired_sub,
                source_url=str(match["full_path"] or ""),
                confidence=float(match["score"]),
                is_manual=False,
            )
        _save_catalog_mapping(conn, product, match)
        assigned += 1

    conn.commit()
    return {"processed": processed, "assigned": assigned, "skipped": skipped}
