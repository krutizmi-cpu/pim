from __future__ import annotations

import re
import sqlite3
from statistics import median
from typing import Any


DIM_FIELDS = (
    "weight",
    "gross_weight",
    "length",
    "width",
    "height",
    "package_length",
    "package_width",
    "package_height",
)


CATEGORY_RULES: list[dict[str, Any]] = [
    {
        "category": "Велосипеды",
        "base_category": "Велосипеды",
        "subcategory": "Беговелы",
        "keywords": ["беговел"],
        "priority": 1.35,
    },
    {
        "category": "Велосипеды",
        "base_category": "Велосипеды",
        "subcategory": "Велосипеды двухколесные",
        "keywords": ["велосипед", "bicycle", "bike", "bmx"],
        "priority": 0.8,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Насосы",
        "keywords": ["насос", "pump"],
        "priority": 1.4,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Замки",
        "keywords": ["замок", "трос", "lock"],
        "priority": 1.4,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Фонари",
        "keywords": ["фонар", "lamp", "light"],
        "priority": 1.4,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Звонки",
        "keywords": ["звонок", "bell"],
        "priority": 1.4,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Фляги",
        "keywords": ["фляг", "бутылк", "bottle"],
        "priority": 1.4,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Велосумки",
        "keywords": ["сумк", "рюкзак", "bag"],
        "priority": 1.3,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Грипсы",
        "keywords": ["грипс", "grip"],
        "priority": 1.4,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Зеркала",
        "keywords": ["зеркал", "mirror"],
        "priority": 1.4,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Мультитулы",
        "keywords": ["мультитул", "multitool"],
        "priority": 1.4,
    },
    {
        "category": "Аксессуары для велосипедов",
        "base_category": "Аксессуары для велосипедов",
        "subcategory": "Велокомпьютеры",
        "keywords": ["велокомпьютер", "компьютер", "odometer", "cycle computer"],
        "priority": 1.4,
    },
    {
        "category": "Спортивная экипировка",
        "base_category": "Спортивная экипировка",
        "subcategory": "Велоперчатки",
        "keywords": ["перчатк", "glove"],
        "priority": 1.3,
    },
    {
        "category": "Спортивная экипировка",
        "base_category": "Спортивная экипировка",
        "subcategory": "Шлемы",
        "keywords": ["шлем", "helmet"],
        "priority": 1.3,
    },
    {
        "category": "Спортивная экипировка",
        "base_category": "Спортивная экипировка",
        "subcategory": "Очки спортивные",
        "keywords": ["очки", "glass", "goggle"],
        "priority": 1.3,
    },
]


def _norm(text: str | None) -> str:
    return " ".join(str(text or "").strip().lower().replace("ё", "е").split())


def _good_num(value: Any) -> bool:
    try:
        num = float(value)
    except Exception:
        return False
    return num > 0


def infer_category_by_name(name: str | None) -> dict[str, Any]:
    source = _norm(name)
    if not source:
        return {"category": None, "base_category": None, "subcategory": None, "score": 0.0, "matched": []}

    best: dict[str, Any] | None = None
    best_score = 0.0
    for rule in CATEGORY_RULES:
        matched: list[str] = []
        score = 0.0
        for kw in rule["keywords"]:
            kw_norm = _norm(kw)
            if not kw_norm:
                continue
            if kw_norm in source:
                matched.append(kw)
                score += 1.0 + min(0.5, float(len(kw_norm)) / 20.0)
        score *= float(rule.get("priority") or 1.0)
        if score > best_score:
            best_score = score
            best = {
                "category": rule["category"],
                "base_category": rule["base_category"],
                "subcategory": rule["subcategory"],
                "score": round(score, 4),
                "matched": matched,
            }
    return best or {"category": None, "base_category": None, "subcategory": None, "score": 0.0, "matched": []}


def _extract_wheel_diameter_inch(name: str | None) -> float | None:
    text = _norm(name).replace(",", ".")
    if not text:
        return None
    patterns = [
        r'(?<!\d)(12|14|16|18|20|24|26|27\.5|29)(?=\s*(?:["”″]|дюйм|inch|in\b))',
        r'(?<!\d)(12|14|16|18|20|24|26|27\.5|29)(?!\d)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        try:
            return float(match.group(1))
        except Exception:
            return None
    return None


def infer_category_fields(product: dict[str, Any]) -> dict[str, Any]:
    name = str(product.get("name") or "").strip()
    inferred = infer_category_by_name(name)
    out: dict[str, Any] = {}
    if inferred.get("category"):
        out["category"] = inferred["category"]
    if inferred.get("base_category"):
        out["base_category"] = inferred["base_category"]
    if inferred.get("subcategory"):
        out["subcategory"] = inferred["subcategory"]
    wheel_diameter = _extract_wheel_diameter_inch(name)
    if wheel_diameter is not None:
        out["wheel_diameter_inch"] = wheel_diameter
    out["category_inference_score"] = float(inferred.get("score") or 0.0)
    out["category_inference_matched"] = inferred.get("matched") or []
    return out


def is_dimension_payload_suspicious(parsed: dict[str, Any]) -> bool:
    values = {k: parsed.get(k) for k in DIM_FIELDS}
    numeric = {k: float(v) for k, v in values.items() if _good_num(v)}
    if not numeric:
        return False

    for key, value in numeric.items():
        if key in {"weight", "gross_weight"} and (value > 200.0 or value < 0.01):
            return True
        if key not in {"weight", "gross_weight"} and (value > 600.0 or value < 0.1):
            return True

    lwh = [numeric.get("length"), numeric.get("width"), numeric.get("height")]
    lwh = [v for v in lwh if v is not None]
    if len(lwh) == 3 and len(set(round(v, 3) for v in lwh)) == 1:
        return True
    return False


def _query_scope_rows(
    conn: sqlite3.Connection,
    where_sql: str,
    params: list[Any],
    limit: int = 5000,
) -> list[sqlite3.Row]:
    sql = f"""
        SELECT
            id, weight, gross_weight, length, width, height,
            package_length, package_width, package_height
        FROM products
        WHERE {where_sql}
          AND (
              (weight IS NOT NULL AND weight > 0) OR
              (gross_weight IS NOT NULL AND gross_weight > 0) OR
              (length IS NOT NULL AND length > 0) OR
              (width IS NOT NULL AND width > 0) OR
              (height IS NOT NULL AND height > 0) OR
              (package_length IS NOT NULL AND package_length > 0) OR
              (package_width IS NOT NULL AND package_width > 0) OR
              (package_height IS NOT NULL AND package_height > 0)
          )
        ORDER BY id DESC
        LIMIT ?
    """
    rows = conn.execute(sql, [*params, int(limit)]).fetchall()
    return rows


def infer_dimensions_from_catalog(
    conn: sqlite3.Connection,
    product: dict[str, Any],
    min_samples: int = 4,
) -> dict[str, Any]:
    product_id = int(product.get("id") or 0)
    scopes: list[tuple[str, str, list[Any]]] = []

    ozon_desc_id = product.get("ozon_description_category_id")
    ozon_type_id = product.get("ozon_type_id")
    if ozon_desc_id and ozon_type_id:
        scopes.append(
            (
                "ozon_category",
                "id <> ? AND ozon_description_category_id = ? AND ozon_type_id = ?",
                [product_id, int(ozon_desc_id), int(ozon_type_id)],
            )
        )

    for field_name in ("subcategory", "base_category", "category"):
        value = str(product.get(field_name) or "").strip()
        if value:
            scopes.append(
                (
                    field_name,
                    f"id <> ? AND LOWER(TRIM({field_name})) = LOWER(TRIM(?))",
                    [product_id, value],
                )
            )

    for scope_name, where_sql, params in scopes:
        rows = _query_scope_rows(conn, where_sql, params)
        if not rows:
            continue
        values_by_field: dict[str, list[float]] = {k: [] for k in DIM_FIELDS}
        for row in rows:
            for field in DIM_FIELDS:
                value = row[field] if isinstance(row, sqlite3.Row) else None
                if not _good_num(value):
                    continue
                values_by_field[field].append(float(value))

        resolved: dict[str, float] = {}
        samples_by_field: dict[str, int] = {}
        for field, values in values_by_field.items():
            if len(values) < int(min_samples):
                continue
            resolved[field] = float(round(median(values), 4))
            samples_by_field[field] = len(values)
        if resolved:
            return {
                "found": True,
                "scope": scope_name,
                "sample_rows": len(rows),
                "samples_by_field": samples_by_field,
                "values": resolved,
            }

    return {"found": False, "scope": None, "sample_rows": 0, "samples_by_field": {}, "values": {}}


def infer_dimensions_from_category_defaults(conn: sqlite3.Connection, product: dict[str, Any]) -> dict[str, Any]:
    base_category = str(product.get("base_category") or product.get("category") or "").strip()
    subcategory = str(product.get("subcategory") or "").strip()
    wheel_diameter_inch = product.get("wheel_diameter_inch")

    candidates = [
        (
            "base+sub+wheel",
            """
            SELECT *
            FROM category_defaults
            WHERE LOWER(TRIM(base_category)) = LOWER(TRIM(?))
              AND LOWER(TRIM(IFNULL(subcategory, ''))) = LOWER(TRIM(?))
              AND wheel_diameter_inch = ?
            ORDER BY priority ASC, id ASC
            LIMIT 1
            """,
            [base_category, subcategory, float(wheel_diameter_inch) if wheel_diameter_inch is not None else None],
        ),
        (
            "base+sub",
            """
            SELECT *
            FROM category_defaults
            WHERE LOWER(TRIM(base_category)) = LOWER(TRIM(?))
              AND LOWER(TRIM(IFNULL(subcategory, ''))) = LOWER(TRIM(?))
            ORDER BY priority ASC, id ASC
            LIMIT 1
            """,
            [base_category, subcategory],
        ),
        (
            "base_only",
            """
            SELECT *
            FROM category_defaults
            WHERE LOWER(TRIM(base_category)) = LOWER(TRIM(?))
            ORDER BY priority ASC, id ASC
            LIMIT 1
            """,
            [base_category],
        ),
    ]

    for scope_name, sql, params in candidates:
        if not base_category:
            continue
        if scope_name in {"base+sub+wheel", "base+sub"} and not subcategory:
            continue
        row = conn.execute(sql, params).fetchone()
        if not row:
            continue
        values = {
            "length": row["length_cm"],
            "width": row["width_cm"],
            "height": row["height_cm"],
            "weight": row["weight_kg"],
            "package_length": row["package_length_cm"],
            "package_width": row["package_width_cm"],
            "package_height": row["package_height_cm"],
            "gross_weight": row["package_weight_kg"],
        }
        clean_values = {k: float(v) for k, v in values.items() if _good_num(v)}
        if clean_values:
            return {
                "found": True,
                "scope": scope_name,
                "values": clean_values,
            }
    return {"found": False, "scope": None, "values": {}}
