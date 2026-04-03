"""
PIM Enrichment Module — обогащение габаритов и веса товаров.

Что делает:
1. Пытается определить категорию по названию
2. Для велосипедов пытается определить подкатегорию
3. Для велосипедов пытается извлечь диаметр колеса
4. Подставляет realistic defaults
5. Опционально может использовать OpenAI, если ключ есть
6. Логирует источник обогащения

Этот модуль можно использовать из app.py или других сервисов.
"""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Optional, Dict, Tuple, Any

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# -----------------------------
# Встроенные category defaults
# -----------------------------

CATEGORY_DEFAULTS_BUILTIN = {
    "Прочее": {
        "length_cm": 40.0,
        "width_cm": 30.0,
        "height_cm": 20.0,
        "weight_kg": 1.0,
        "package_length_cm": 42.0,
        "package_width_cm": 32.0,
        "package_height_cm": 22.0,
        "package_weight_kg": 1.2,
    },
    "Самокаты": {
        "length_cm": 110.0,
        "width_cm": 45.0,
        "height_cm": 110.0,
        "weight_kg": 5.0,
        "package_length_cm": 112.0,
        "package_width_cm": 18.0,
        "package_height_cm": 35.0,
        "package_weight_kg": 6.0,
    },
    "Велосипеды": {
        "length_cm": 170.0,
        "width_cm": 25.0,
        "height_cm": 100.0,
        "weight_kg": 16.0,
        "package_length_cm": 172.0,
        "package_width_cm": 27.0,
        "package_height_cm": 103.0,
        "package_weight_kg": 18.0,
    },
}


BICYCLE_DEFAULTS = [
    {
        "subcategory": "child_bicycle",
        "wheel_diameter_inch": 12.0,
        "length_cm": 90.0,
        "width_cm": 18.0,
        "height_cm": 55.0,
        "weight_kg": 8.5,
        "package_length_cm": 92.0,
        "package_width_cm": 20.0,
        "package_height_cm": 58.0,
        "package_weight_kg": 10.0,
    },
    {
        "subcategory": "child_bicycle",
        "wheel_diameter_inch": 14.0,
        "length_cm": 98.0,
        "width_cm": 18.0,
        "height_cm": 60.0,
        "weight_kg": 9.5,
        "package_length_cm": 100.0,
        "package_width_cm": 20.0,
        "package_height_cm": 62.0,
        "package_weight_kg": 11.0,
    },
    {
        "subcategory": "child_bicycle",
        "wheel_diameter_inch": 16.0,
        "length_cm": 110.0,
        "width_cm": 18.0,
        "height_cm": 65.0,
        "weight_kg": 10.5,
        "package_length_cm": 112.0,
        "package_width_cm": 20.0,
        "package_height_cm": 67.0,
        "package_weight_kg": 12.0,
    },
    {
        "subcategory": "child_bicycle",
        "wheel_diameter_inch": 18.0,
        "length_cm": 118.0,
        "width_cm": 20.0,
        "height_cm": 70.0,
        "weight_kg": 11.5,
        "package_length_cm": 120.0,
        "package_width_cm": 22.0,
        "package_height_cm": 72.0,
        "package_weight_kg": 13.0,
    },
    {
        "subcategory": "child_bicycle",
        "wheel_diameter_inch": 20.0,
        "length_cm": 128.0,
        "width_cm": 20.0,
        "height_cm": 75.0,
        "weight_kg": 12.8,
        "package_length_cm": 130.0,
        "package_width_cm": 22.0,
        "package_height_cm": 77.0,
        "package_weight_kg": 14.5,
    },
    {
        "subcategory": "teen_bicycle",
        "wheel_diameter_inch": 24.0,
        "length_cm": 145.0,
        "width_cm": 22.0,
        "height_cm": 85.0,
        "weight_kg": 14.5,
        "package_length_cm": 148.0,
        "package_width_cm": 24.0,
        "package_height_cm": 88.0,
        "package_weight_kg": 16.5,
    },
    {
        "subcategory": "adult_bicycle",
        "wheel_diameter_inch": 26.0,
        "length_cm": 170.0,
        "width_cm": 25.0,
        "height_cm": 100.0,
        "weight_kg": 16.5,
        "package_length_cm": 172.0,
        "package_width_cm": 27.0,
        "package_height_cm": 103.0,
        "package_weight_kg": 19.0,
    },
    {
        "subcategory": "adult_bicycle",
        "wheel_diameter_inch": 27.5,
        "length_cm": 175.0,
        "width_cm": 25.0,
        "height_cm": 102.0,
        "weight_kg": 17.2,
        "package_length_cm": 178.0,
        "package_width_cm": 27.0,
        "package_height_cm": 105.0,
        "package_weight_kg": 19.8,
    },
    {
        "subcategory": "adult_bicycle",
        "wheel_diameter_inch": 29.0,
        "length_cm": 180.0,
        "width_cm": 25.0,
        "height_cm": 105.0,
        "weight_kg": 18.0,
        "package_length_cm": 183.0,
        "package_width_cm": 27.0,
        "package_height_cm": 108.0,
        "package_weight_kg": 20.5,
    },
]


CATEGORY_KEYWORDS = {
    "Велосипеды": ["велосипед", "bike", "bicycle"],
    "Самокаты": ["самокат", "scooter"],
}


# -----------------------------
# Helpers
# -----------------------------

def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _name_from_product(product: Dict[str, Any]) -> str:
    return _safe_str(
        product.get("name")
        or product.get("title")
        or product.get("raw_name")
        or product.get("name_raw")
    )


def _sku_from_product(product: Dict[str, Any]) -> str:
    return _safe_str(
        product.get("sku")
        or product.get("article")
        or product.get("supplier_article")
        or product.get("vendor_code")
    )


def _category_from_product(product: Dict[str, Any]) -> str:
    return _safe_str(product.get("category"))


def _is_missing(v) -> bool:
    if v is None:
        return True
    try:
        val = float(v)
        return val <= 0
    except (ValueError, TypeError):
        return str(v).strip() == ""


def guess_category_by_name(product_name: str) -> str:
    if not product_name:
        return "Прочее"
    p_lower = str(product_name).lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in p_lower for kw in keywords):
            return category
    return "Прочее"


# -----------------------------
# Bicycle inference
# -----------------------------

def infer_bicycle_subcategory(name: str) -> str | None:
    s = (name or "").lower()

    if "велосипед" not in s:
        return None

    if "детск" in s:
        return "child_bicycle"
    if "подрост" in s:
        return "teen_bicycle"
    if "bmx" in s:
        return "adult_bicycle"
    if "горн" in s:
        return "adult_bicycle"
    if "городск" in s:
        return "adult_bicycle"

    return "bicycle_unknown"


def infer_wheel_diameter(name: str) -> float | None:
    s = (name or "").lower().replace(",", ".")

    patterns = [
        r'(?<!\d)(12|14|16|18|20|24|26|27\.5|29)(?=\s*(?:["”″]|дюйм|inch|in\b))',
        r'(?<!\d)(12|14|16|18|20|24|26|27\.5|29)(?!\d)',
    ]

    for pattern in patterns:
        match = re.search(pattern, s)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
    return None


def find_bicycle_default(subcategory: str | None, wheel_diameter_inch: float | None) -> dict | None:
    exact = None
    fallback = None

    for row in BICYCLE_DEFAULTS:
        if row["subcategory"] != subcategory:
            continue

        fallback = row

        if wheel_diameter_inch is not None and float(row["wheel_diameter_inch"]) == float(wheel_diameter_inch):
            exact = row
            break

    return exact or fallback


# -----------------------------
# OpenAI enrichment
# -----------------------------

def enrich_product_via_ai(product: Dict[str, Any], openai_api_key: str) -> Optional[Dict[str, Any]]:
    if not OpenAI:
        return None
    if not openai_api_key:
        return None

    client = OpenAI(api_key=openai_api_key)
    search_query = f"{_name_from_product(product)} {_sku_from_product(product)} dimensions weight"

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a product data specialist. "
                        "Find product dimensions and weight. "
                        "Return ONLY JSON with keys: "
                        "length_cm, width_cm, height_cm, weight_kg, "
                        "package_length_cm, package_width_cm, package_height_cm, package_weight_kg. "
                        "If some package values are not found, you may estimate them realistically."
                    ),
                },
                {"role": "user", "content": f"Find data for: {search_query}"},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
        )

        result_text = response.choices[0].message.content.strip()
        parsed = json.loads(result_text)

        required_keys = [
            "length_cm",
            "width_cm",
            "height_cm",
            "weight_kg",
        ]

        for key in required_keys:
            if key not in parsed or not isinstance(parsed[key], (int, float)) or parsed[key] <= 0:
                return None

        parsed["source"] = "ai_search"
        return parsed

    except Exception:
        return None


# -----------------------------
# DB init
# -----------------------------

def init_pim_tables(conn: sqlite3.Connection):
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT UNIQUE,
            name TEXT,
            length_cm REAL,
            width_cm REAL,
            height_cm REAL,
            weight_kg REAL,
            package_length_cm REAL,
            package_width_cm REAL,
            package_height_cm REAL,
            package_weight_kg REAL,
            cost REAL DEFAULT 0,
            ean TEXT,
            brand TEXT,
            category TEXT,
            description TEXT,
            main_image_url TEXT,
            enrich_status TEXT DEFAULT 'pending',
            enrich_source TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pim_enrichment_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            method TEXT,
            success INTEGER,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.commit()


# -----------------------------
# Main enrichment logic
# -----------------------------

def enrich_product(
    product: Dict[str, Any],
    conn: sqlite3.Connection,
    openai_api_key: str,
    search_results=None,
    force: bool = False,
) -> Tuple[Dict[str, Any], str]:
    """
    Основная логика:
    1. Если всё заполнено и force=False — не трогаем
    2. Если есть OpenAI — пробуем AI
    3. Если велосипед — используем bicycle defaults
    4. Иначе — category defaults
    """
    dim_keys = [
        "length_cm",
        "width_cm",
        "height_cm",
        "weight_kg",
    ]

    package_keys = [
        "package_length_cm",
        "package_width_cm",
        "package_height_cm",
        "package_weight_kg",
    ]

    all_keys = dim_keys + package_keys

    if not force and not any(_is_missing(product.get(k)) for k in dim_keys):
        updated = dict(product)
        if not updated.get("enrich_status"):
            updated["enrich_status"] = "already_filled"
        if not updated.get("enrich_source"):
            updated["enrich_source"] = "existing_data"
        return updated, "already_filled"

    updated = dict(product)
    method = "failed"

    # 1. Попытка через OpenAI
    if openai_api_key:
        ai_res = enrich_product_via_ai(product, openai_api_key)
        if ai_res:
            for key in all_keys:
                if key in ai_res and ai_res.get(key):
                    updated[key] = ai_res.get(key)
            updated["enrich_source"] = "ai_search"
            updated["enrich_status"] = "enriched"
            return updated, "ai_search"

    # 2. Fallback на category defaults / bicycle defaults
    product_name = _name_from_product(product)
    category = _category_from_product(product) or guess_category_by_name(product_name)

    if category == "Велосипеды":
        subcategory = infer_bicycle_subcategory(product_name)
        wheel_diameter_inch = infer_wheel_diameter(product_name)
        bicycle_default = find_bicycle_default(subcategory, wheel_diameter_inch)

        if bicycle_default:
            for key in all_keys:
                updated[key] = bicycle_default.get(key)

            updated["category"] = category
            if subcategory:
                updated["subcategory"] = subcategory
            if wheel_diameter_inch:
                updated["wheel_diameter_inch"] = wheel_diameter_inch

            method = f"category_default_bicycle ({subcategory or 'unknown'})"
            updated["enrich_source"] = method
            updated["enrich_status"] = "enriched"
            return updated, method

    defaults = CATEGORY_DEFAULTS_BUILTIN.get(category, CATEGORY_DEFAULTS_BUILTIN["Прочее"])

    for key in all_keys:
        updated[key] = defaults.get(key)

    updated["category"] = category
    method = f"category_default ({category})"
    updated["enrich_source"] = method
    updated["enrich_status"] = "enriched"

    return updated, method


def log_enrichment(conn: sqlite3.Connection, product_id: int, method: str, success: bool):
    try:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO pim_enrichment_log (product_id, method, success)
            VALUES (?,?,?)
            """,
            (int(product_id), str(method), 1 if success else 0),
        )
        conn.commit()
    except Exception:
        pass
