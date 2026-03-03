# Скопировано из оригинального файла - см. https://github.com/krutizmi-cpu/unit/blob/main/pim_enrich.py
# Полный код слишком большой для одного коммита, временная заглушка
"""
PIM Enrichment Module — поиск габаритов и веса товаров.

Логика:
1. Поиск в интернете по названию/артикулу/EAN через AI (web search + GPT)
2. Извлечение характеристик из результатов поиска
3. Fallback на средние значения по категории из category_defaults
4. Логирование источника значений

Используется в app.py (Streamlit приложение PIM).
"""

import json
import sqlite3
from typing import Optional, Dict, Tuple

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

# Средние габариты по категориям
CATEGORY_DEFAULTS_BUILTIN = {
    "Велосипеды": {"length_cm": 170, "width_cm": 60, "height_cm": 100, "weight_kg": 14.0},
    "Самокаты": {"length_cm": 110, "width_cm": 45, "height_cm": 110, "weight_kg": 5.0},
    "Прочее": {"length_cm": 40, "width_cm": 30, "height_cm": 20, "weight_kg": 1.0},
}

CATEGORY_KEYWORDS = {
    "Велосипеды": ["велосипед", "bike"],
    "Самокаты": ["самокат", "scooter"],
}

def guess_category_by_name(product_name: str) -> str:
    product_lower = product_name.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in product_lower:
                return category
    return "Прочее"

def enrich_product_via_ai(product: Dict, openai_api_key: str) -> Optional[Dict]:
    if OpenAI is None:
        raise RuntimeError("openai package not installed")
    client = OpenAI(api_key=openai_api_key)
    search_query = f"{product['name']} {product.get('sku', '')} габариты вес"
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Find dimensions (length, width, height in cm) and weight (kg). Return JSON: {\"length_cm\": X, \"width_cm\": Y, \"height_cm\": Z, \"weight_kg\": W}"},
                {"role": "user", "content": f"Find dimensions: {search_query}"}
            ],
            temperature=0.3
        )
        result_text = response.choices[0].message.content.strip()
        parsed = json.loads(result_text)
        if all(parsed.get(k) and parsed[k] > 0 for k in ["length_cm", "width_cm", "height_cm", "weight_kg"]):
            return {**parsed, "source": "ai_search"}
    except Exception:
        pass
    guessed_category = guess_category_by_name(product["name"])
    defaults = CATEGORY_DEFAULTS_BUILTIN.get(guessed_category, CATEGORY_DEFAULTS_BUILTIN["Прочее"])
    return {**defaults, "source": f"category_default ({guessed_category})"}

def init_pim_tables(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cols = [r[1] for r in cursor.execute("PRAGMA table_info(products)")]
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT UNIQUE,
            name TEXT,
            length_cm REAL,
            width_cm REAL,
            height_cm REAL,
            weight_kg REAL,
            cost REAL DEFAULT 0,
            ean TEXT,
            brand TEXT,
            category TEXT,
            description TEXT,
            main_image_url TEXT,
            enrich_status TEXT DEFAULT 'pending',
            enrich_source TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pim_enrichment_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            method TEXT,
            success INTEGER,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

def _is_missing(v) -> bool:
    return v is None or str(v).strip() == "" or v == 0

def enrich_product(product: Dict, conn: sqlite3.Connection, openai_api_key: str, search_results=None, force: bool = False) -> Tuple[Dict, str]:
    if not force and not any(_is_missing(product.get(k)) for k in ["length_cm", "width_cm", "height_cm", "weight_kg"]):
        return product, "already_filled"
    method = "failed"
    updated = dict(product)
    if openai_api_key:
        try:
            r = enrich_product_via_ai(product, openai_api_key)
            if r:
                updated.update({"length_cm": r.get("length_cm"), "width_cm": r.get("width_cm"), "height_cm": r.get("height_cm"), "weight_kg": r.get("weight_kg")})
                method = r.get("source", "ai")
        except Exception:
            method = "failed"
    if method == "failed":
        guessed_category = guess_category_by_name(str(product.get("name") or ""))
        defaults = CATEGORY_DEFAULTS_BUILTIN.get(guessed_category, CATEGORY_DEFAULTS_BUILTIN["Прочее"])
        updated.update(defaults)
        method = f"category_default ({guessed_category})"
    updated["enrich_source"] = method
    updated["enrich_status"] = "enriched" if method != "failed" else "failed"
    return updated, method

def log_enrichment(conn: sqlite3.Connection, product_id: int, method: str, success: bool):
    try:
        c = conn.cursor()
        c.execute("INSERT INTO pim_enrichment_log (product_id, method, success) VALUES (?,?,?)", (int(product_id), str(method), 1 if success else 0))
        conn.commit()
    except Exception:
        pass
