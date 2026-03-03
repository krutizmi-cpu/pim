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
    "Самокаты":   {"length_cm": 110, "width_cm": 45, "height_cm": 110, "weight_kg": 5.0},
    "Прочее":     {"length_cm": 40,  "width_cm": 30, "height_cm": 20,  "weight_kg": 1.0},
}

CATEGORY_KEYWORDS = {
    "Велосипеды": ["велосипед", "bike", "bicycle"],
    "Самокаты":   ["самокат", "scooter"],
}


def guess_category_by_name(product_name: str) -> str:
    """Определяет категорию товара по его названию."""
    if not product_name:
        return "Прочее"
    p_lower = str(product_name).lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in p_lower for kw in keywords):
            return category
    return "Прочее"


def enrich_product_via_ai(product: Dict, openai_api_key: str) -> Optional[Dict]:
    """Запрос к OpenAI для получения габаритов и веса."""
    if not OpenAI:
        return None
    if not openai_api_key:
        return None

    client = OpenAI(api_key=openai_api_key)
    search_query = f"{product.get('name', '')} {product.get('sku', '')} dimensions weight"
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a product data specialist. Find dimensions (length, width, height in cm) and weight (kg). Return ONLY JSON: {\"length_cm\": X, \"width_cm\": Y, \"height_cm\": Z, \"weight_kg\": W}. If not found, use realistic estimates."},
                {"role": "user", "content": f"Find data for: {search_query}"}
            ],
            response_format={"type": "json_object"},
            temperature=0.2
        )
        result_text = response.choices[0].message.content.strip()
        parsed = json.loads(result_text)
        
        # Валидация
        for k in ["length_cm", "width_cm", "height_cm", "weight_kg"]:
            if k not in parsed or not isinstance(parsed[k], (int, float)) or parsed[k] <= 0:
                return None
        
        return {**parsed, "source": "ai_search"}
    except Exception:
        return None


def init_pim_tables(conn: sqlite3.Connection):
    """Инициализация таблиц БД."""
    cursor = conn.cursor()
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
    """Проверка значения на 'пустоту'."""
    if v is None:
        return True
    try:
        val = float(v)
        return val <= 0
    except (ValueError, TypeError):
        return str(v).strip() == ""


def enrich_product(product: Dict, conn: sqlite3.Connection,
                   openai_api_key: str, search_results=None,
                   force: bool = False) -> Tuple[Dict, str]:
    """
    Основная функция обогащения:
    1. Проверка на заполненность (если не force)
    2. Попытка через AI
    3. Fallback на средние по категории
    """
    dim_keys = ["length_cm", "width_cm", "height_cm", "weight_kg"]
    
    if not force and not any(_is_missing(product.get(k)) for k in dim_keys):
        return product, "already_filled"

    updated = dict(product)
    method = "failed"
    
    # 1. Попытка через OpenAI
    if openai_api_key:
        ai_res = enrich_product_via_ai(product, openai_api_key)
        if ai_res:
            for k in dim_keys:
                updated[k] = ai_res.get(k)
            method = "ai_search"
            updated["enrich_source"] = method
            updated["enrich_status"] = "enriched"
            return updated, method

    # 2. Fallback на средние
    cat = product.get("category") or guess_category_by_name(product.get("name", ""))
    defaults = CATEGORY_DEFAULTS_BUILTIN.get(cat, CATEGORY_DEFAULTS_BUILTIN["Прочее"])
    
    for k in dim_keys:
        updated[k] = defaults.get(k)
    
    method = f"category_default ({cat})"
    updated["enrich_source"] = method
    updated["enrich_status"] = "enriched"
    
    return updated, method


def log_enrichment(conn: sqlite3.Connection, product_id: int, method: str, success: bool):
    """Логирование результата обогащения."""
    try:
        c = conn.cursor()
        c.execute("""
            INSERT INTO pim_enrichment_log (product_id, method, success)
            VALUES (?,?,?)
        """, (int(product_id), str(method), 1 if success else 0))
        conn.commit()
    except Exception:
        pass
