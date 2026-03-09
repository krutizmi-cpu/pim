from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import sqlite3

from db import init_db
from services.duplicate_service import refresh_duplicates_for_product
from services.text_utils import normalize_name

SUPPORTED_FIELDS = [
    "article",
    "name",
    "barcode",
    "category",
    "supplier_url",
    "weight",
    "length",
    "width",
    "height",
    "package_length",
    "package_width",
    "package_height",
    "gross_weight",
    "image_url",
    "description",
]

COLUMN_MAP: dict[str, list[str]] = {
    "article": ["article", "артикул", "sku", "код товара", "код"],
    "name": ["name", "название", "наименование"],
    "barcode": ["barcode", "ean", "штрихкод"],
    "category": ["category", "категория"],
    "supplier_url": ["supplier_url", "url поставщика", "ссылка поставщика", "supplier link"],
    "weight": ["weight", "вес"],
    "length": ["length", "длина"],
    "width": ["width", "ширина"],
    "height": ["height", "высота"],
    "package_length": ["package_length", "длина упаковки"],
    "package_width": ["package_width", "ширина упаковки"],
    "package_height": ["package_height", "высота упаковки"],
    "gross_weight": ["gross_weight", "вес брутто"],
    "image_url": ["image_url", "фото", "картинка", "image"],
    "description": ["description", "описание"],
}


@dataclass
class ImportResult:
    imported: int
    created: int
    updated: int
    duplicates: list[dict[str, Any]]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    source_columns = {str(col).strip().lower(): col for col in df.columns}
    rename_map: dict[str, str] = {}

    for target, aliases in COLUMN_MAP.items():
        for alias in aliases:
            src = source_columns.get(alias)
            if src:
                rename_map[src] = target
                break

    normalized = df.rename(columns=rename_map).copy()
    for col in SUPPORTED_FIELDS:
        if col not in normalized.columns:
            normalized[col] = None

    return normalized[SUPPORTED_FIELDS]


def _to_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text or None


def _to_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).replace(",", ".").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def import_catalog_from_excel(conn: sqlite3.Connection, excel_path: Path) -> ImportResult:
    init_db(conn)
    df = pd.read_excel(excel_path)
    normalized = normalize_columns(df)

    created = 0
    updated = 0
    duplicates: list[dict[str, Any]] = []

    for _, row in normalized.iterrows():
        article = _to_text(row.get("article"))
        name = _to_text(row.get("name"))
        if not article or not name:
            continue

        normalized_name = normalize_name(name)
        now = datetime.utcnow().isoformat(timespec="seconds")

        exists = conn.execute("SELECT id FROM products WHERE article = ?", (article,)).fetchone()
        if exists:
            conn.execute(
                """
                UPDATE products
                SET name = ?, barcode = ?, category = ?, supplier_url = ?,
                    weight = ?, length = ?, width = ?, height = ?,
                    package_length = ?, package_width = ?, package_height = ?, gross_weight = ?,
                    image_url = ?, description = ?, normalized_name = ?, updated_at = ?
                WHERE article = ?
                """,
                (
                    name,
                    _to_text(row.get("barcode")),
                    _to_text(row.get("category")),
                    _to_text(row.get("supplier_url")),
                    _to_float(row.get("weight")),
                    _to_float(row.get("length")),
                    _to_float(row.get("width")),
                    _to_float(row.get("height")),
                    _to_float(row.get("package_length")),
                    _to_float(row.get("package_width")),
                    _to_float(row.get("package_height")),
                    _to_float(row.get("gross_weight")),
                    _to_text(row.get("image_url")),
                    _to_text(row.get("description")),
                    normalized_name,
                    now,
                    article,
                ),
            )
            product_id = conn.execute("SELECT id FROM products WHERE article = ?", (article,)).fetchone()["id"]
            updated += 1
        else:
            cur = conn.execute(
                """
                INSERT INTO products (
                    article, name, barcode, category, supplier_url,
                    weight, length, width, height,
                    package_length, package_width, package_height, gross_weight,
                    image_url, description,
                    normalized_name, enrichment_status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article,
                    name,
                    _to_text(row.get("barcode")),
                    _to_text(row.get("category")),
                    _to_text(row.get("supplier_url")),
                    _to_float(row.get("weight")),
                    _to_float(row.get("length")),
                    _to_float(row.get("width")),
                    _to_float(row.get("height")),
                    _to_float(row.get("package_length")),
                    _to_float(row.get("package_width")),
                    _to_float(row.get("package_height")),
                    _to_float(row.get("gross_weight")),
                    _to_text(row.get("image_url")),
                    _to_text(row.get("description")),
                    normalized_name,
                    "new",
                    now,
                    now,
                ),
            )
            product_id = cur.lastrowid
            created += 1

        duplicates.extend(refresh_duplicates_for_product(conn, int(product_id)))

    conn.commit()
    unique_duplicates = []
    seen = set()
    for item in duplicates:
        key = (item["product_id_1"], item["product_id_2"], item["reason"])
        if key not in seen:
            seen.add(key)
            unique_duplicates.append(item)
    return ImportResult(imported=created + updated, created=created, updated=updated, duplicates=unique_duplicates)
