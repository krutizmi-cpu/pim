from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import sqlite3

from db import init_db
from pim_enrich import enrich_product
from services.duplicate_service import refresh_duplicates_for_product
from services.text_utils import normalize_name


SUPPORTED_FIELDS = [
    "article",
    "supplier_article",
    "name",
    "barcode",
    "category",
    "supplier_name",
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
    "article": [
        "article",
        "артикул",
        "sku",
        "код товара",
        "код",
        "vendor_code",
        "артикул товара",
    ],
    "supplier_article": [
        "supplier_article",
        "артикул поставщика",
        "артикул поставщ.",
        "артикул производителя",
        "модель",
    ],
    "name": [
        "name",
        "название",
        "наименование",
        "номенклатура",
        "title",
        "name_on_site",
    ],
    "barcode": [
        "barcode",
        "ean",
        "штрихкод",
        "штрих код",
        "bar_code",
    ],
    "category": [
        "category",
        "категория",
        "группа2",
        "группа 2",
        "group2",
    ],
    "supplier_name": [
        "supplier_name",
        "поставщик",
        "бренд поставщика",
        "торговая марка",
        "brand",
    ],
    "supplier_url": [
        "supplier_url",
        "url поставщика",
        "ссылка поставщика",
        "supplier link",
        "ссылки на товар на оф. сайте",
        "ссылка на товар",
        "url",
    ],
    "weight": [
        "weight",
        "вес",
        "ves",
    ],
    "length": [
        "length",
        "длина",
        "dlina",
    ],
    "width": [
        "width",
        "ширина",
        "shirina",
    ],
    "height": [
        "height",
        "высота",
        "vysota",
    ],
    "package_length": [
        "package_length",
        "длина упаковки",
        "глубина упаковки",
        "packing_length",
        "packing_depth",
        "packing dlina",
    ],
    "package_width": [
        "package_width",
        "ширина упаковки",
        "packing_width",
    ],
    "package_height": [
        "package_height",
        "высота упаковки",
        "packing_height",
    ],
    "gross_weight": [
        "gross_weight",
        "вес брутто",
        "вес в упаковке",
        "packing_weight",
        "packing weight",
    ],
    "image_url": [
        "image_url",
        "фото",
        "картинка",
        "image",
        "главное фото",
    ],
    "description": [
        "description",
        "описание",
        "основное описание",
    ],
}


@dataclass
class ImportResult:
    imported: int
    created: int
    updated: int
    duplicates: list[dict[str, Any]]
    batch_id: str


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


def _make_internal_article(
    article: str | None,
    supplier_article: str | None,
) -> str | None:
    if article:
        return article
    if supplier_article:
        return f"SUP-{supplier_article}"
    return None


def _build_product_dict(row: pd.Series, normalized_name: str) -> dict[str, Any]:
    article = _to_text(row.get("article"))
    supplier_article = _to_text(row.get("supplier_article"))
    name = _to_text(row.get("name"))

    return {
        "article": article,
        "supplier_article": supplier_article,
        "internal_article": _make_internal_article(article, supplier_article),
        "name": name,
        "barcode": _to_text(row.get("barcode")),
        "category": _to_text(row.get("category")),
        "supplier_name": _to_text(row.get("supplier_name")),
        "supplier_url": _to_text(row.get("supplier_url")),
        "weight": _to_float(row.get("weight")),
        "length": _to_float(row.get("length")),
        "width": _to_float(row.get("width")),
        "height": _to_float(row.get("height")),
        "package_length": _to_float(row.get("package_length")),
        "package_width": _to_float(row.get("package_width")),
        "package_height": _to_float(row.get("package_height")),
        "gross_weight": _to_float(row.get("gross_weight")),
        "image_url": _to_text(row.get("image_url")),
        "description": _to_text(row.get("description")),
        "normalized_name": normalized_name,
    }


def _apply_enrichment(
    conn: sqlite3.Connection,
    product_data: dict[str, Any],
) -> dict[str, Any]:
    enrich_input = {
        "article": product_data.get("article"),
        "supplier_article": product_data.get("supplier_article"),
        "name": product_data.get("name"),
        "barcode": product_data.get("barcode"),
        "category": product_data.get("category"),
        "supplier_url": product_data.get("supplier_url"),
        "weight": product_data.get("weight"),
        "length": product_data.get("length"),
        "width": product_data.get("width"),
        "height": product_data.get("height"),
    }

    enriched, _method = enrich_product(
        enrich_input,
        conn=conn,
        openai_api_key="",
        force=False,
    )

    if enriched.get("category"):
        product_data["category"] = enriched.get("category")

    if enriched.get("weight") is not None:
        product_data["weight"] = enriched.get("weight")
    if enriched.get("length") is not None:
        product_data["length"] = enriched.get("length")
    if enriched.get("width") is not None:
        product_data["width"] = enriched.get("width")
    if enriched.get("height") is not None:
        product_data["height"] = enriched.get("height")

    if enriched.get("package_length_cm") is not None:
        product_data["package_length"] = enriched.get("package_length_cm")
    if enriched.get("package_width_cm") is not None:
        product_data["package_width"] = enriched.get("package_width_cm")
    if enriched.get("package_height_cm") is not None:
        product_data["package_height"] = enriched.get("package_height_cm")
    if enriched.get("package_weight_kg") is not None:
        product_data["gross_weight"] = enriched.get("package_weight_kg")

    product_data["subcategory"] = enriched.get("subcategory")
    product_data["wheel_diameter_inch"] = enriched.get("wheel_diameter_inch")
    product_data["enrichment_status"] = enriched.get("enrich_status") or "new"
    product_data["enrichment_comment"] = enriched.get("enrich_source") or None

    return product_data


def import_catalog_from_excel(conn: sqlite3.Connection, excel_path: Path) -> ImportResult:
    init_db(conn)
    df = pd.read_excel(excel_path)
    normalized = normalize_columns(df)

    created = 0
    updated = 0
    duplicates: list[dict[str, Any]] = []
    batch_id = uuid4().hex

    for _, row in normalized.iterrows():
        article = _to_text(row.get("article"))
        supplier_article = _to_text(row.get("supplier_article"))
        name = _to_text(row.get("name"))

        if not name:
            continue

        if not article and not supplier_article:
            continue

        article_key = article or f"SUP-{supplier_article}"
        normalized_name = normalize_name(name)
        now = datetime.utcnow().isoformat(timespec="seconds")

        product_data = _build_product_dict(row, normalized_name)
        product_data["article"] = article_key
        product_data["internal_article"] = _make_internal_article(article_key, supplier_article)
        product_data = _apply_enrichment(conn, product_data)

        exists = conn.execute(
            "SELECT id FROM products WHERE article = ?",
            (article_key,),
        ).fetchone()

        if exists:
            conn.execute(
                """
                UPDATE products
                SET
                    name = ?,
                    barcode = ?,
                    category = ?,
                    supplier_url = ?,
                    weight = ?,
                    length = ?,
                    width = ?,
                    height = ?,
                    package_length = ?,
                    package_width = ?,
                    package_height = ?,
                    gross_weight = ?,
                    image_url = ?,
                    description = ?,
                    normalized_name = ?,
                    supplier_name = ?,
                    supplier_article = ?,
                    internal_article = ?,
                    subcategory = ?,
                    wheel_diameter_inch = ?,
                    enrichment_status = ?,
                    enrichment_comment = ?,
                    updated_at = ?,
                    import_batch_id = ?
                WHERE article = ?
                """,
                (
                    product_data["name"],
                    product_data["barcode"],
                    product_data["category"],
                    product_data["supplier_url"],
                    product_data["weight"],
                    product_data["length"],
                    product_data["width"],
                    product_data["height"],
                    product_data["package_length"],
                    product_data["package_width"],
                    product_data["package_height"],
                    product_data["gross_weight"],
                    product_data["image_url"],
                    product_data["description"],
                    product_data["normalized_name"],
                    product_data["supplier_name"],
                    product_data["supplier_article"],
                    product_data["internal_article"],
                    product_data["subcategory"],
                    product_data["wheel_diameter_inch"],
                    product_data["enrichment_status"],
                    product_data["enrichment_comment"],
                    now,
                    batch_id,
                    article_key,
                ),
            )
            product_id = int(exists["id"])
            updated += 1
        else:
            cur = conn.execute(
                """
                INSERT INTO products (
                    article,
                    name,
                    barcode,
                    category,
                    supplier_url,
                    weight,
                    length,
                    width,
                    height,
                    package_length,
                    package_width,
                    package_height,
                    gross_weight,
                    image_url,
                    description,
                    normalized_name,
                    enrichment_status,
                    enrichment_comment,
                    duplicate_status,
                    supplier_name,
                    supplier_article,
                    internal_article,
                    subcategory,
                    wheel_diameter_inch,
                    created_at,
                    updated_at,
                    import_batch_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product_data["article"],
                    product_data["name"],
                    product_data["barcode"],
                    product_data["category"],
                    product_data["supplier_url"],
                    product_data["weight"],
                    product_data["length"],
                    product_data["width"],
                    product_data["height"],
                    product_data["package_length"],
                    product_data["package_width"],
                    product_data["package_height"],
                    product_data["gross_weight"],
                    product_data["image_url"],
                    product_data["description"],
                    product_data["normalized_name"],
                    product_data["enrichment_status"],
                    product_data["enrichment_comment"],
                    None,
                    product_data["supplier_name"],
                    product_data["supplier_article"],
                    product_data["internal_article"],
                    product_data["subcategory"],
                    product_data["wheel_diameter_inch"],
                    now,
                    now,
                    batch_id,
                ),
            )
            product_id = int(cur.lastrowid)
            created += 1

        duplicates.extend(refresh_duplicates_for_product(conn, product_id))

    conn.commit()

    unique_duplicates: list[dict[str, Any]] = []
    seen = set()
    for item in duplicates:
        key = (item["product_id_1"], item["product_id_2"], item["reason"])
        if key not in seen:
            seen.add(key)
            unique_duplicates.append(item)

    return ImportResult(
        imported=created + updated,
        created=created,
        updated=updated,
        duplicates=unique_duplicates,
        batch_id=batch_id,
    )
