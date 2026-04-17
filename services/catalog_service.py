from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote
from uuid import uuid4

import pandas as pd
import sqlite3

from db import init_db
from pim_enrich import enrich_product
from services.attribute_service import set_product_attribute_value
from services.duplicate_service import refresh_duplicates_for_product
from services.text_utils import normalize_name


SUPPORTED_FIELDS = [
    "article",
    "supplier_article",
    "name",
    "brand",
    "barcode",
    "category",
    "base_category",
    "subcategory",
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
    "gallery_images_raw",
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
    "brand": [
        "brand",
        "бренд",
        "торговая марка",
        "производитель",
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
        "группа0",
        "группа 0",
        "group0",
        "группа1",
        "группа 1",
        "group1",
        "группа2",
        "группа 2",
        "group2",
    ],
    "base_category": [
        "base_category",
        "базовая категория",
        "группа0",
        "группа 0",
        "group0",
    ],
    "subcategory": [
        "subcategory",
        "подкатегория",
        "группа2",
        "группа 2",
        "group2",
    ],
    "supplier_name": [
        "supplier_name",
        "поставщик",
        "бренд поставщика",
    ],
    "supplier_url": [
        "supplier_url",
        "url поставщика",
        "ссылка поставщика",
        "supplier link",
        "ссылка на товар на оф. сайте",
        "ссылка на товар на официальном сайте",
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
        "фото №1",
        "фото n1",
        "фото no1",
        "ссылка на фото",
        "ссылка на изображение",
        "ссылки на изображения",
        "ссылки на изображения товара",
        "картинка",
        "изображение",
        "image links",
        "image_links",
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


def _alias_universe() -> set[str]:
    values = set()
    for aliases in COLUMN_MAP.values():
        for alias in aliases:
            values.add(str(alias).strip().lower())
    return values


def _detect_best_sheet_and_header(excel_path: Path, max_scan_rows: int = 12) -> tuple[str, int]:
    xls = pd.ExcelFile(excel_path)
    aliases = _alias_universe()
    best_sheet = xls.sheet_names[0]
    best_header = 0
    best_score = -1

    for sheet in xls.sheet_names:
        try:
            probe = pd.read_excel(excel_path, sheet_name=sheet, header=None, nrows=max_scan_rows)
        except Exception:
            continue
        if probe.empty:
            continue

        for row_idx in range(min(max_scan_rows, len(probe))):
            row_values = [str(v).strip().lower() for v in probe.iloc[row_idx].tolist() if str(v).strip() and str(v).lower() != "nan"]
            if not row_values:
                continue
            score = sum(1 for v in row_values if v in aliases)
            # slight bonus for rows that look like headers (contain multiple short tokens)
            short_tokens = sum(1 for v in row_values if len(v) <= 30)
            score = score * 10 + short_tokens
            if score > best_score:
                best_score = score
                best_sheet = sheet
                best_header = row_idx

    return best_sheet, int(best_header)


def _read_excel_smart(
    excel_path: Path,
    sheet_name: str | None = None,
    header_row: int | None = None,
) -> pd.DataFrame:
    if sheet_name is not None or header_row is not None:
        xls = pd.ExcelFile(excel_path)
        selected_sheet = sheet_name if sheet_name in xls.sheet_names else xls.sheet_names[0]
        selected_header = max(0, int(header_row)) if header_row is not None else 0
        df = pd.read_excel(excel_path, sheet_name=selected_sheet, header=selected_header)
    else:
        selected_sheet, selected_header = _detect_best_sheet_and_header(excel_path)
        df = pd.read_excel(excel_path, sheet_name=selected_sheet, header=selected_header)
    # Drop fully empty columns and rows early.
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
    return df


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

    image_hint_cols: list[str] = []
    for col in normalized.columns:
        label = str(col).strip().lower()
        if label == "image_url" or any(hint in label for hint in ("фото", "image", "картин", "изображ")):
            image_hint_cols.append(col)
    if image_hint_cols:
        detected_main: list[str | None] = []
        detected_gallery: list[str | None] = []
        for _, row in normalized[image_hint_cols].iterrows():
            refs: list[str] = []
            seen: set[str] = set()
            for col in image_hint_cols:
                for ref in _extract_image_refs(row.get(col)):
                    if ref in seen:
                        continue
                    seen.add(ref)
                    refs.append(ref)
            detected_main.append(refs[0] if refs else None)
            detected_gallery.append("\n".join(refs[1:]) if len(refs) > 1 else None)
        if "image_url" in normalized.columns:
            current_image = normalized["image_url"].apply(_normalize_image_ref)
            normalized["image_url"] = current_image.where(current_image.notna(), pd.Series(detected_main, index=normalized.index))
        else:
            normalized["image_url"] = detected_main
        normalized["gallery_images_raw"] = detected_gallery

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


def _looks_like_url(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if text.startswith("http://") or text.startswith("https://"):
        return True
    if " " in text:
        return False
    # Accept domains like example.com/path
    return bool(re.match(r"^[a-z0-9][a-z0-9\.\-]+\.[a-z]{2,}.*$", text))


def _normalize_image_ref(value: object) -> str | None:
    text = _to_text(value)
    if not text:
        return None
    cleaned = text.strip().strip(",;")
    low = cleaned.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return cleaned
    if _looks_like_url(cleaned):
        return f"https://{cleaned}"
    if (cleaned.startswith("\\\\") or re.match(r"^[a-zA-Z]:\\", cleaned)) and re.search(r"\.(jpg|jpeg|png|webp|gif)$", low):
        return cleaned
    return None


def _extract_image_refs(value: object) -> list[str]:
    text = _to_text(value)
    if not text:
        return []
    refs: list[str] = []
    seen: set[str] = set()
    direct = _normalize_image_ref(text)
    if direct:
        refs.append(direct)
        seen.add(direct)
    for chunk in re.split(r"[\n\r\t,;| ]+", text):
        norm = _normalize_image_ref(chunk)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        refs.append(norm)
    return refs


def _normalize_supplier_url(value: object) -> str | None:
    text = _to_text(value)
    if not text:
        return None
    if not _looks_like_url(text):
        return None
    if text.lower().startswith("http://") or text.lower().startswith("https://"):
        return text
    return f"https://{text}"


def _render_url_template(template: str, payload: dict[str, str | None]) -> str:
    rendered = str(template or "")
    for key, raw_val in payload.items():
        safe_val = str(raw_val or "").strip()
        rendered = rendered.replace("{" + key + "}", safe_val)
        rendered = rendered.replace("{" + key + "_q}", quote(safe_val, safe=""))
    return rendered.strip()


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
        "brand": _to_text(row.get("brand")),
        "barcode": _to_text(row.get("barcode")),
        "category": _to_text(row.get("category")),
        "base_category": _to_text(row.get("base_category")) or _to_text(row.get("category")),
        "subcategory": _to_text(row.get("subcategory")),
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
        "image_url": _normalize_image_ref(row.get("image_url")),
        "gallery_images_raw": _to_text(row.get("gallery_images_raw")),
        "description": _to_text(row.get("description")),
        "normalized_name": normalized_name,
    }


def _save_gallery_attributes(conn: sqlite3.Connection, product_id: int, product_data: dict[str, Any]) -> None:
    refs: list[str] = []
    seen: set[str] = set()
    for candidate in [product_data.get("image_url"), product_data.get("gallery_images_raw")]:
        for ref in _extract_image_refs(candidate):
            if ref in seen:
                continue
            seen.add(ref)
            refs.append(ref)
    if not refs:
        return
    set_product_attribute_value(conn, int(product_id), "main_image", refs[0])
    if len(refs) > 1:
        set_product_attribute_value(
            conn,
            int(product_id),
            "gallery_images",
            json.dumps(refs, ensure_ascii=False),
        )


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


def _product_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(products)").fetchall()
    cols: set[str] = set()
    for row in rows:
        if isinstance(row, sqlite3.Row):
            cols.add(str(row["name"]))
        else:
            cols.add(str(row[1]))
    return cols


def _update_product_dynamic(
    conn: sqlite3.Connection,
    article_key: str,
    product_data: dict[str, Any],
    now: str,
    batch_id: str,
    cols: set[str],
) -> None:
    update_order = [
        "name",
        "brand",
        "barcode",
        "category",
        "base_category",
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
        "normalized_name",
        "supplier_name",
        "supplier_article",
        "internal_article",
        "subcategory",
        "wheel_diameter_inch",
        "enrichment_status",
        "enrichment_comment",
    ]
    assignments: list[str] = []
    params: list[Any] = []
    for field in update_order:
        if field in cols:
            assignments.append(f"{field} = ?")
            params.append(product_data.get(field))
    if "updated_at" in cols:
        assignments.append("updated_at = ?")
        params.append(now)
    if "import_batch_id" in cols:
        assignments.append("import_batch_id = ?")
        params.append(batch_id)
    if not assignments:
        return
    params.append(article_key)
    conn.execute(
        f"UPDATE products SET {', '.join(assignments)} WHERE article = ?",
        tuple(params),
    )


def _insert_product_dynamic(
    conn: sqlite3.Connection,
    product_data: dict[str, Any],
    now: str,
    batch_id: str,
    cols: set[str],
) -> sqlite3.Cursor:
    insert_order = [
        "article",
        "name",
        "brand",
        "barcode",
        "category",
        "base_category",
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
        "normalized_name",
        "enrichment_status",
        "enrichment_comment",
        "duplicate_status",
        "supplier_name",
        "supplier_article",
        "internal_article",
        "subcategory",
        "wheel_diameter_inch",
        "created_at",
        "updated_at",
        "import_batch_id",
    ]
    fields: list[str] = []
    values: list[Any] = []
    for field in insert_order:
        if field not in cols:
            continue
        fields.append(field)
        if field == "duplicate_status":
            values.append(None)
        elif field in {"created_at", "updated_at"}:
            values.append(now)
        elif field == "import_batch_id":
            values.append(batch_id)
        else:
            values.append(product_data.get(field))

    placeholders = ", ".join(["?"] * len(fields))
    sql = f"INSERT INTO products ({', '.join(fields)}) VALUES ({placeholders})"
    return conn.execute(sql, tuple(values))


def import_catalog_from_excel(
    conn: sqlite3.Connection,
    excel_path: Path,
    sheet_name: str | None = None,
    header_row: int | None = None,
    default_supplier_name: str | None = None,
    default_supplier_url_template: str | None = None,
) -> ImportResult:
    init_db(conn)
    df = _read_excel_smart(excel_path, sheet_name=sheet_name, header_row=header_row)
    normalized = normalize_columns(df)
    cols = _product_columns(conn)

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
        product_data["supplier_url"] = _normalize_supplier_url(product_data.get("supplier_url"))
        if default_supplier_name and not product_data.get("supplier_name"):
            product_data["supplier_name"] = str(default_supplier_name).strip()
        if not product_data.get("supplier_url") and default_supplier_url_template:
            rendered_url = _render_url_template(
                str(default_supplier_url_template),
                {
                    "article": article_key,
                    "supplier_article": supplier_article or article_key,
                    "name": name,
                    "category": _to_text(row.get("category")),
                    "code": _to_text(row.get("article")) or _to_text(row.get("supplier_article")),
                },
            )
            product_data["supplier_url"] = _normalize_supplier_url(rendered_url)
        product_data = _apply_enrichment(conn, product_data)

        exists = conn.execute(
            "SELECT id FROM products WHERE article = ?",
            (article_key,),
        ).fetchone()

        if exists:
            _update_product_dynamic(
                conn=conn,
                article_key=article_key,
                product_data=product_data,
                now=now,
                batch_id=batch_id,
                cols=cols,
            )
            product_id = int(exists["id"])
            updated += 1
        else:
            cur = _insert_product_dynamic(
                conn=conn,
                product_data=product_data,
                now=now,
                batch_id=batch_id,
                cols=cols,
            )
            product_id = int(cur.lastrowid)
            created += 1

        _save_gallery_attributes(conn, product_id, product_data)
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
