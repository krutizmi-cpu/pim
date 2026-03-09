from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import sqlite3

from db import init_db

SUPPORTED_FIELDS = [
    "article",
    "name",
    "barcode",
    "weight",
    "length",
    "width",
    "height",
    "supplier_url",
    "image_url",
    "description",
]

COLUMN_MAP: dict[str, list[str]] = {
    "article": ["article", "артикул", "sku", "код товара", "код"],
    "name": ["name", "название", "наименование"],
    "barcode": ["barcode", "ean", "штрихкод"],
    "weight": ["weight", "вес", "вес, кг"],
    "length": ["length", "длина", "длина, см"],
    "width": ["width", "ширина", "ширина, см"],
    "height": ["height", "высота", "высота, см"],
    "supplier_url": ["supplier_url", "url поставщика", "ссылка поставщика", "supplier link", "supplier_url/site"],
    "image_url": ["image_url", "фото", "картинка", "image", "ссылка на фото"],
    "description": ["description", "описание"],
}

TEMPLATE_COLUMNS = [
    "article",
    "name",
    "barcode",
    "weight",
    "length",
    "width",
    "height",
    "supplier_url",
    "image_url",
    "description",
]

TEMPLATE_EXAMPLE = [
    {
        "article": "TDMX850V2",
        "name": "Беговая дорожка UNIX Fit MX-850",
        "barcode": "",
        "weight": "",
        "length": "",
        "width": "",
        "height": "",
        "supplier_url": "https://unixfit.ru/",
        "image_url": "",
        "description": "",
    },
    {
        "article": "BRC-PLT-001",
        "name": "Силовая скамья складная",
        "barcode": "",
        "weight": "18.5",
        "length": "128",
        "width": "46",
        "height": "18",
        "supplier_url": "https://example.com/product",
        "image_url": "",
        "description": "Черновое описание товара",
    },
]

TEMPLATE_INSTRUCTIONS = """
**Как заполнять шаблон**

- `article` — обязательно, уникальный артикул товара в твоей базе.
- `name` — обязательно, наименование товара.
- `barcode` — необязательно, штрихкод / EAN.
- `weight` — необязательно, вес в **кг**.
- `length`, `width`, `height` — необязательно, габариты в **см**.
- `supplier_url` — необязательно, ссылка на сайт поставщика или производителя.
- `image_url` — необязательно, прямая ссылка на фото.
- `description` — необязательно, короткое описание.

**Важно:** для импорта используй лист `catalog`. Листы `example` и `instructions` нужны только как образец.
"""


@dataclass
class ImportResult:
    imported: int
    created: int
    updated: int
    duplicates: list[dict[str, Any]]
    skipped: int
    skipped_rows: list[dict[str, Any]]


def build_catalog_template_excel() -> bytes:
    output = BytesIO()
    catalog_df = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    example_df = pd.DataFrame(TEMPLATE_EXAMPLE)
    instructions_df = pd.DataFrame(
        [
            {"field": "article", "rule": "Обязательно. Уникальный артикул товара в вашей базе."},
            {"field": "name", "rule": "Обязательно. Наименование товара."},
            {"field": "barcode", "rule": "Необязательно. Штрихкод / EAN."},
            {"field": "weight", "rule": "Необязательно. Вес в кг."},
            {"field": "length", "rule": "Необязательно. Длина в см."},
            {"field": "width", "rule": "Необязательно. Ширина в см."},
            {"field": "height", "rule": "Необязательно. Высота в см."},
            {"field": "supplier_url", "rule": "Необязательно. Ссылка на сайт поставщика или производителя."},
            {"field": "image_url", "rule": "Необязательно. Прямая ссылка на фото товара."},
            {"field": "description", "rule": "Необязательно. Черновое описание."},
        ]
    )

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        catalog_df.to_excel(writer, index=False, sheet_name="catalog")
        example_df.to_excel(writer, index=False, sheet_name="example")
        instructions_df.to_excel(writer, index=False, sheet_name="instructions")
    return output.getvalue()


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


def _is_instruction_row(article: str | None, name: str | None) -> bool:
    joined = " ".join([article or "", name or ""]).strip().lower()
    if not joined:
        return True
    markers = [
        "обязательно.",
        "необязательно.",
        "уникальный артикул",
        "наименование товара",
        "штрихкод / ean",
        "ссылка на сайт поставщика",
        "прямая ссылка на фото",
    ]
    return any(marker in joined for marker in markers)


def _name_similarity(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left.lower().strip(), right.lower().strip()).ratio() * 100.0


def _find_name_duplicate(conn: sqlite3.Connection, article: str, name: str, threshold: float = 85.0) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT article, name
        FROM products
        WHERE article != ? AND name IS NOT NULL AND name != ''
        """,
        (article,),
    ).fetchall()

    best: dict[str, Any] | None = None
    score = 0.0
    for row in rows:
        similarity = _name_similarity(name, row["name"])
        if similarity >= threshold and similarity > score:
            score = similarity
            best = {
                "new_article": article,
                "existing_article": row["article"],
                "similarity": round(similarity, 2),
                "reason": "similar_name",
            }
    return best


def import_catalog_from_excel(conn: sqlite3.Connection, excel_path: Path) -> ImportResult:
    init_db(conn)
    sheets = pd.read_excel(excel_path, sheet_name=None)
    df = sheets["catalog"] if "catalog" in sheets else next(iter(sheets.values()))
    normalized = normalize_columns(df)

    created = 0
    updated = 0
    duplicates: list[dict[str, Any]] = []
    skipped = 0
    skipped_rows: list[dict[str, Any]] = []

    for idx, row in normalized.iterrows():
        article = _to_text(row.get("article"))
        name = _to_text(row.get("name"))

        if _is_instruction_row(article, name):
            skipped += 1
            skipped_rows.append({"row": int(idx) + 2, "reason": "empty_or_instruction", "article": article, "name": name})
            continue

        if not article or not name:
            skipped += 1
            skipped_rows.append({"row": int(idx) + 2, "reason": "missing_required_fields", "article": article, "name": name})
            continue

        exists = conn.execute("SELECT id FROM products WHERE article = ?", (article,)).fetchone()
        payload = (
            name,
            _to_text(row.get("barcode")),
            _to_float(row.get("weight")),
            _to_float(row.get("length")),
            _to_float(row.get("width")),
            _to_float(row.get("height")),
            _to_text(row.get("supplier_url")),
            _to_text(row.get("image_url")),
            _to_text(row.get("description")),
        )

        if exists:
            conn.execute(
                """
                UPDATE products
                SET name = ?, barcode = ?, weight = ?, length = ?, width = ?, height = ?,
                    supplier_url = ?, image_url = ?, description = ?, updated_at = ?
                WHERE article = ?
                """,
                (*payload, datetime.utcnow().isoformat(timespec="seconds"), article),
            )
            updated += 1
            continue

        duplicate = _find_name_duplicate(conn, article, name)
        if duplicate:
            conn.execute(
                "INSERT INTO duplicate_candidates (new_article, existing_article, similarity, reason) VALUES (?, ?, ?, ?)",
                (duplicate["new_article"], duplicate["existing_article"], duplicate["similarity"], duplicate["reason"]),
            )
            duplicates.append(duplicate)

        conn.execute(
            """
            INSERT INTO products (
                article, name, barcode, weight, length, width, height,
                supplier_url, image_url, description, enrichment_status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                article,
                *payload,
                "new",
                datetime.utcnow().isoformat(timespec="seconds"),
            ),
        )
        created += 1

    conn.commit()
    return ImportResult(
        imported=created + updated,
        created=created,
        updated=updated,
        duplicates=duplicates,
        skipped=skipped,
        skipped_rows=skipped_rows,
    )


def load_products_df(conn: sqlite3.Connection) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT id, article, name, barcode, weight, length, width, height,
               supplier_url, image_url, description,
               enrichment_status, enrichment_comment, created_at, updated_at
        FROM products
        ORDER BY id DESC
        """
    ).fetchall()
    return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()


def load_product_by_id(conn: sqlite3.Connection, product_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT id, article, name, barcode, weight, length, width, height,
               supplier_url, image_url, description,
               enrichment_status, enrichment_comment, created_at, updated_at
        FROM products
        WHERE id = ?
        """,
        (product_id,),
    ).fetchone()


def save_product(
    conn: sqlite3.Connection,
    *,
    product_id: int,
    article: str,
    name: str,
    barcode: str | None,
    weight: float | None,
    length: float | None,
    width: float | None,
    height: float | None,
    supplier_url: str | None,
    image_url: str | None,
    description: str | None,
    enrichment_status: str | None,
    enrichment_comment: str | None,
) -> None:
    conn.execute(
        """
        UPDATE products
        SET article = ?,
            name = ?,
            barcode = ?,
            weight = ?,
            length = ?,
            width = ?,
            height = ?,
            supplier_url = ?,
            image_url = ?,
            description = ?,
            enrichment_status = ?,
            enrichment_comment = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            article,
            name,
            barcode,
            weight,
            length,
            width,
            height,
            supplier_url,
            image_url,
            description,
            enrichment_status,
            enrichment_comment,
            datetime.utcnow().isoformat(timespec="seconds"),
            product_id,
        ),
    )
    conn.commit()
