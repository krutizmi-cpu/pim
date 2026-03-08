from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
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
    "weight": ["weight", "вес"],
    "length": ["length", "длина"],
    "width": ["width", "ширина"],
    "height": ["height", "высота"],
    "supplier_url": ["supplier_url", "url поставщика", "ссылка поставщика", "supplier link"],
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


def _name_similarity(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left.lower().strip(), right.lower().strip()).ratio() * 100.0


def _find_name_duplicate(
    conn: sqlite3.Connection,
    incoming_article: str,
    incoming_name: str,
    threshold: float = 85.0,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT article, name
        FROM products
        WHERE article != ?
          AND name IS NOT NULL
          AND name != ''
        """,
        (incoming_article,),
    ).fetchall()

    best_match: dict[str, Any] | None = None
    best_score = 0.0

    for item in row:
        similarity = _name_similarity(incoming_name, item["name"])
        if similarity >= threshold and similarity > best_score:
            best_score = similarity
            best_match = {
                "new_article": incoming_article,
                "existing_article": item["article"],
                "similarity": round(similarity, 2),
                "reason": "similar_name",
            }

    return best_match


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

        exists = conn.execute(
            "SELECT id FROM products WHERE article = ?",
            (article,),
        ).fetchone()

        if exists:
            conn.execute(
                """
                UPDATE products
                SET name = ?, barcode = ?, weight = ?, length = ?, width = ?,
                    height = ?, supplier_url = ?, description = ?, image_url = ?
                WHERE article = ?
                """,
                (
                    name,
                    _to_text(row.get("barcode")),
                    _to_float(row.get("weight")),
                    _to_float(row.get("length")),
                    _to_float(row.get("width")),
                    _to_float(row.get("height")),
                    _to_text(row.get("supplier_url")),
                    _to_text(row.get("description")),
                    _to_text(row.get("image_url")),
                    article,
                ),
            )
            updated += 1
            continue

        duplicate = _find_name_duplicate(conn, article, name)
        if duplicate:
            conn.execute(
                """
                INSERT INTO duplicate_candidates
                (new_article, existing_article, similarity, reason)
                VALUES (?, ?, ?, ?)
                """,
                (
                    duplicate["new_article"],
                    duplicate["existing_article"],
                    duplicate["similarity"],
                    duplicate["reason"],
                ),
            )
            duplicates.append(duplicate)

        conn.execute(
            """
            INSERT INTO products (
                article, name, barcode, weight, length, width, height,
                supplier_url, image_url, description
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                article,
                name,
                _to_text(row.get("barcode")),
                _to_float(row.get("weight")),
                _to_float(row.get("length")),
                _to_float(row.get("width")),
                _to_float(row.get("height")),
                _to_text(row.get("supplier_url")),
                _to_text(row.get("image_url")),
                _to_text(row.get("description")),
            ),
        )
        created += 1

    conn.commit()

    return ImportResult(
        imported=created + updated,
        created=created,
        updated=updated,
        duplicates=duplicates,
    )
