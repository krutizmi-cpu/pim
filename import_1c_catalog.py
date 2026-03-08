from __future__ import annotations

import argparse
import shutil
import sqlite3
from pathlib import Path

import pandas as pd

from db import count_rows, get_connection, get_or_create_category, init_db

DATA_DIR = Path("data")

COLUMN_MAP = {
    "article": ["article", "артикул", "sku", "код"],
    "name": ["name", "название", "наименование"],
    "brand": ["brand", "бренд", "марка"],
    "barcode": ["barcode", "ean", "штрихкод"],
    "category_path": ["category_path", "категория", "путь категории", "category"],
    "weight": ["weight", "вес"],
    "length": ["length", "длина"],
    "width": ["width", "ширина"],
    "height": ["height", "высота"],
    "description": ["description", "описание"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Импорт каталога 1С из Excel в SQLite")
    parser.add_argument("excel_path", help="Путь до Excel файла каталога")
    return parser.parse_args()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    source_columns = {c.lower().strip(): c for c in df.columns}

    for target, aliases in COLUMN_MAP.items():
        for alias in aliases:
            source_col = source_columns.get(alias)
            if source_col:
                renamed[source_col] = target
                break

    normalized = df.rename(columns=renamed).copy()

    for required_col in COLUMN_MAP.keys():
        if required_col not in normalized.columns:
            normalized[required_col] = None

    return normalized[list(COLUMN_MAP.keys())]


def clean_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text or None


def as_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def save_excel_copy(src_path: Path) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dst_path = DATA_DIR / src_path.name
    shutil.copy2(src_path, dst_path)
    return dst_path


def insert_products(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    inserted = 0
    for _, row in df.iterrows():
        category_id = get_or_create_category(conn, row.get("category_path"))

        conn.execute(
            """
            INSERT INTO products (
                article, name, brand, barcode, category_id,
                weight, length, width, height, description
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                clean_text(row.get("article")),
                clean_text(row.get("name")),
                clean_text(row.get("brand")),
                clean_text(row.get("barcode")),
                category_id,
                as_float(row.get("weight")),
                as_float(row.get("length")),
                as_float(row.get("width")),
                as_float(row.get("height")),
                clean_text(row.get("description")),
            ),
        )
        inserted += 1

    conn.commit()
    return inserted


def print_examples(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT p.id, p.article, p.name, p.brand, p.barcode, c.path AS category_path
        FROM products p
        LEFT JOIN categories c ON c.id = p.category_id
        ORDER BY p.id DESC
        LIMIT 5
        """
    ).fetchall()

    print("\n5 примеров товаров:")
    for row in rows:
        print(
            {
                "id": row["id"],
                "article": row["article"],
                "name": row["name"],
                "brand": row["brand"],
                "barcode": row["barcode"],
                "category_path": row["category_path"],
            }
        )


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel_path)

    if not excel_path.exists():
        raise SystemExit(f"Файл не найден: {excel_path}")

    copied_excel = save_excel_copy(excel_path)
    print(f"Исходный Excel скопирован в: {copied_excel}")

    df = pd.read_excel(excel_path)
    normalized_df = normalize_columns(df)

    conn = get_connection()
    init_db(conn)

    inserted_count = insert_products(conn, normalized_df)
    categories_count = count_rows(conn, "categories")

    print(f"Количество товаров (добавлено): {inserted_count}")
    print(f"Количество категорий (всего): {categories_count}")
    print_examples(conn)

    conn.close()


if __name__ == "__main__":
    main()
