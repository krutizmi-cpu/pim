from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("data/catalog.db")


REQUIRED_PRODUCT_COLUMNS: dict[str, str] = {
    "article": "TEXT",
    "name": "TEXT",
    "barcode": "TEXT",
    "weight": "REAL",
    "length": "REAL",
    "width": "REAL",
    "height": "REAL",
    "supplier_url": "TEXT",
    "image_url": "TEXT",
    "description": "TEXT",
    "created_at": "TEXT DEFAULT CURRENT_TIMESTAMP",
}


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _ensure_products_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            barcode TEXT,
            weight REAL,
            length REAL,
            width REAL,
            height REAL,
            supplier_url TEXT,
            image_url TEXT,
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    existing = _table_columns(conn, "products")
    for col, col_type in REQUIRED_PRODUCT_COLUMNS.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")

    conn.execute(
        """
        DELETE FROM products
        WHERE article IS NOT NULL
          AND rowid NOT IN (
              SELECT MAX(rowid) FROM products WHERE article IS NOT NULL GROUP BY article
          )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_article_unique ON products(article)")


def init_db(conn: sqlite3.Connection) -> None:
    _ensure_products_table(conn)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS duplicate_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            new_article TEXT NOT NULL,
            existing_article TEXT NOT NULL,
            similarity REAL NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
