from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("data/catalog.db")


REQUIRED_PRODUCT_COLUMNS: dict[str, str] = {
    "article": "TEXT",
    "name": "TEXT",
    "barcode": "TEXT",
    "category": "TEXT",
    "supplier_url": "TEXT",
    "weight": "REAL",
    "length": "REAL",
    "width": "REAL",
    "height": "REAL",
    "package_length": "REAL",
    "package_width": "REAL",
    "package_height": "REAL",
    "gross_weight": "REAL",
    "is_estimated_logistics": "INTEGER DEFAULT 0",
    "image_url": "TEXT",
    "description": "TEXT",
    "enrichment_status": "TEXT",
    "enrichment_comment": "TEXT",
    "duplicate_status": "TEXT",
    "normalized_name": "TEXT",
    "created_at": "TEXT DEFAULT CURRENT_TIMESTAMP",
    "updated_at": "TEXT",
}


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
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
            category TEXT,
            supplier_url TEXT,
            weight REAL,
            length REAL,
            width REAL,
            height REAL,
            package_length REAL,
            package_width REAL,
            package_height REAL,
            gross_weight REAL,
            is_estimated_logistics INTEGER DEFAULT 0,
            image_url TEXT,
            description TEXT,
            enrichment_status TEXT,
            enrichment_comment TEXT,
            duplicate_status TEXT,
            normalized_name TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_normalized_name ON products(normalized_name)")


def _ensure_duplicate_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS duplicate_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id_1 INTEGER NOT NULL,
            product_id_2 INTEGER NOT NULL,
            similarity_score REAL NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_id_1, product_id_2, reason)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dup_p1 ON duplicate_candidates(product_id_1)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dup_p2 ON duplicate_candidates(product_id_2)")


def init_db(conn: sqlite3.Connection) -> None:
    _ensure_products_table(conn)
    _ensure_duplicate_table(conn)
    conn.commit()
