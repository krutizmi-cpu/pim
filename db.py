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
    # PIM intake / master extensions
    "supplier_name": "TEXT",
    "supplier_article": "TEXT",
    "internal_article": "TEXT",
    "barcode_source": "TEXT",
    "uom": "TEXT",
    "subcategory": "TEXT",
    "wheel_diameter_inch": "REAL",
}


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row['name']) for row in rows}


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
            updated_at TEXT,
            supplier_name TEXT,
            supplier_article TEXT,
            internal_article TEXT,
            barcode_source TEXT,
            uom TEXT,
            subcategory TEXT,
            wheel_diameter_inch REAL
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
              SELECT MAX(rowid)
              FROM products
              WHERE article IS NOT NULL
              GROUP BY article
          )
        """
    )

    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_article_unique ON products(article)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_normalized_name ON products(normalized_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_supplier_article ON products(supplier_article)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_internal_article ON products(internal_article)")


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


def _ensure_product_intake_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_intake (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_name TEXT,
            supplier_article TEXT,
            supplier_url TEXT,
            raw_name TEXT NOT NULL,
            barcode TEXT,
            barcode_source TEXT,
            uom TEXT,
            internal_article TEXT,
            duplicate_status TEXT DEFAULT 'unchecked',
            intake_status TEXT DEFAULT 'draft',
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_product_intake_supplier_article ON product_intake(supplier_article)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_product_intake_barcode ON product_intake(barcode)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_product_intake_status ON product_intake(intake_status)")


def _ensure_category_defaults_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS category_defaults (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            base_category TEXT NOT NULL,
            subcategory TEXT,
            wheel_diameter_inch REAL,
            length_cm REAL,
            width_cm REAL,
            height_cm REAL,
            weight_kg REAL,
            package_length_cm REAL,
            package_width_cm REAL,
            package_height_cm REAL,
            package_weight_kg REAL,
            priority INTEGER DEFAULT 100,
            comment TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_category_defaults_match
        ON category_defaults(base_category, subcategory, wheel_diameter_inch)
        """
    )


def _seed_category_defaults(conn: sqlite3.Connection) -> None:
    count = conn.execute("SELECT COUNT(*) AS cnt FROM category_defaults").fetchone()["cnt"]
    if count and int(count) > 0:
        return

    rows = [
        ("bicycle", "child_bicycle", 12.0, 90.0, 18.0, 55.0, 8.5, 92.0, 20.0, 58.0, 10.0, 1, "детский 12"),
        ("bicycle", "child_bicycle", 14.0, 98.0, 18.0, 60.0, 9.5, 100.0, 20.0, 62.0, 11.0, 1, "детский 14"),
        ("bicycle", "child_bicycle", 16.0, 110.0, 18.0, 65.0, 10.5, 112.0, 20.0, 67.0, 12.0, 1, "детский 16"),
        ("bicycle", "child_bicycle", 18.0, 118.0, 20.0, 70.0, 11.5, 120.0, 22.0, 72.0, 13.0, 1, "детский 18"),
        ("bicycle", "child_bicycle", 20.0, 128.0, 20.0, 75.0, 12.8, 130.0, 22.0, 77.0, 14.5, 1, "детский 20"),
        ("bicycle", "teen_bicycle", 24.0, 145.0, 22.0, 85.0, 14.5, 148.0, 24.0, 88.0, 16.5, 1, "подростковый 24"),
        ("bicycle", "adult_bicycle", 26.0, 170.0, 25.0, 100.0, 16.5, 172.0, 27.0, 103.0, 19.0, 1, "взрослый 26"),
        ("bicycle", "adult_bicycle", 27.5, 175.0, 25.0, 102.0, 17.2, 178.0, 27.0, 105.0, 19.8, 1, "взрослый 27.5"),
        ("bicycle", "adult_bicycle", 29.0, 180.0, 25.0, 105.0, 18.0, 183.0, 27.0, 108.0, 20.5, 1, "взрослый 29"),
    ]

    conn.executemany(
        """
        INSERT INTO category_defaults (
            base_category,
            subcategory,
            wheel_diameter_inch,
            length_cm,
            width_cm,
            height_cm,
            weight_kg,
            package_length_cm,
            package_width_cm,
            package_height_cm,
            package_weight_kg,
            priority,
            comment
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def init_db(conn: sqlite3.Connection) -> None:
    _ensure_products_table(conn)
    _ensure_duplicate_table(conn)
    _ensure_product_intake_table(conn)
    _ensure_category_defaults_table(conn)
    _seed_category_defaults(conn)
    conn.commit()
