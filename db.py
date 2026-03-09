def _ensure_products_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article TEXT UNIQUE,
        name TEXT,
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

        description TEXT,
        image_url TEXT,

        enrichment_status TEXT,
        enrichment_comment TEXT,

        duplicate_status TEXT,

        normalized_name TEXT,

        created_at TEXT,
        updated_at TEXT
    )
    """)
