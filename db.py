from __future__ import annotations

import os
import sqlite3
import tempfile
import shutil
from pathlib import Path

DB_PATH = Path("data/catalog.db")
_ACTIVE_DB_PATH: Path | None = None


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
    "supplier_name": "TEXT",
    "supplier_article": "TEXT",
    "internal_article": "TEXT",
    "barcode_source": "TEXT",
    "uom": "TEXT",
    "subcategory": "TEXT",
    "wheel_diameter_inch": "REAL",
    "brand": "TEXT",
    "model": "TEXT",
    "base_category": "TEXT",
    "ozon_description_category_id": "INTEGER",
    "ozon_type_id": "INTEGER",
    "ozon_category_path": "TEXT",
    "ozon_category_confidence": "REAL",
    "tnved_code": "TEXT",
    "import_batch_id": "TEXT",
    "supplier_last_parsed_at": "TEXT",
    "supplier_parse_status": "TEXT",
    "supplier_parse_comment": "TEXT",
}


def _candidate_db_paths(db_path: Path) -> list[Path]:
    env_path = (os.getenv("PIM_DB_PATH") or os.getenv("DATABASE_PATH") or "").strip()
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path).expanduser())
    # Prefer user-home persistent path over repo-local data path.
    candidates.append(Path.home() / ".pim" / "catalog.db")
    candidates.append(Path(db_path).expanduser())
    candidates.append(Path.cwd() / "data" / "catalog.db")
    candidates.append(Path(tempfile.gettempdir()) / "pim" / "catalog.db")
    unique: list[Path] = []
    seen: set[str] = set()
    for item in candidates:
        key = str(item.resolve()) if item.exists() else str(item)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _db_is_readable_with_products(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(path)
        row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='products'
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return False
        cnt = conn.execute("SELECT COUNT(*) FROM products").fetchone()
        return bool(cnt and int(cnt[0] or 0) > 0)
    except Exception:
        return False
    finally:
        if conn is not None:
            conn.close()


def _seed_preferred_db_if_needed(preferred: Path, fallbacks: list[Path]) -> None:
    """
    If preferred DB is absent/empty, try to copy the richest existing DB from fallback paths.
    This protects catalog data when app path changes (e.g., Streamlit Cloud redeploy).
    """
    if preferred.exists() and preferred.is_file():
        return

    source: Path | None = None
    for candidate in fallbacks:
        if candidate.resolve() == preferred.resolve():
            continue
        if _db_is_readable_with_products(candidate):
            source = candidate
            break
    if source is None:
        return

    try:
        preferred.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, preferred)
    except Exception:
        # Best effort only; normal candidate probing continues.
        return


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    global _ACTIVE_DB_PATH

    def _connect(path: Path) -> sqlite3.Connection:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn_local = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
        conn_local.row_factory = sqlite3.Row
        return conn_local

    candidates = _candidate_db_paths(Path(db_path))
    if candidates:
        _seed_preferred_db_if_needed(candidates[0], candidates[1:])

    last_error: Exception | None = None
    for candidate in candidates:
        conn = _connect(candidate)
        try:
            # Fast writeability probe for read-only mount cases.
            conn.execute("CREATE TABLE IF NOT EXISTS _pim_rw_probe (id INTEGER PRIMARY KEY)")
            conn.commit()
            _ACTIVE_DB_PATH = candidate
            return conn
        except sqlite3.OperationalError as e:
            conn.close()
            last_error = e
            msg = str(e).lower()
            if "readonly" not in msg and "read-only" not in msg:
                raise
            continue

    if last_error:
        raise last_error
    raise RuntimeError("Не удалось открыть SQLite базу данных")


def get_active_db_path() -> str | None:
    if _ACTIVE_DB_PATH is None:
        return None
    return str(_ACTIVE_DB_PATH)


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
            updated_at TEXT,
            supplier_name TEXT,
            supplier_article TEXT,
            internal_article TEXT,
            barcode_source TEXT,
            uom TEXT,
            subcategory TEXT,
            wheel_diameter_inch REAL,
            brand TEXT,
            model TEXT,
            base_category TEXT,
            ozon_description_category_id INTEGER,
            ozon_type_id INTEGER,
            ozon_category_path TEXT,
            ozon_category_confidence REAL,
            tnved_code TEXT,
            import_batch_id TEXT,
            supplier_last_parsed_at TEXT,
            supplier_parse_status TEXT,
            supplier_parse_comment TEXT
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_base_category ON products(base_category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_normalized_name ON products(normalized_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_supplier_article ON products(supplier_article)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_internal_article ON products(internal_article)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_import_batch_id ON products(import_batch_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_ozon_category ON products(ozon_description_category_id, ozon_type_id)")


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


def _ensure_attribute_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS attribute_definitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            data_type TEXT NOT NULL DEFAULT 'text',
            scope TEXT NOT NULL DEFAULT 'master',
            entity_type TEXT NOT NULL DEFAULT 'product',
            is_required INTEGER DEFAULT 0,
            is_multi_value INTEGER DEFAULT 0,
            unit TEXT,
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_attr_defs_scope ON attribute_definitions(scope)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_attr_defs_entity_type ON attribute_definitions(entity_type)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_attribute_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            attribute_code TEXT NOT NULL,
            value_text TEXT,
            value_number REAL,
            value_boolean INTEGER,
            value_json TEXT,
            locale TEXT,
            channel_code TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pav_product ON product_attribute_values(product_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pav_attr ON product_attribute_values(attribute_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pav_channel ON product_attribute_values(channel_code)")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pav_unique
        ON product_attribute_values(
            product_id,
            attribute_code,
            IFNULL(channel_code, ''),
            IFNULL(locale, '')
        )
        """
    )


def _ensure_product_data_sources_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_data_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_value_raw TEXT,
            source_url TEXT,
            confidence REAL,
            is_manual INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pds_product_id ON product_data_sources(product_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pds_field_name ON product_data_sources(field_name)")


def _ensure_system_settings_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _ensure_template_profile_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS template_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_name TEXT NOT NULL,
            channel_code TEXT NOT NULL,
            category_code TEXT,
            file_name TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tp_channel_code ON template_profiles(channel_code)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS template_profile_columns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL,
            template_column TEXT NOT NULL,
            source_type TEXT,
            source_name TEXT,
            matched_by TEXT,
            transform_rule TEXT,
            sort_order INTEGER DEFAULT 100,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tpc_profile_id ON template_profile_columns(profile_id)")


def _ensure_supplier_profiles_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS supplier_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_name TEXT NOT NULL,
            base_url TEXT,
            url_template TEXT,
            is_active INTEGER DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_supplier_profiles_name ON supplier_profiles(supplier_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_supplier_profiles_active ON supplier_profiles(is_active)")


def _ensure_ozon_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ozon_category_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description_category_id INTEGER,
            category_name TEXT,
            full_path TEXT,
            type_id INTEGER,
            type_name TEXT,
            disabled INTEGER DEFAULT 0,
            children_count INTEGER DEFAULT 0,
            raw_json TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    existing_cat_cols = _table_columns(conn, "ozon_category_cache")
    if "full_path" not in existing_cat_cols:
        conn.execute("ALTER TABLE ozon_category_cache ADD COLUMN full_path TEXT")
    if "children_count" not in existing_cat_cols:
        conn.execute("ALTER TABLE ozon_category_cache ADD COLUMN children_count INTEGER DEFAULT 0")
    conn.execute("DROP INDEX IF EXISTS idx_ozon_category_unique")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ozon_cat_desc ON ozon_category_cache(description_category_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ozon_cat_type ON ozon_category_cache(type_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ozon_cat_path ON ozon_category_cache(full_path)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ozon_attribute_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description_category_id INTEGER NOT NULL,
            type_id INTEGER NOT NULL,
            attribute_id INTEGER NOT NULL,
            name TEXT,
            description TEXT,
            type TEXT,
            group_id INTEGER,
            group_name TEXT,
            dictionary_id INTEGER,
            is_required INTEGER DEFAULT 0,
            is_collection INTEGER DEFAULT 0,
            max_value_count INTEGER,
            category_dependent INTEGER DEFAULT 0,
            raw_json TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ozon_attr_desc_type ON ozon_attribute_cache(description_category_id, type_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ozon_attr_attr_id ON ozon_attribute_cache(attribute_id)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ozon_attribute_value_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description_category_id INTEGER NOT NULL,
            type_id INTEGER NOT NULL,
            attribute_id INTEGER NOT NULL,
            dictionary_id INTEGER,
            value_id INTEGER NOT NULL,
            value TEXT,
            info TEXT,
            picture TEXT,
            raw_json TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ozon_attr_val_key ON ozon_attribute_value_cache(description_category_id, type_id, attribute_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ozon_attr_val_dict ON ozon_attribute_value_cache(dictionary_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ozon_attr_val_value_id ON ozon_attribute_value_cache(value_id)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ozon_dictionary_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description_category_id INTEGER NOT NULL,
            type_id INTEGER NOT NULL,
            attribute_id INTEGER NOT NULL,
            raw_value TEXT NOT NULL,
            normalized_raw_value TEXT NOT NULL,
            value_id INTEGER NOT NULL,
            value TEXT,
            comment TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ozon_dict_override_unique
        ON ozon_dictionary_overrides(
            description_category_id,
            type_id,
            attribute_id,
            normalized_raw_value
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ozon_dict_override_attr ON ozon_dictionary_overrides(attribute_id)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ozon_update_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description_category_id INTEGER,
            type_id INTEGER,
            offer_id_field TEXT,
            items_count INTEGER DEFAULT 0,
            request_json TEXT,
            response_json TEXT,
            status TEXT,
            task_id TEXT,
            retry_of_job_id INTEGER,
            error_message TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    existing_job_cols = _table_columns(conn, "ozon_update_jobs")
    if "task_id" not in existing_job_cols:
        conn.execute("ALTER TABLE ozon_update_jobs ADD COLUMN task_id TEXT")
    if "retry_of_job_id" not in existing_job_cols:
        conn.execute("ALTER TABLE ozon_update_jobs ADD COLUMN retry_of_job_id INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ozon_jobs_created ON ozon_update_jobs(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ozon_jobs_status ON ozon_update_jobs(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ozon_jobs_retry_of ON ozon_update_jobs(retry_of_job_id)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ozon_update_job_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            offer_id TEXT,
            product_id INTEGER,
            description_category_id INTEGER,
            type_id INTEGER,
            attributes_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ozon_job_items_job ON ozon_update_job_items(job_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ozon_job_items_offer ON ozon_update_job_items(offer_id)")


def _ensure_ozon_catalog_mapping_memory_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ozon_catalog_mapping_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mapping_key TEXT NOT NULL UNIQUE,
            supplier_name TEXT,
            category TEXT,
            base_category TEXT,
            subcategory TEXT,
            description_category_id INTEGER NOT NULL,
            type_id INTEGER NOT NULL,
            ozon_category_path TEXT,
            confidence REAL DEFAULT 0.0,
            hit_count INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ocmm_desc_type ON ozon_catalog_mapping_memory(description_category_id, type_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ocmm_supplier ON ozon_catalog_mapping_memory(supplier_name)")



def _ensure_channel_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_code TEXT UNIQUE NOT NULL,
            channel_name TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_attribute_requirements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_code TEXT NOT NULL,
            category_code TEXT,
            attribute_code TEXT NOT NULL,
            is_required INTEGER DEFAULT 0,
            sort_order INTEGER DEFAULT 100,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_car_channel ON channel_attribute_requirements(channel_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_car_category ON channel_attribute_requirements(category_code)")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_car_unique
        ON channel_attribute_requirements(
            channel_code,
            IFNULL(category_code, ''),
            attribute_code
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_mapping_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_code TEXT NOT NULL,
            category_code TEXT,
            target_field TEXT NOT NULL,
            source_type TEXT NOT NULL DEFAULT 'attribute',
            source_name TEXT NOT NULL,
            transform_rule TEXT,
            is_required INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cmr_channel ON channel_mapping_rules(channel_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cmr_category ON channel_mapping_rules(category_code)")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cmr_unique
        ON channel_mapping_rules(
            channel_code,
            IFNULL(category_code, ''),
            target_field
        )
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


def _seed_attribute_definitions(conn: sqlite3.Connection) -> None:
    rows = [
        ("brand", "Бренд", "text", "master", 0, 0, None, "Бренд товара"),
        ("model", "Модель", "text", "master", 0, 0, None, "Модель товара"),
        ("color", "Цвет", "text", "master", 0, 0, None, "Цвет"),
        ("material", "Материал", "text", "master", 0, 0, None, "Материал"),
        ("country_of_origin", "Страна производства", "text", "master", 0, 0, None, "Страна производства"),
        ("wheel_diameter_inch", "Диаметр колеса", "number", "master", 0, 0, "inch", "Диаметр колеса в дюймах"),
        ("frame_size", "Размер рамы", "text", "master", 0, 0, None, "Размер рамы"),
        ("brake_type", "Тип тормоза", "text", "master", 0, 0, None, "Тип тормоза"),
        ("age_group", "Возрастная группа", "text", "master", 0, 0, None, "Возрастная группа"),
        ("gender", "Пол", "text", "master", 0, 0, None, "Пол"),
        ("main_image", "Главное фото", "text", "master", 0, 0, None, "Главное фото"),
        ("gallery_images", "Галерея фото", "json", "master", 0, 1, None, "Дополнительные фото"),
    ]

    for code, name, data_type, scope, is_required, is_multi_value, unit, description in rows:
        conn.execute(
            """
            INSERT OR IGNORE INTO attribute_definitions
            (code, name, data_type, scope, entity_type, is_required, is_multi_value, unit, description)
            VALUES (?, ?, ?, ?, 'product', ?, ?, ?, ?)
            """,
            (code, name, data_type, scope, is_required, is_multi_value, unit, description),
        )


def _seed_channels(conn: sqlite3.Connection) -> None:
    rows = [
        ("detmir", "Детский Мир"),
        ("ozon", "Ozon"),
        ("wildberries", "Wildberries"),
        ("sportmaster", "Спортмастер"),
        ("mvideo", "М.Видео"),
    ]
    for channel_code, channel_name in rows:
        conn.execute(
            """
            INSERT OR IGNORE INTO channel_profiles (channel_code, channel_name, is_active)
            VALUES (?, ?, 1)
            """,
            (channel_code, channel_name),
        )


def _seed_supplier_profiles(conn: sqlite3.Connection) -> None:
    defaults = [
        (
            "Веломай",
            "https://technosite.ru/",
            "https://technosite.ru/search/?q={article_q}",
            "Базовый профиль поставщика для ассортимента велосипедов под Детский Мир.",
        ),
        (
            "Рокви",
            "https://velocitygroup.ru/?category=velo",
            "https://velocitygroup.ru/?category=velo&search={article_q}",
            "Профиль поставщика для ассортимента Rockbros / Moon / SKS.",
        ),
    ]
    for supplier_name, base_url, url_template, notes in defaults:
        conn.execute(
            """
            INSERT OR IGNORE INTO supplier_profiles (supplier_name, base_url, url_template, is_active, notes, created_at, updated_at)
            VALUES (?, ?, ?, 1, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (supplier_name, base_url, url_template, notes),
        )
        conn.execute(
            """
            UPDATE supplier_profiles
            SET base_url = COALESCE(NULLIF(base_url, ''), ?),
                url_template = COALESCE(NULLIF(url_template, ''), ?),
                notes = COALESCE(NULLIF(notes, ''), ?),
                updated_at = CURRENT_TIMESTAMP
            WHERE supplier_name = ?
            """,
            (base_url, url_template, notes, supplier_name),
        )


def init_db(conn: sqlite3.Connection) -> None:
    _ensure_products_table(conn)
    _ensure_duplicate_table(conn)
    _ensure_product_intake_table(conn)
    _ensure_category_defaults_table(conn)
    _ensure_attribute_tables(conn)
    _ensure_product_data_sources_table(conn)
    _ensure_system_settings_table(conn)
    _ensure_template_profile_tables(conn)
    _ensure_supplier_profiles_table(conn)
    _ensure_ozon_tables(conn)
    _ensure_ozon_catalog_mapping_memory_table(conn)
    _ensure_channel_tables(conn)

    _seed_category_defaults(conn)
    _seed_attribute_definitions(conn)
    _seed_channels(conn)
    _seed_supplier_profiles(conn)

    conn.commit()
