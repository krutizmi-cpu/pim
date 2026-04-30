from __future__ import annotations

import os
import sqlite3
import tempfile
import shutil
import json
from pathlib import Path

DB_PATH = Path("data/catalog.db")
_ACTIVE_DB_PATH: Path | None = None
PIM_HOME_DIR = Path.home() / ".pim"
PERSISTENT_DB_PATH = PIM_HOME_DIR / "catalog.db"
PERSISTENT_UPLOADS_DIR = PIM_HOME_DIR / "uploads"
PERSISTENT_IMPORTS_DIR = PERSISTENT_UPLOADS_DIR / "imports"
PERSISTENT_TEMPLATES_DIR = PERSISTENT_UPLOADS_DIR / "templates"
PERSISTENT_BACKUPS_DIR = PIM_HOME_DIR / "backups"
PERSISTENT_DB_BACKUPS_DIR = PERSISTENT_BACKUPS_DIR / "db"
PERSISTENT_OZON_BACKUPS_DIR = PERSISTENT_BACKUPS_DIR / "ozon"


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
    candidates.append(PERSISTENT_DB_PATH)
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
    for discovered in _discover_additional_catalog_db_paths():
        key = str(discovered.resolve()) if discovered.exists() else str(discovered)
        if key in seen:
            continue
        seen.add(key)
        unique.append(discovered)
    return unique


def _discover_additional_catalog_db_paths() -> list[Path]:
    results: list[Path] = []
    seen: set[str] = set()
    roots: list[Path] = []
    cwd = Path.cwd()
    roots.append(cwd)
    if cwd.parent != cwd:
        roots.append(cwd.parent)
    home = Path.home()
    for child in home.iterdir() if home.exists() else []:
        try:
            if child.is_dir() and child.name.lower().startswith("onedrive"):
                roots.append(child)
        except Exception:
            continue

    for root in roots:
        try:
            matches = root.glob("**/pim/data/catalog.db")
        except Exception:
            continue
        for match in matches:
            try:
                key = str(match.resolve()) if match.exists() else str(match)
            except Exception:
                key = str(match)
            if key in seen:
                continue
            seen.add(key)
            results.append(match)
    return results


def _value_is_meaningful(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return True


def _parse_sortable_ts(value: object) -> tuple[int, str]:
    text = str(value or "").strip()
    if not text:
        return (0, "")
    return (1, text)


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


def _safe_count(conn: sqlite3.Connection, table_name: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    except Exception:
        return 0
    if not row:
        return 0
    try:
        return int(row[0] or 0)
    except Exception:
        return 0


def _db_state_summary(path: Path) -> dict[str, int] | None:
    if not path.exists() or not path.is_file():
        return None

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(path)
        tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if row and row[0]
        }
        if not tables:
            return {
                "products": 0,
                "product_attribute_values": 0,
                "client_channels": 0,
                "template_profiles": 0,
                "channel_mapping_rules": 0,
                "channel_attribute_requirements": 0,
                "uploaded_files": 0,
                "catalog_import_history": 0,
                "ai_connection_profiles": 0,
                "ozon_category_cache": 0,
                "ozon_attribute_cache": 0,
                "ozon_attribute_value_cache": 0,
                "detmir_category_cache": 0,
                "detmir_attribute_cache": 0,
                "detmir_attribute_value_cache": 0,
                "detmir_product_cache": 0,
                "supplier_profiles": 0,
                "product_registry_documents": 0,
                "score": 0,
            }

        summary = {
            "products": _safe_count(conn, "products") if "products" in tables else 0,
            "product_attribute_values": _safe_count(conn, "product_attribute_values") if "product_attribute_values" in tables else 0,
            "client_channels": _safe_count(conn, "client_channels") if "client_channels" in tables else 0,
            "template_profiles": _safe_count(conn, "template_profiles") if "template_profiles" in tables else 0,
            "channel_mapping_rules": _safe_count(conn, "channel_mapping_rules") if "channel_mapping_rules" in tables else 0,
            "channel_attribute_requirements": _safe_count(conn, "channel_attribute_requirements") if "channel_attribute_requirements" in tables else 0,
            "uploaded_files": _safe_count(conn, "uploaded_files") if "uploaded_files" in tables else 0,
            "catalog_import_history": _safe_count(conn, "catalog_import_history") if "catalog_import_history" in tables else 0,
            "ai_connection_profiles": _safe_count(conn, "ai_connection_profiles") if "ai_connection_profiles" in tables else 0,
            "ozon_category_cache": _safe_count(conn, "ozon_category_cache") if "ozon_category_cache" in tables else 0,
            "ozon_attribute_cache": _safe_count(conn, "ozon_attribute_cache") if "ozon_attribute_cache" in tables else 0,
            "ozon_attribute_value_cache": _safe_count(conn, "ozon_attribute_value_cache") if "ozon_attribute_value_cache" in tables else 0,
            "detmir_category_cache": _safe_count(conn, "detmir_category_cache") if "detmir_category_cache" in tables else 0,
            "detmir_attribute_cache": _safe_count(conn, "detmir_attribute_cache") if "detmir_attribute_cache" in tables else 0,
            "detmir_attribute_value_cache": _safe_count(conn, "detmir_attribute_value_cache") if "detmir_attribute_value_cache" in tables else 0,
            "detmir_product_cache": _safe_count(conn, "detmir_product_cache") if "detmir_product_cache" in tables else 0,
            "supplier_profiles": _safe_count(conn, "supplier_profiles") if "supplier_profiles" in tables else 0,
            "product_registry_documents": _safe_count(conn, "product_registry_documents") if "product_registry_documents" in tables else 0,
        }
        # Weight product/master data highest, but preserve Ozon cache and mappings too.
        summary["score"] = (
            summary["products"] * 1000
            + summary["product_attribute_values"] * 30
            + summary["client_channels"] * 60
            + summary["template_profiles"] * 200
            + summary["channel_mapping_rules"] * 120
            + summary["channel_attribute_requirements"] * 10
            + summary["uploaded_files"] * 30
            + summary["catalog_import_history"] * 45
            + summary["ai_connection_profiles"] * 25
            + summary["ozon_category_cache"] * 5
            + summary["ozon_attribute_cache"] * 2
            + summary["ozon_attribute_value_cache"]
            + summary["detmir_category_cache"] * 5
            + summary["detmir_attribute_cache"] * 2
            + summary["detmir_attribute_value_cache"]
            + summary["detmir_product_cache"] * 3
            + summary["supplier_profiles"] * 20
            + summary["product_registry_documents"] * 25
        )
        return summary
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def _seed_preferred_db_if_needed(preferred: Path, fallbacks: list[Path]) -> None:
    """
    If preferred DB is absent/empty, or clearly poorer than another known DB,
    copy the richest existing DB from fallback paths. This protects catalog,
    Ozon cache, and mappings when app path changes or an empty DB file was
    accidentally created earlier.
    """
    preferred_summary = _db_state_summary(preferred)
    preferred_score = int((preferred_summary or {}).get("score") or 0)

    source: Path | None = None
    source_score = preferred_score
    for candidate in fallbacks:
        if candidate.resolve() == preferred.resolve():
            continue
        candidate_summary = _db_state_summary(candidate)
        candidate_score = int((candidate_summary or {}).get("score") or 0)
        if candidate_score > source_score:
            source = candidate
            source_score = candidate_score
    if source is None or source_score <= 0:
        return
    if preferred.exists() and preferred.is_file() and preferred_score >= source_score:
        return

    try:
        preferred.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, preferred)
    except Exception:
        # Best effort only; normal candidate probing continues.
        return


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name = ?
        LIMIT 1
        """,
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _insert_row_with_common_columns(
    target_conn: sqlite3.Connection,
    table_name: str,
    row: dict[str, object],
) -> None:
    target_cols = _table_columns(target_conn, table_name)
    payload = {k: v for k, v in row.items() if k in target_cols and k != "id"}
    if not payload:
        return
    fields = list(payload.keys())
    placeholders = ", ".join(["?"] * len(fields))
    target_conn.execute(
        f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})",
        tuple(payload[field] for field in fields),
    )


def _merge_upsert_table(
    target_conn: sqlite3.Connection,
    source_conn: sqlite3.Connection,
    table_name: str,
    key_columns: list[str],
    prefer_latest_updated_at: bool = False,
) -> None:
    if not _table_exists(source_conn, table_name) or not _table_exists(target_conn, table_name):
        return

    source_rows = source_conn.execute(f"SELECT * FROM {table_name}").fetchall()
    target_cols = _table_columns(target_conn, table_name)
    for row_obj in source_rows:
        row = dict(row_obj)
        if any(col not in row for col in key_columns):
            continue
        key_values = [row.get(col) for col in key_columns]
        where_sql = " AND ".join([f"IFNULL({col}, '') = IFNULL(?, '')" for col in key_columns])
        existing = target_conn.execute(
            f"SELECT * FROM {table_name} WHERE {where_sql} LIMIT 1",
            tuple(key_values),
        ).fetchone()
        if not existing:
            payload = {k: v for k, v in row.items() if k in target_cols and k != "id"}
            if not payload:
                continue
            fields = list(payload.keys())
            placeholders = ", ".join(["?"] * len(fields))
            target_conn.execute(
                f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})",
                tuple(payload[field] for field in fields),
            )
            continue

        existing_dict = dict(existing)
        source_newer = False
        if prefer_latest_updated_at:
            source_newer = _parse_sortable_ts(row.get("updated_at")) > _parse_sortable_ts(existing_dict.get("updated_at"))
        updates: dict[str, object] = {}
        for col, value in row.items():
            if col not in target_cols or col == "id" or col in key_columns:
                continue
            current = existing_dict.get(col)
            if _value_is_meaningful(value) and not _value_is_meaningful(current):
                updates[col] = value
            elif prefer_latest_updated_at and source_newer and _value_is_meaningful(value):
                updates[col] = value
        if not updates:
            continue
        set_sql = ", ".join([f"{col} = ?" for col in updates.keys()])
        target_conn.execute(
            f"UPDATE {table_name} SET {set_sql} WHERE {where_sql}",
            tuple(updates.values()) + tuple(key_values),
        )


def _merge_products_table(target_conn: sqlite3.Connection, source_conn: sqlite3.Connection) -> dict[int, int]:
    product_id_map: dict[int, int] = {}
    if not _table_exists(source_conn, "products") or not _table_exists(target_conn, "products"):
        return product_id_map

    source_rows = source_conn.execute("SELECT * FROM products ORDER BY id").fetchall()
    target_cols = _table_columns(target_conn, "products")
    for row_obj in source_rows:
        row = dict(row_obj)
        article = str(row.get("article") or "").strip()
        if not article:
            continue
        existing = target_conn.execute(
            "SELECT * FROM products WHERE article = ? LIMIT 1",
            (article,),
        ).fetchone()
        if not existing:
            payload = {k: v for k, v in row.items() if k in target_cols and k != "id"}
            fields = list(payload.keys())
            placeholders = ", ".join(["?"] * len(fields))
            cur = target_conn.execute(
                f"INSERT INTO products ({', '.join(fields)}) VALUES ({placeholders})",
                tuple(payload[field] for field in fields),
            )
            product_id_map[int(row.get("id") or 0)] = int(cur.lastrowid)
            continue

        existing_dict = dict(existing)
        updates: dict[str, object] = {}
        source_newer = _parse_sortable_ts(row.get("updated_at")) > _parse_sortable_ts(existing_dict.get("updated_at"))
        for col, value in row.items():
            if col not in target_cols or col in {"id", "article"}:
                continue
            current = existing_dict.get(col)
            if _value_is_meaningful(value) and not _value_is_meaningful(current):
                updates[col] = value
            elif source_newer and _value_is_meaningful(value) and col in {
                "name",
                "brand",
                "barcode",
                "category",
                "base_category",
                "subcategory",
                "supplier_name",
                "supplier_url",
                "description",
                "image_url",
                "ozon_description_category_id",
                "ozon_type_id",
                "ozon_category_path",
                "ozon_category_confidence",
                "tnved_code",
                "import_batch_id",
            }:
                updates[col] = value
        if updates:
            set_sql = ", ".join([f"{col} = ?" for col in updates.keys()])
            target_conn.execute(
                f"UPDATE products SET {set_sql} WHERE article = ?",
                tuple(updates.values()) + (article,),
            )
        refreshed = target_conn.execute("SELECT id FROM products WHERE article = ? LIMIT 1", (article,)).fetchone()
        if refreshed:
            product_id_map[int(row.get("id") or 0)] = int(refreshed["id"])
    return product_id_map


def _merge_product_attribute_values(
    target_conn: sqlite3.Connection,
    source_conn: sqlite3.Connection,
    product_id_map: dict[int, int],
) -> None:
    if not _table_exists(source_conn, "product_attribute_values") or not _table_exists(target_conn, "product_attribute_values"):
        return
    source_rows = source_conn.execute("SELECT * FROM product_attribute_values ORDER BY id").fetchall()
    for row_obj in source_rows:
        row = dict(row_obj)
        source_product_id = int(row.get("product_id") or 0)
        target_product_id = product_id_map.get(source_product_id)
        if not target_product_id:
            continue
        key_params = (
            target_product_id,
            row.get("attribute_code"),
            row.get("channel_code"),
            row.get("locale"),
        )
        existing = target_conn.execute(
            """
            SELECT *
            FROM product_attribute_values
            WHERE product_id = ?
              AND attribute_code = ?
              AND IFNULL(channel_code, '') = IFNULL(?, '')
              AND IFNULL(locale, '') = IFNULL(?, '')
            LIMIT 1
            """,
            key_params,
        ).fetchone()
        if not existing:
            target_conn.execute(
                """
                INSERT INTO product_attribute_values (
                    product_id,
                    attribute_code,
                    value_text,
                    value_number,
                    value_boolean,
                    value_json,
                    locale,
                    channel_code,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_product_id,
                    row.get("attribute_code"),
                    row.get("value_text"),
                    row.get("value_number"),
                    row.get("value_boolean"),
                    row.get("value_json"),
                    row.get("locale"),
                    row.get("channel_code"),
                    row.get("created_at"),
                    row.get("updated_at"),
                ),
            )
            continue

        existing_dict = dict(existing)
        updates: dict[str, object] = {}
        for value_col in ("value_text", "value_number", "value_boolean", "value_json"):
            if _value_is_meaningful(row.get(value_col)) and not _value_is_meaningful(existing_dict.get(value_col)):
                updates[value_col] = row.get(value_col)
        if _parse_sortable_ts(row.get("updated_at")) > _parse_sortable_ts(existing_dict.get("updated_at")):
            for value_col in ("value_text", "value_number", "value_boolean", "value_json"):
                if _value_is_meaningful(row.get(value_col)):
                    updates[value_col] = row.get(value_col)
            if _value_is_meaningful(row.get("updated_at")):
                updates["updated_at"] = row.get("updated_at")
        if updates:
            set_sql = ", ".join([f"{col} = ?" for col in updates.keys()])
            target_conn.execute(
                f"""
                UPDATE product_attribute_values
                SET {set_sql}
                WHERE product_id = ?
                  AND attribute_code = ?
                  AND IFNULL(channel_code, '') = IFNULL(?, '')
                  AND IFNULL(locale, '') = IFNULL(?, '')
                """,
                tuple(updates.values()) + key_params,
            )


def _merge_template_profiles(target_conn: sqlite3.Connection, source_conn: sqlite3.Connection) -> None:
    if not _table_exists(source_conn, "template_profiles") or not _table_exists(target_conn, "template_profiles"):
        return
    profile_rows = source_conn.execute("SELECT * FROM template_profiles ORDER BY id").fetchall()
    for profile_row_obj in profile_rows:
        profile_row = dict(profile_row_obj)
        profile_name = str(profile_row.get("profile_name") or "").strip()
        channel_code = str(profile_row.get("channel_code") or "").strip()
        category_code = profile_row.get("category_code")
        if not profile_name or not channel_code:
            continue
        existing = target_conn.execute(
            """
            SELECT *
            FROM template_profiles
            WHERE profile_name = ?
              AND channel_code = ?
              AND IFNULL(category_code, '') = IFNULL(?, '')
            LIMIT 1
            """,
            (profile_name, channel_code, category_code),
        ).fetchone()
        if not existing:
            cur = target_conn.execute(
                """
                INSERT INTO template_profiles (
                    profile_name,
                    channel_code,
                    category_code,
                    file_name,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    profile_name,
                    channel_code,
                    category_code,
                    profile_row.get("file_name"),
                    profile_row.get("created_at"),
                    profile_row.get("updated_at"),
                ),
            )
            target_profile_id = int(cur.lastrowid)
        else:
            existing_dict = dict(existing)
            target_profile_id = int(existing_dict["id"])
            updates: dict[str, object] = {}
            if _value_is_meaningful(profile_row.get("file_name")) and not _value_is_meaningful(existing_dict.get("file_name")):
                updates["file_name"] = profile_row.get("file_name")
            if _parse_sortable_ts(profile_row.get("updated_at")) > _parse_sortable_ts(existing_dict.get("updated_at")):
                updates["updated_at"] = profile_row.get("updated_at")
            if updates:
                set_sql = ", ".join([f"{col} = ?" for col in updates.keys()])
                target_conn.execute(
                    f"UPDATE template_profiles SET {set_sql} WHERE id = ?",
                    tuple(updates.values()) + (target_profile_id,),
                )

        if not _table_exists(source_conn, "template_profile_columns") or not _table_exists(target_conn, "template_profile_columns"):
            continue
        source_columns = source_conn.execute(
            "SELECT * FROM template_profile_columns WHERE profile_id = ? ORDER BY sort_order, id",
            (int(profile_row.get("id") or 0),),
        ).fetchall()
        for col_row_obj in source_columns:
            col_row = dict(col_row_obj)
            existing_col = target_conn.execute(
                """
                SELECT *
                FROM template_profile_columns
                WHERE profile_id = ?
                  AND template_column = ?
                LIMIT 1
                """,
                (target_profile_id, col_row.get("template_column")),
            ).fetchone()
            if not existing_col:
                target_conn.execute(
                    """
                    INSERT INTO template_profile_columns (
                        profile_id,
                        template_column,
                        source_type,
                        source_name,
                        matched_by,
                        transform_rule,
                        sort_order,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target_profile_id,
                        col_row.get("template_column"),
                        col_row.get("source_type"),
                        col_row.get("source_name"),
                        col_row.get("matched_by"),
                        col_row.get("transform_rule"),
                        col_row.get("sort_order"),
                        col_row.get("created_at"),
                        col_row.get("updated_at"),
                    ),
                )
                continue
            existing_col_dict = dict(existing_col)
            updates: dict[str, object] = {}
            for key in ("source_type", "source_name", "matched_by", "transform_rule", "sort_order", "updated_at"):
                if _value_is_meaningful(col_row.get(key)) and (
                    not _value_is_meaningful(existing_col_dict.get(key))
                    or _parse_sortable_ts(col_row.get("updated_at")) > _parse_sortable_ts(existing_col_dict.get("updated_at"))
                ):
                    updates[key] = col_row.get(key)
            if updates:
                set_sql = ", ".join([f"{col} = ?" for col in updates.keys()])
                target_conn.execute(
                    f"UPDATE template_profile_columns SET {set_sql} WHERE id = ?",
                    tuple(updates.values()) + (int(existing_col_dict["id"]),),
                )


def _merge_ozon_caches(target_conn: sqlite3.Connection, source_conn: sqlite3.Connection) -> None:
    cache_specs = [
        (
            "ozon_category_cache",
            ["description_category_id", "type_id", "full_path"],
        ),
        (
            "ozon_attribute_cache",
            ["description_category_id", "type_id", "attribute_id"],
        ),
        (
            "ozon_attribute_value_cache",
            ["description_category_id", "type_id", "attribute_id", "value_id"],
        ),
        (
            "ozon_dictionary_overrides",
            ["description_category_id", "type_id", "attribute_id", "normalized_raw_value"],
        ),
        (
            "ozon_update_jobs",
            ["id"],
        ),
        (
            "ozon_update_job_items",
            ["job_id", "offer_id", "product_id"],
        ),
        (
            "ozon_catalog_mapping_memory",
            ["mapping_key"],
        ),
    ]
    for table_name, key_columns in cache_specs:
        _merge_upsert_table(
            target_conn=target_conn,
            source_conn=source_conn,
            table_name=table_name,
            key_columns=key_columns,
            prefer_latest_updated_at=True,
        )


def _merge_detmir_caches(target_conn: sqlite3.Connection, source_conn: sqlite3.Connection) -> None:
    cache_specs = [
        (
            "detmir_category_cache",
            ["category_id"],
        ),
        (
            "detmir_attribute_cache",
            ["category_id", "attribute_id", "is_variant_attribute"],
        ),
        (
            "detmir_attribute_value_cache",
            ["attribute_key", "value_key"],
        ),
        (
            "detmir_product_cache",
            ["product_id"],
        ),
    ]
    for table_name, key_columns in cache_specs:
        _merge_upsert_table(
            target_conn=target_conn,
            source_conn=source_conn,
            table_name=table_name,
            key_columns=key_columns,
            prefer_latest_updated_at=True,
        )


def _merge_product_linked_tables(
    target_conn: sqlite3.Connection,
    source_conn: sqlite3.Connection,
    product_id_map: dict[int, int],
) -> None:
    if _table_exists(source_conn, "product_registry_documents") and _table_exists(target_conn, "product_registry_documents"):
        rows = source_conn.execute("SELECT * FROM product_registry_documents ORDER BY id").fetchall()
        for row_obj in rows:
            row = dict(row_obj)
            target_product_id = product_id_map.get(int(row.get("product_id") or 0))
            if not target_product_id:
                continue
            existing = target_conn.execute(
                """
                SELECT *
                FROM product_registry_documents
                WHERE product_id = ?
                  AND IFNULL(doc_kind, '') = IFNULL(?, '')
                  AND IFNULL(doc_number, '') = IFNULL(?, '')
                LIMIT 1
                """,
                (
                    target_product_id,
                    row.get("doc_kind"),
                    row.get("doc_number"),
                ),
            ).fetchone()
            if not existing:
                target_conn.execute(
                    """
                    INSERT INTO product_registry_documents (
                        product_id,
                        doc_kind,
                        doc_number,
                        valid_from,
                        valid_to,
                        authority_name,
                        applicant_name,
                        tnved_code,
                        source_url,
                        pdf_url,
                        local_file_path,
                        raw_payload,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target_product_id,
                        row.get("doc_kind"),
                        row.get("doc_number"),
                        row.get("valid_from"),
                        row.get("valid_to"),
                        row.get("authority_name"),
                        row.get("applicant_name"),
                        row.get("tnved_code"),
                        row.get("source_url"),
                        row.get("pdf_url"),
                        row.get("local_file_path"),
                        row.get("raw_payload"),
                        row.get("created_at"),
                        row.get("updated_at"),
                    ),
                )


def _merge_db_into_preferred(preferred: Path, source: Path) -> None:
    if not source.exists() or not source.is_file():
        return
    if preferred.resolve() == source.resolve():
        return

    target_conn: sqlite3.Connection | None = None
    source_conn: sqlite3.Connection | None = None
    try:
        preferred.parent.mkdir(parents=True, exist_ok=True)
        target_conn = sqlite3.connect(preferred, timeout=30.0)
        target_conn.row_factory = sqlite3.Row
        init_db(target_conn)

        source_conn = sqlite3.connect(source, timeout=30.0)
        source_conn.row_factory = sqlite3.Row

        _merge_upsert_table(target_conn, source_conn, "attribute_definitions", ["code"], prefer_latest_updated_at=True)
        _merge_upsert_table(target_conn, source_conn, "client_channels", ["client_code"], prefer_latest_updated_at=True)
        _merge_upsert_table(target_conn, source_conn, "channel_profiles", ["channel_code"], prefer_latest_updated_at=True)
        _merge_upsert_table(target_conn, source_conn, "channel_attribute_requirements", ["channel_code", "category_code", "attribute_code"], prefer_latest_updated_at=True)
        _merge_upsert_table(target_conn, source_conn, "channel_mapping_rules", ["channel_code", "category_code", "target_field"], prefer_latest_updated_at=True)
        _merge_upsert_table(target_conn, source_conn, "system_settings", ["key"], prefer_latest_updated_at=True)
        _merge_upsert_table(target_conn, source_conn, "supplier_profiles", ["supplier_name"], prefer_latest_updated_at=True)
        _merge_upsert_table(target_conn, source_conn, "uploaded_files", ["storage_kind", "file_hash"], prefer_latest_updated_at=True)
        _merge_upsert_table(target_conn, source_conn, "catalog_import_history", ["batch_id"], prefer_latest_updated_at=True)
        _merge_upsert_table(target_conn, source_conn, "ai_connection_profiles", ["profile_name"], prefer_latest_updated_at=True)
        _merge_template_profiles(target_conn, source_conn)
        product_id_map = _merge_products_table(target_conn, source_conn)
        _merge_product_attribute_values(target_conn, source_conn, product_id_map)
        _merge_product_linked_tables(target_conn, source_conn, product_id_map)
        _merge_ozon_caches(target_conn, source_conn)
        _merge_detmir_caches(target_conn, source_conn)
        target_conn.commit()
    except Exception:
        if target_conn is not None:
            target_conn.rollback()
    finally:
        if source_conn is not None:
            source_conn.close()
        if target_conn is not None:
            target_conn.close()


def _merge_candidate_dbs(preferred: Path, fallbacks: list[Path]) -> None:
    for source in fallbacks:
        try:
            _merge_db_into_preferred(preferred, source)
        except Exception:
            continue


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
        _merge_candidate_dbs(candidates[0], candidates[1:])

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

    _dedupe_products_by_article_best_effort(conn)
    _ensure_products_article_unique_index_best_effort(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_base_category ON products(base_category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_normalized_name ON products(normalized_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_supplier_article ON products(supplier_article)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_internal_article ON products(internal_article)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_import_batch_id ON products(import_batch_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_ozon_category ON products(ozon_description_category_id, ozon_type_id)")


def _dedupe_products_by_article_best_effort(conn: sqlite3.Connection) -> None:
    """
    Older deployed databases may contain duplicate articles.
    This cleanup is useful, but it must never crash app startup.
    """
    try:
        dup_row = conn.execute(
            """
            SELECT article
            FROM products
            WHERE article IS NOT NULL
            GROUP BY article
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        ).fetchone()
        if not dup_row:
            return
        conn.execute(
            """
            DELETE FROM products
            WHERE article IS NOT NULL
              AND id NOT IN (
                  SELECT MAX(id)
                  FROM products
                  WHERE article IS NOT NULL
                  GROUP BY article
              )
            """
        )
    except sqlite3.OperationalError:
        # Do not block app startup if table is temporarily locked or legacy schema behaves oddly.
        return


def _ensure_products_article_unique_index_best_effort(conn: sqlite3.Connection) -> None:
    """
    Try to enforce uniqueness on article, but keep the app bootable even if
    a legacy database still contains duplicates or the table is locked.
    """
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_article_unique ON products(article)")
    except sqlite3.OperationalError:
        return


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


def _ensure_uploaded_files_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS uploaded_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            storage_kind TEXT NOT NULL,
            file_hash TEXT NOT NULL,
            original_file_name TEXT,
            stored_rel_path TEXT NOT NULL,
            channel_code TEXT,
            category_code TEXT,
            batch_id TEXT,
            metadata_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_uploaded_files_unique
        ON uploaded_files(storage_kind, file_hash)
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_uploaded_files_kind ON uploaded_files(storage_kind)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_uploaded_files_channel ON uploaded_files(channel_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_uploaded_files_batch ON uploaded_files(batch_id)")


def _ensure_catalog_import_history_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_import_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            uploaded_file_id INTEGER,
            original_file_name TEXT,
            supplier_name TEXT,
            supplier_url_template TEXT,
            selected_sheet TEXT,
            header_row INTEGER,
            imported_count INTEGER DEFAULT 0,
            created_count INTEGER DEFAULT 0,
            updated_count INTEGER DEFAULT 0,
            duplicates_count INTEGER DEFAULT 0,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_import_batch_unique ON catalog_import_history(batch_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_import_created ON catalog_import_history(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_import_supplier ON catalog_import_history(supplier_name)")


def _ensure_ai_connection_profiles_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_connection_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_name TEXT NOT NULL,
            provider TEXT NOT NULL,
            base_url TEXT,
            chat_model TEXT,
            image_model TEXT,
            api_key TEXT,
            use_env_api_key INTEGER DEFAULT 1,
            temperature REAL DEFAULT 0.3,
            max_tokens INTEGER DEFAULT 1800,
            image_size TEXT,
            openrouter_referer TEXT,
            openrouter_title TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_profiles_name_unique ON ai_connection_profiles(profile_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_profiles_provider ON ai_connection_profiles(provider)")


def _ensure_client_channels_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS client_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT NOT NULL,
            client_name TEXT,
            is_active INTEGER DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_client_channels_code_unique ON client_channels(client_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_client_channels_active ON client_channels(is_active)")


def _ensure_supplier_profiles_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS supplier_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_name TEXT NOT NULL,
            legal_entity_name TEXT,
            base_url TEXT,
            url_template TEXT,
            is_active INTEGER DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    existing_cols = _table_columns(conn, "supplier_profiles")
    if "legal_entity_name" not in existing_cols:
        try:
            conn.execute("ALTER TABLE supplier_profiles ADD COLUMN legal_entity_name TEXT")
        except Exception:
            pass
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_supplier_profiles_name ON supplier_profiles(supplier_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_supplier_profiles_active ON supplier_profiles(is_active)")


def _ensure_product_registry_documents_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_registry_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            doc_kind TEXT,
            doc_number TEXT,
            valid_from TEXT,
            valid_to TEXT,
            authority_name TEXT,
            applicant_name TEXT,
            tnved_code TEXT,
            source_url TEXT,
            pdf_url TEXT,
            local_file_path TEXT,
            raw_payload TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prd_product_id ON product_registry_documents(product_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prd_doc_number ON product_registry_documents(doc_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prd_kind ON product_registry_documents(doc_kind)")


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


def _ensure_detmir_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detmir_category_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL UNIQUE,
            name TEXT,
            full_path TEXT,
            parent_id INTEGER,
            level INTEGER,
            published INTEGER DEFAULT 0,
            product_type_name TEXT,
            dimension_type TEXT,
            is_dimensional INTEGER DEFAULT 0,
            is_non_dimensional INTEGER DEFAULT 0,
            children_count INTEGER DEFAULT 0,
            is_leaf INTEGER DEFAULT 0,
            updated_remote_at TEXT,
            attributes_count INTEGER DEFAULT 0,
            variant_attributes_count INTEGER DEFAULT 0,
            blocks_count INTEGER DEFAULT 0,
            site_name_data_json TEXT,
            raw_json TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_cat_parent ON detmir_category_cache(parent_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_cat_path ON detmir_category_cache(full_path)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_cat_leaf ON detmir_category_cache(is_leaf)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detmir_attribute_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            attribute_id INTEGER NOT NULL,
            attribute_key TEXT NOT NULL,
            attribute_name TEXT,
            vendor_description TEXT,
            data_type TEXT,
            is_required INTEGER DEFAULT 0,
            min_value REAL,
            max_value REAL,
            min_length INTEGER,
            max_length INTEGER,
            decimal_places INTEGER,
            could_be_negative INTEGER DEFAULT 0,
            regexp_json TEXT,
            restriction_type TEXT,
            restriction_keys_json TEXT,
            feature_type TEXT,
            available_for_union INTEGER DEFAULT 0,
            transitive INTEGER DEFAULT 0,
            auto_moderation INTEGER DEFAULT 0,
            is_variant_attribute INTEGER DEFAULT 0,
            block_names_json TEXT,
            visibility_rule_json TEXT,
            raw_json TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_detmir_attr_unique
        ON detmir_attribute_cache(category_id, attribute_id, is_variant_attribute)
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_attr_cat ON detmir_attribute_cache(category_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_attr_key ON detmir_attribute_cache(attribute_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_attr_required ON detmir_attribute_cache(is_required)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detmir_attribute_value_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            attribute_key TEXT NOT NULL,
            value_key TEXT NOT NULL,
            value_label TEXT,
            raw_json TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_detmir_attr_value_unique
        ON detmir_attribute_value_cache(attribute_key, value_key)
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_attr_value_attr ON detmir_attribute_value_cache(attribute_key)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detmir_product_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL UNIQUE,
            mastercard_id TEXT,
            sku_id TEXT,
            product_code TEXT,
            contract_number TEXT,
            category_id INTEGER,
            commission_category_code TEXT,
            commission_category_full_name TEXT,
            title TEXT,
            site_name TEXT,
            barcodes_json TEXT,
            attributes_json TEXT,
            sizes_json TEXT,
            prices_json TEXT,
            photos_json TEXT,
            photo_session_status TEXT,
            certificates_json TEXT,
            sales_scheme_json TEXT,
            status TEXT,
            rejection_info_json TEXT,
            archive INTEGER DEFAULT 0,
            blocked_json TEXT,
            fbo_stock_level INTEGER,
            fbs_stock_level INTEGER,
            reviews_count INTEGER,
            created_remote_at TEXT,
            updated_remote_at TEXT,
            marking INTEGER DEFAULT 0,
            raw_json TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_product_cat ON detmir_product_cache(category_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_product_status ON detmir_product_cache(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_product_code ON detmir_product_cache(product_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detmir_product_updated ON detmir_product_cache(updated_remote_at)")


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
            None,
            "https://technosite.ru/",
            "https://technosite.ru/search/?q={article_q}",
            "Базовый профиль поставщика для ассортимента велосипедов под Детский Мир.",
        ),
        (
            "Рокви",
            None,
            "https://velocitygroup.ru/?category=velo",
            "https://velocitygroup.ru/?category=velo&search={article_q}",
            "Профиль поставщика для ассортимента Rockbros / Moon / SKS.",
        ),
    ]
    for supplier_name, legal_entity_name, base_url, url_template, notes in defaults:
        conn.execute(
            """
            INSERT OR IGNORE INTO supplier_profiles (supplier_name, legal_entity_name, base_url, url_template, is_active, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (supplier_name, legal_entity_name, base_url, url_template, notes),
        )
        conn.execute(
            """
            UPDATE supplier_profiles
            SET legal_entity_name = COALESCE(NULLIF(legal_entity_name, ''), ?),
                base_url = COALESCE(NULLIF(base_url, ''), ?),
                url_template = COALESCE(NULLIF(url_template, ''), ?),
                notes = COALESCE(NULLIF(notes, ''), ?),
                updated_at = CURRENT_TIMESTAMP
            WHERE supplier_name = ?
            """,
            (legal_entity_name, base_url, url_template, notes, supplier_name),
        )


def init_db(conn: sqlite3.Connection) -> None:
    _ensure_products_table(conn)
    _ensure_duplicate_table(conn)
    _ensure_product_intake_table(conn)
    _ensure_category_defaults_table(conn)
    _ensure_attribute_tables(conn)
    _ensure_product_data_sources_table(conn)
    _ensure_system_settings_table(conn)
    _ensure_client_channels_table(conn)
    _ensure_template_profile_tables(conn)
    _ensure_uploaded_files_tables(conn)
    _ensure_catalog_import_history_table(conn)
    _ensure_ai_connection_profiles_table(conn)
    _ensure_supplier_profiles_table(conn)
    _ensure_product_registry_documents_table(conn)
    _ensure_ozon_tables(conn)
    _ensure_ozon_catalog_mapping_memory_table(conn)
    _ensure_detmir_tables(conn)
    _ensure_channel_tables(conn)

    _seed_category_defaults(conn)
    _seed_attribute_definitions(conn)
    _seed_channels(conn)
    _seed_supplier_profiles(conn)

    conn.commit()
