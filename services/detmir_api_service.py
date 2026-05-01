from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from datetime import datetime
from typing import Any

import httpx

from services.attribute_service import upsert_attribute_definition, upsert_channel_attribute_requirement, upsert_channel_mapping_rule


BASE_URL = "https://api.detmir.market"
DEFAULT_TIMEOUT = 30.0
DETMIR_LAST_SCHEMA_SYNC_KEY = "detmir_last_schema_sync_at"
DETMIR_LAST_PRODUCT_SYNC_KEY = "detmir_last_product_sync_at"

_SELECT_TYPES = {"SELECT", "SELECT_MULTIPLE", "EXTENDED_DICTIONARY"}
_LOCAL_TYPE_MAP = {
    "INTEGER": "number",
    "FLOAT": "number",
    "NUMBER": "number",
    "BOOLEAN": "boolean",
    "BOOL": "boolean",
    "JSON": "json",
}

# Shared fields should reuse the master-card whenever possible.
_DETMIR_SHARED_ATTRIBUTE_CODES: dict[str, str] = {
    "brand": "brand",
    "model": "model",
    "strana_proizvodstva": "country_of_origin",
    "pol": "gender",
    "material_igr_osn": "material",
    "cvet_f": "color",
    "equipment": "equipment",
}

_DETMIR_STANDARD_MAPPING_RULES: dict[str, tuple[str, str, str | None]] = {
    "brand": ("attribute", "brand", None),
    "vendor_code": ("column", "supplier_article", None),
    "packing_height": ("column", "package_height", None),
    "packing_width": ("column", "package_width", None),
    "package_length": ("column", "package_length", None),
    "package_weight": ("column", "gross_weight", None),
    "strana_proizvodstva": ("attribute", "country_of_origin", None),
    "pol": ("attribute", "gender", None),
    "material_igr_osn": ("attribute", "material", None),
    "cvet_f": ("attribute", "color", None),
    "model": ("attribute", "model", None),
    "description": ("column", "description", None),
    "tnved": ("column", "tnved_code", None),
}
_STOP_TOKENS = {
    "для",
    "и",
    "или",
    "товары",
    "товар",
    "прочее",
    "разное",
    "аксессуары",
}


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


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


def _get_setting(conn: sqlite3.Connection, key: str) -> str | None:
    _ensure_system_settings_table(conn)
    row = conn.execute("SELECT value FROM system_settings WHERE key = ? LIMIT 1", (str(key),)).fetchone()
    return str(row["value"]) if row and row["value"] is not None else None


def _set_setting(conn: sqlite3.Connection, key: str, value: str | None) -> None:
    _ensure_system_settings_table(conn)
    conn.execute(
        """
        INSERT INTO system_settings (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = CURRENT_TIMESTAMP
        """,
        (str(key), value),
    )


def _to_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def get_env_credentials() -> tuple[str | None, str | None]:
    client_id = os.getenv("DETMIR_CLIENT_ID") or os.getenv("DETMIR_CLIENTID") or None
    api_key = os.getenv("DETMIR_API_KEY") or os.getenv("DETMIR_APIKEY") or None
    return client_id, api_key


def load_detmir_settings(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "client_id": str(_get_setting(conn, "detmir.client_id") or "").strip(),
        "api_key": str(_get_setting(conn, "detmir.api_key") or "").strip(),
        "use_env_api_key": _to_bool(_get_setting(conn, "detmir.use_env_api_key"), default=True),
    }


def save_detmir_settings(conn: sqlite3.Connection, settings: dict[str, Any]) -> None:
    _set_setting(conn, "detmir.client_id", str(settings.get("client_id") or "").strip())
    _set_setting(conn, "detmir.api_key", str(settings.get("api_key") or "").strip())
    _set_setting(conn, "detmir.use_env_api_key", "1" if bool(settings.get("use_env_api_key", True)) else "0")
    conn.commit()


def resolve_credentials(
    conn: sqlite3.Connection | None = None,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
) -> tuple[str | None, str | None]:
    saved: dict[str, Any] = {}
    if conn is not None:
        try:
            saved = load_detmir_settings(conn)
        except Exception:
            saved = {}
    env_client_id, env_api_key = get_env_credentials()
    use_env = bool(saved.get("use_env_api_key", True)) if use_env_api_key is None else bool(use_env_api_key)
    resolved_client_id = str(client_id or saved.get("client_id") or env_client_id or "").strip() or None
    resolved_api_key = str(api_key or "").strip() or None
    if not resolved_api_key:
        if str(saved.get("api_key") or "").strip():
            resolved_api_key = str(saved.get("api_key")).strip()
        elif use_env and env_api_key:
            resolved_api_key = str(env_api_key).strip()
    return resolved_client_id, resolved_api_key


def is_configured(
    conn: sqlite3.Connection | None = None,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
) -> bool:
    resolved_client_id, resolved_api_key = resolve_credentials(
        conn=conn,
        client_id=client_id,
        api_key=api_key,
        use_env_api_key=use_env_api_key,
    )
    return bool(resolved_client_id and resolved_api_key)


def _headers(
    conn: sqlite3.Connection | None = None,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
) -> dict[str, str]:
    resolved_client_id, resolved_api_key = resolve_credentials(
        conn=conn,
        client_id=client_id,
        api_key=api_key,
        use_env_api_key=use_env_api_key,
    )
    if not resolved_client_id or not resolved_api_key:
        raise ValueError("Не заданы Detmir Client ID / API Key")
    return {
        "client-id": str(resolved_client_id),
        "api-key": str(resolved_api_key),
        "Content-Type": "application/json",
    }


def _request_json(
    method: str,
    path: str,
    *,
    conn: sqlite3.Connection | None = None,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
    payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    max_retries: int = 5,
    retry_backoff_seconds: float = 1.2,
) -> Any:
    attempts = max(1, int(max_retries))
    for attempt in range(1, attempts + 1):
        try:
            with httpx.Client(base_url=BASE_URL, timeout=DEFAULT_TIMEOUT) as client:
                response = client.request(
                    method=method,
                    url=path,
                    headers=_headers(conn=conn, client_id=client_id, api_key=api_key, use_env_api_key=use_env_api_key),
                    json=payload,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()
            if isinstance(data, dict) and isinstance(data.get("errors"), list) and data["errors"]:
                err_text = "; ".join(str(item.get("message") or item) for item in data["errors"])
                raise RuntimeError(err_text or "Detmir API error")
            return data
        except httpx.HTTPStatusError as e:
            status_code = int(e.response.status_code) if e.response is not None else 0
            retriable = status_code in (429, 500, 502, 503, 504)
            if (not retriable) or attempt >= attempts:
                raise
            time.sleep(float(retry_backoff_seconds) * attempt)
        except (httpx.TimeoutException, httpx.RequestError):
            if attempt >= attempts:
                raise
            time.sleep(float(retry_backoff_seconds) * attempt)
    raise RuntimeError("Не удалось выполнить запрос к Detmir API")


def _local_data_type(detmir_type: str | None) -> str:
    key = str(detmir_type or "").strip().upper()
    return _LOCAL_TYPE_MAP.get(key, "text")


def _detmir_category_code(category_id: int) -> str:
    return f"detmir:{int(category_id)}"


def _detmir_attribute_code(attribute_key: str, attribute_name: str | None = None) -> str:
    raw_key = str(attribute_key or "").strip()
    if raw_key in _DETMIR_SHARED_ATTRIBUTE_CODES:
        return _DETMIR_SHARED_ATTRIBUTE_CODES[raw_key]
    safe = raw_key.lower().strip()
    safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in safe)
    safe = "_".join(part for part in safe.split("_") if part)
    if not safe:
        fallback = str(attribute_name or "field").strip().lower()
        fallback = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in fallback)
        safe = "_".join(part for part in fallback.split("_") if part) or "field"
    return f"detmir_attr_{safe}"


def _normalize_text(value: object) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("ё", "е")
    text = re.sub(r"[^a-zа-я0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _tokenize_text(value: object) -> set[str]:
    normalized = _normalize_text(value)
    return {
        token
        for token in normalized.split(" ")
        if token and len(token) > 1 and token not in _STOP_TOKENS
    }


def _split_multi_values(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, (tuple, set)):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, (int, float, bool)):
        return [str(value).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass
    parts = re.split(r"[,\n;/]+", text)
    return [part.strip() for part in parts if part.strip()]


def _value_present(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value not in (0, 0.0)


def _flatten_category_tree(nodes: list[dict[str, Any]], parent_id: int | None = None, parent_path: str = "", level: int = 1) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for node in nodes or []:
        node_id = int(node.get("id") or 0)
        name = str(node.get("name") or "").strip()
        path = " / ".join(part for part in [parent_path, name] if part)
        children = node.get("children") or []
        product_type = node.get("productType") or {}
        rows.append(
            {
                "category_id": node_id,
                "name": name,
                "full_path": path,
                "parent_id": int(parent_id) if parent_id is not None else None,
                "level": int(level),
                "published": 1 if bool(node.get("published")) else 0,
                "product_type_name": str(product_type.get("name") or "").strip() or None,
                "dimension_type": str(product_type.get("dimensionType") or "").strip() or None,
                "is_dimensional": 1 if bool(product_type.get("dimensional")) else 0,
                "is_non_dimensional": 1 if bool(product_type.get("nonDimensional")) else 0,
                "children_count": len(children),
                "is_leaf": 0 if children else 1,
                "updated_remote_at": None,
                "attributes_count": 0,
                "variant_attributes_count": 0,
                "blocks_count": 0,
                "site_name_data_json": None,
                "raw_json": json.dumps(node, ensure_ascii=False),
            }
        )
        rows.extend(_flatten_category_tree(children, parent_id=node_id, parent_path=path, level=level + 1))
    return rows


def _upsert_detmir_category_row(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO detmir_category_cache (
            category_id,
            name,
            full_path,
            parent_id,
            level,
            published,
            product_type_name,
            dimension_type,
            is_dimensional,
            is_non_dimensional,
            children_count,
            is_leaf,
            updated_remote_at,
            attributes_count,
            variant_attributes_count,
            blocks_count,
            site_name_data_json,
            raw_json,
            fetched_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(category_id) DO UPDATE SET
            name = excluded.name,
            full_path = COALESCE(NULLIF(excluded.full_path, ''), detmir_category_cache.full_path),
            parent_id = excluded.parent_id,
            level = excluded.level,
            published = excluded.published,
            product_type_name = excluded.product_type_name,
            dimension_type = excluded.dimension_type,
            is_dimensional = excluded.is_dimensional,
            is_non_dimensional = excluded.is_non_dimensional,
            children_count = excluded.children_count,
            is_leaf = excluded.is_leaf,
            updated_remote_at = COALESCE(excluded.updated_remote_at, detmir_category_cache.updated_remote_at),
            attributes_count = excluded.attributes_count,
            variant_attributes_count = excluded.variant_attributes_count,
            blocks_count = excluded.blocks_count,
            site_name_data_json = COALESCE(excluded.site_name_data_json, detmir_category_cache.site_name_data_json),
            raw_json = COALESCE(excluded.raw_json, detmir_category_cache.raw_json),
            fetched_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            int(row.get("category_id") or 0),
            row.get("name"),
            row.get("full_path"),
            row.get("parent_id"),
            row.get("level"),
            int(bool(row.get("published"))),
            row.get("product_type_name"),
            row.get("dimension_type"),
            int(bool(row.get("is_dimensional"))),
            int(bool(row.get("is_non_dimensional"))),
            int(row.get("children_count") or 0),
            int(bool(row.get("is_leaf"))),
            row.get("updated_remote_at"),
            int(row.get("attributes_count") or 0),
            int(row.get("variant_attributes_count") or 0),
            int(row.get("blocks_count") or 0),
            row.get("site_name_data_json"),
            row.get("raw_json"),
        ),
    )


def check_connection(
    conn: sqlite3.Connection | None = None,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
) -> dict[str, Any]:
    try:
        data = _request_json(
            "GET",
            "/public/api/seller/v1/categories",
            conn=conn,
            client_id=client_id,
            api_key=api_key,
            use_env_api_key=use_env_api_key,
        )
        items = data if isinstance(data, list) else []
        return {"ok": True, "categories_root": len(items)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def sync_category_tree(
    conn: sqlite3.Connection,
    *,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
) -> dict[str, Any]:
    payload = _request_json(
        "GET",
        "/public/api/seller/v1/categories",
        conn=conn,
        client_id=client_id,
        api_key=api_key,
        use_env_api_key=use_env_api_key,
    )
    nodes = payload if isinstance(payload, list) else []
    rows = _flatten_category_tree(nodes)
    inserted = 0
    conn.execute("BEGIN")
    try:
        for row in rows:
            _upsert_detmir_category_row(conn, row)
            inserted += 1
        _set_setting(conn, DETMIR_LAST_SCHEMA_SYNC_KEY, _now_iso())
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"categories": inserted, "roots": len(nodes)}


def _iter_category_pages(
    conn: sqlite3.Connection | None,
    *,
    client_id: str | None,
    api_key: str | None,
    use_env_api_key: bool | None,
    category_ids: list[int] | None = None,
    limit: int = 100,
    max_pages: int | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    page_token: str | None = None
    page = 0
    while True:
        body: dict[str, Any] = {
            "limit": max(1, min(100, int(limit))),
        }
        if category_ids:
            body["categoryIds"] = [int(x) for x in category_ids]
        if page_token:
            body["pageToken"] = page_token
        data = _request_json(
            "POST",
            "/public/api/seller/v1/categories/search",
            conn=conn,
            client_id=client_id,
            api_key=api_key,
            use_env_api_key=use_env_api_key,
            payload=body,
        )
        categories = data.get("categories") or []
        results.extend(categories)
        page_token = data.get("nextPageToken")
        page += 1
        if not page_token:
            break
        if max_pages is not None and page >= int(max_pages):
            break
    return results


def sync_categories_with_attributes(
    conn: sqlite3.Connection,
    *,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
    category_ids: list[int] | None = None,
    limit: int = 100,
    max_pages: int | None = None,
) -> dict[str, Any]:
    categories = _iter_category_pages(
        conn,
        client_id=client_id,
        api_key=api_key,
        use_env_api_key=use_env_api_key,
        category_ids=category_ids,
        limit=limit,
        max_pages=max_pages,
    )
    synced_categories = 0
    synced_attributes = 0
    synced_variant_attributes = 0
    seen_category_ids: list[int] = []
    conn.execute("BEGIN")
    try:
        for cat in categories:
            category_id = int(cat.get("id") or 0)
            if category_id <= 0:
                continue
            seen_category_ids.append(category_id)
            product_type = cat.get("productType") or {}
            category_row = {
                "category_id": category_id,
                "name": str(cat.get("name") or "").strip(),
                "full_path": None,
                "parent_id": cat.get("parentId"),
                "level": int(cat.get("level") or 0),
                "published": 1 if bool(cat.get("published")) else 0,
                "product_type_name": str(product_type.get("name") or "").strip() or None,
                "dimension_type": str(product_type.get("dimensionType") or "").strip() or None,
                "is_dimensional": 1 if bool(product_type.get("dimensional")) else 0,
                "is_non_dimensional": 1 if bool(product_type.get("nonDimensional")) else 0,
                "children_count": 0,
                "is_leaf": 1,
                "updated_remote_at": cat.get("updated"),
                "attributes_count": len(cat.get("attributes") or []),
                "variant_attributes_count": len(cat.get("variantAttributes") or []),
                "blocks_count": len(cat.get("blocks") or []),
                "site_name_data_json": json.dumps(cat.get("siteNameData") or {}, ensure_ascii=False),
                "raw_json": json.dumps(cat, ensure_ascii=False),
            }
            _upsert_detmir_category_row(conn, category_row)
            conn.execute("DELETE FROM detmir_attribute_cache WHERE category_id = ?", (category_id,))
            block_name_map = {
                int(block.get("id") or 0): str(block.get("name") or "").strip()
                for block in (cat.get("blocks") or [])
                if block.get("id") is not None
            }
            block_memberships: dict[int, list[str]] = {}
            for block in cat.get("blocks") or []:
                block_name = str(block.get("name") or "").strip()
                for attr_id in block.get("attributeIds") or []:
                    try:
                        aid = int(attr_id)
                    except Exception:
                        continue
                    block_memberships.setdefault(aid, []).append(block_name)

            for attr in cat.get("attributes") or []:
                attribute_id = int(attr.get("id") or 0)
                synced_attributes += 1
                conn.execute(
                    """
                    INSERT INTO detmir_attribute_cache (
                        category_id,
                        attribute_id,
                        attribute_key,
                        attribute_name,
                        vendor_description,
                        data_type,
                        is_required,
                        min_value,
                        max_value,
                        min_length,
                        max_length,
                        decimal_places,
                        could_be_negative,
                        regexp_json,
                        restriction_type,
                        restriction_keys_json,
                        feature_type,
                        available_for_union,
                        transitive,
                        auto_moderation,
                        is_variant_attribute,
                        block_names_json,
                        visibility_rule_json,
                        raw_json,
                        fetched_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (
                        category_id,
                        attribute_id,
                        attr.get("key"),
                        attr.get("name"),
                        attr.get("vendorDescription"),
                        attr.get("type"),
                        1 if bool(attr.get("required")) else 0,
                        attr.get("min"),
                        attr.get("max"),
                        attr.get("minLength"),
                        attr.get("maxLength"),
                        attr.get("decimalPlaces"),
                        1 if bool(attr.get("couldBeNegative")) else 0,
                        json.dumps(attr.get("regexp") or [], ensure_ascii=False),
                        (attr.get("listRestrictionType") or {}).get("type") if isinstance(attr.get("listRestrictionType"), dict) else None,
                        json.dumps((attr.get("listRestrictionType") or {}).get("keys") or [], ensure_ascii=False),
                        attr.get("featureType"),
                        1 if bool(attr.get("availableForUnion")) else 0,
                        1 if bool(attr.get("transitive")) else 0,
                        1 if bool(attr.get("autoModeration")) else 0,
                        json.dumps(block_memberships.get(attribute_id) or [], ensure_ascii=False),
                        json.dumps(attr, ensure_ascii=False),
                    ),
                )
            for attr in cat.get("variantAttributes") or []:
                attribute_id = int(attr.get("id") or 0)
                synced_variant_attributes += 1
                conn.execute(
                    """
                    INSERT INTO detmir_attribute_cache (
                        category_id,
                        attribute_id,
                        attribute_key,
                        attribute_name,
                        vendor_description,
                        data_type,
                        is_required,
                        min_value,
                        max_value,
                        min_length,
                        max_length,
                        decimal_places,
                        could_be_negative,
                        regexp_json,
                        restriction_type,
                        restriction_keys_json,
                        feature_type,
                        available_for_union,
                        transitive,
                        auto_moderation,
                        is_variant_attribute,
                        block_names_json,
                        visibility_rule_json,
                        raw_json,
                        fetched_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (
                        category_id,
                        attribute_id,
                        attr.get("key"),
                        attr.get("name"),
                        attr.get("vendorDescription"),
                        attr.get("type"),
                        1 if bool(attr.get("required")) else 0,
                        attr.get("min"),
                        attr.get("max"),
                        attr.get("minLength"),
                        attr.get("maxLength"),
                        attr.get("decimalPlaces"),
                        1 if bool(attr.get("couldBeNegative")) else 0,
                        json.dumps(attr.get("regexp") or [], ensure_ascii=False),
                        (attr.get("listRestrictionType") or {}).get("type") if isinstance(attr.get("listRestrictionType"), dict) else None,
                        json.dumps((attr.get("listRestrictionType") or {}).get("keys") or [], ensure_ascii=False),
                        attr.get("featureType"),
                        1 if bool(attr.get("availableForUnion")) else 0,
                        1 if bool(attr.get("transitive")) else 0,
                        1 if bool(attr.get("autoModeration")) else 0,
                        json.dumps(block_memberships.get(attribute_id) or [], ensure_ascii=False),
                        json.dumps((attr.get("visibilityRule") or {}), ensure_ascii=False),
                        json.dumps(attr, ensure_ascii=False),
                    ),
                )
            synced_categories += 1
        _set_setting(conn, DETMIR_LAST_SCHEMA_SYNC_KEY, _now_iso())
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "categories": synced_categories,
        "attributes": synced_attributes,
        "variant_attributes": synced_variant_attributes,
        "category_ids": seen_category_ids,
    }


def list_cached_categories(
    conn: sqlite3.Connection,
    *,
    search: str | None = None,
    only_leaf: bool = False,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if search:
        term = f"%{str(search).strip().lower()}%"
        where.append("(LOWER(IFNULL(full_path, '')) LIKE ? OR LOWER(IFNULL(name, '')) LIKE ?)")
        params.extend([term, term])
    if only_leaf:
        where.append("IFNULL(is_leaf, 0) = 1")
    sql = "SELECT * FROM detmir_category_cache"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY COALESCE(full_path, name), category_id LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def list_cached_attributes(
    conn: sqlite3.Connection,
    *,
    category_id: int | None = None,
    required_only: bool = False,
    include_variant: bool = True,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if category_id is not None:
        where.append("category_id = ?")
        params.append(int(category_id))
    if required_only:
        where.append("IFNULL(is_required, 0) = 1")
    if not include_variant:
        where.append("IFNULL(is_variant_attribute, 0) = 0")
    sql = "SELECT * FROM detmir_attribute_cache"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY is_variant_attribute, is_required DESC, attribute_name, attribute_key LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def _sync_attribute_values_for_key(
    conn: sqlite3.Connection,
    *,
    attribute_key: str,
    client_id: str | None,
    api_key: str | None,
    use_env_api_key: bool | None,
    limit: int = 1000,
    max_pages: int | None = None,
) -> int:
    if not str(attribute_key or "").strip():
        return 0
    total = 0
    page_token: str | None = None
    page = 0
    conn.execute("DELETE FROM detmir_attribute_value_cache WHERE attribute_key = ?", (str(attribute_key).strip(),))
    while True:
        body: dict[str, Any] = {
            "limit": max(1, min(1000, int(limit))),
            "searchRequest": {"attributeKey": str(attribute_key).strip()},
        }
        if page_token:
            body["pageToken"] = page_token
        data = _request_json(
            "POST",
            "/public/api/seller/v1/categories/attributes/values/search",
            conn=conn,
            client_id=client_id,
            api_key=api_key,
            use_env_api_key=use_env_api_key,
            payload=body,
        )
        items = data.get("items") or []
        for item in items:
            conn.execute(
                """
                INSERT INTO detmir_attribute_value_cache (
                    attribute_key,
                    value_key,
                    value_label,
                    raw_json,
                    fetched_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(attribute_key, value_key) DO UPDATE SET
                    value_label = excluded.value_label,
                    raw_json = excluded.raw_json,
                    fetched_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    str(item.get("attributeKey") or attribute_key).strip(),
                    str(item.get("key") or "").strip(),
                    str(item.get("value") or "").strip(),
                    json.dumps(item, ensure_ascii=False),
                ),
            )
            total += 1
        page_token = data.get("nextPageToken")
        page += 1
        if not page_token:
            break
        if max_pages is not None and page >= int(max_pages):
            break
    return total


def sync_attribute_values(
    conn: sqlite3.Connection,
    *,
    attribute_key: str,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
    limit: int = 1000,
    max_pages: int | None = None,
) -> dict[str, Any]:
    conn.execute("BEGIN")
    try:
        total = _sync_attribute_values_for_key(
            conn,
            attribute_key=attribute_key,
            client_id=client_id,
            api_key=api_key,
            use_env_api_key=use_env_api_key,
            limit=limit,
            max_pages=max_pages,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"attribute_key": str(attribute_key).strip(), "values": total}


def sync_all_attribute_values(
    conn: sqlite3.Connection,
    *,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
    limit: int = 1000,
    max_pages_per_attribute: int | None = None,
    max_attributes: int | None = None,
    dictionary_only: bool = True,
) -> dict[str, Any]:
    where = "WHERE attribute_key IS NOT NULL AND TRIM(attribute_key) <> ''"
    if dictionary_only:
        where += " AND UPPER(IFNULL(data_type, '')) IN ('SELECT', 'SELECT_MULTIPLE', 'EXTENDED_DICTIONARY')"
    sql = f"""
        SELECT DISTINCT attribute_key
        FROM detmir_attribute_cache
        {where}
        ORDER BY attribute_key
    """
    rows = conn.execute(sql).fetchall()
    keys = [str(r["attribute_key"]).strip() for r in rows if str(r["attribute_key"] or "").strip()]
    if max_attributes is not None and int(max_attributes) > 0:
        keys = keys[: int(max_attributes)]
    synced_attributes = 0
    synced_values = 0
    errors: list[str] = []
    for key in keys:
        try:
            result = sync_attribute_values(
                conn,
                attribute_key=key,
                client_id=client_id,
                api_key=api_key,
                use_env_api_key=use_env_api_key,
                limit=limit,
                max_pages=max_pages_per_attribute,
            )
            synced_attributes += 1
            synced_values += int(result.get("values") or 0)
        except Exception as e:
            errors.append(f"{key}: {e}")
    return {
        "attributes": synced_attributes,
        "values": synced_values,
        "errors": errors[:200],
    }


def list_cached_attribute_values(
    conn: sqlite3.Connection,
    *,
    attribute_key: str | None = None,
    search: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if attribute_key:
        where.append("attribute_key = ?")
        params.append(str(attribute_key).strip())
    if search:
        term = f"%{str(search).strip().lower()}%"
        where.append("(LOWER(IFNULL(value_key, '')) LIKE ? OR LOWER(IFNULL(value_label, '')) LIKE ?)")
        params.extend([term, term])
    sql = "SELECT * FROM detmir_attribute_value_cache"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY attribute_key, value_label LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def sync_products(
    conn: sqlite3.Connection,
    *,
    client_id: str | None = None,
    api_key: str | None = None,
    use_env_api_key: bool | None = None,
    limit: int = 100,
    max_pages: int | None = None,
    product_ids: list[int] | None = None,
) -> dict[str, Any]:
    page_token: str | None = None
    page = 0
    total = 0
    conn.execute("BEGIN")
    try:
        while True:
            body: dict[str, Any] = {
                "limit": max(1, min(100, int(limit))),
                "searchRequest": {},
            }
            if product_ids:
                body["searchRequest"]["productIds"] = [int(x) for x in product_ids[:100]]
            if page_token:
                body["pageToken"] = page_token
            data = _request_json(
                "POST",
                "/public/api/seller/v1/products/search",
                conn=conn,
                client_id=client_id,
                api_key=api_key,
                use_env_api_key=use_env_api_key,
                payload=body,
            )
            items = data.get("items") or []
            for item in items:
                product_id = int(item.get("id") or 0)
                if product_id <= 0:
                    continue
                contract = item.get("contract") or {}
                category = item.get("category") or {}
                commission = item.get("commissionCategory") or {}
                conn.execute(
                    """
                    INSERT INTO detmir_product_cache (
                        product_id,
                        mastercard_id,
                        sku_id,
                        product_code,
                        contract_number,
                        category_id,
                        commission_category_code,
                        commission_category_full_name,
                        title,
                        site_name,
                        barcodes_json,
                        attributes_json,
                        sizes_json,
                        prices_json,
                        photos_json,
                        photo_session_status,
                        certificates_json,
                        sales_scheme_json,
                        status,
                        rejection_info_json,
                        archive,
                        blocked_json,
                        fbo_stock_level,
                        fbs_stock_level,
                        reviews_count,
                        created_remote_at,
                        updated_remote_at,
                        marking,
                        raw_json,
                        fetched_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(product_id) DO UPDATE SET
                        mastercard_id = excluded.mastercard_id,
                        sku_id = excluded.sku_id,
                        product_code = excluded.product_code,
                        contract_number = excluded.contract_number,
                        category_id = excluded.category_id,
                        commission_category_code = excluded.commission_category_code,
                        commission_category_full_name = excluded.commission_category_full_name,
                        title = excluded.title,
                        site_name = excluded.site_name,
                        barcodes_json = excluded.barcodes_json,
                        attributes_json = excluded.attributes_json,
                        sizes_json = excluded.sizes_json,
                        prices_json = excluded.prices_json,
                        photos_json = excluded.photos_json,
                        photo_session_status = excluded.photo_session_status,
                        certificates_json = excluded.certificates_json,
                        sales_scheme_json = excluded.sales_scheme_json,
                        status = excluded.status,
                        rejection_info_json = excluded.rejection_info_json,
                        archive = excluded.archive,
                        blocked_json = excluded.blocked_json,
                        fbo_stock_level = excluded.fbo_stock_level,
                        fbs_stock_level = excluded.fbs_stock_level,
                        reviews_count = excluded.reviews_count,
                        created_remote_at = excluded.created_remote_at,
                        updated_remote_at = excluded.updated_remote_at,
                        marking = excluded.marking,
                        raw_json = excluded.raw_json,
                        fetched_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        product_id,
                        item.get("mastercardId"),
                        item.get("skuId"),
                        item.get("productCode"),
                        contract.get("number"),
                        category.get("id"),
                        commission.get("code"),
                        commission.get("fullName"),
                        item.get("title"),
                        item.get("siteName"),
                        json.dumps(item.get("barcodes") or [], ensure_ascii=False),
                        json.dumps(item.get("attributes") or [], ensure_ascii=False),
                        json.dumps(item.get("sizes") or [], ensure_ascii=False),
                        json.dumps(item.get("prices") or {}, ensure_ascii=False),
                        json.dumps(item.get("photos") or [], ensure_ascii=False),
                        item.get("photoSessionStatus"),
                        json.dumps(item.get("certificates") or [], ensure_ascii=False),
                        json.dumps(item.get("salesScheme") or [], ensure_ascii=False),
                        item.get("status"),
                        json.dumps(item.get("rejectionInfo") or {}, ensure_ascii=False) if item.get("rejectionInfo") is not None else None,
                        1 if bool(item.get("archive")) else 0,
                        json.dumps(item.get("blocked") or {}, ensure_ascii=False) if item.get("blocked") is not None else None,
                        item.get("fboStockLevel"),
                        item.get("fbsStockLevel"),
                        item.get("reviewsCount"),
                        item.get("created"),
                        item.get("updated"),
                        1 if bool(item.get("marking")) else 0,
                        json.dumps(item, ensure_ascii=False),
                    ),
                )
                total += 1
            page_token = data.get("nextPageToken")
            page += 1
            if product_ids:
                break
            if not page_token:
                break
            if max_pages is not None and page >= int(max_pages):
                break
        _set_setting(conn, DETMIR_LAST_PRODUCT_SYNC_KEY, _now_iso())
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"products": total, "pages": page}


def list_cached_products(
    conn: sqlite3.Connection,
    *,
    search: str | None = None,
    status: str | None = None,
    category_id: int | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if search:
        term = f"%{str(search).strip().lower()}%"
        where.append(
            "("
            "LOWER(IFNULL(title, '')) LIKE ? OR "
            "LOWER(IFNULL(site_name, '')) LIKE ? OR "
            "LOWER(IFNULL(product_code, '')) LIKE ? OR "
            "LOWER(IFNULL(mastercard_id, '')) LIKE ?"
            ")"
        )
        params.extend([term, term, term, term])
    if status:
        where.append("status = ?")
        params.append(str(status).strip())
    if category_id is not None:
        where.append("category_id = ?")
        params.append(int(category_id))
    sql = "SELECT * FROM detmir_product_cache"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY COALESCE(updated_remote_at, created_remote_at) DESC, product_id DESC LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def get_cached_category(conn: sqlite3.Connection, category_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM detmir_category_cache WHERE category_id = ? LIMIT 1",
        (int(category_id),),
    ).fetchone()
    return dict(row) if row else None


def suggest_categories_for_product(
    conn: sqlite3.Connection,
    product_row: dict[str, Any],
    *,
    limit: int = 12,
    only_leaf: bool = True,
) -> list[dict[str, Any]]:
    rows = list_cached_categories(conn, only_leaf=bool(only_leaf), limit=5000)
    if not rows:
        return []
    product_name = str(product_row.get("name") or "").strip()
    hints = [
        str(product_row.get("ozon_category_path") or "").strip(),
        str(product_row.get("subcategory") or "").strip(),
        str(product_row.get("category") or "").strip(),
        str(product_row.get("base_category") or "").strip(),
        product_name,
    ]
    hint_tokens = set()
    for hint in hints:
        hint_tokens.update(_tokenize_text(hint))
    ozon_path_tokens = _tokenize_text(product_row.get("ozon_category_path"))
    category_tokens = _tokenize_text(" ".join(str(x or "") for x in [product_row.get("category"), product_row.get("base_category"), product_row.get("subcategory")]))
    ranked: list[dict[str, Any]] = []
    for row in rows:
        path = str(row.get("full_path") or row.get("name") or "").strip()
        path_tokens = _tokenize_text(path)
        if not path_tokens:
            continue
        overlap = len(hint_tokens & path_tokens)
        score = 0.0
        if overlap:
            score += overlap * 2.5
        if ozon_path_tokens:
            score += len(ozon_path_tokens & path_tokens) * 1.8
        if category_tokens:
            score += len(category_tokens & path_tokens) * 2.2
        name_norm = _normalize_text(product_name)
        path_norm = _normalize_text(path)
        if name_norm and path_norm and any(token in name_norm for token in path_tokens):
            score += 1.0
        if path_norm.endswith("велосипеды") and "велосипед" in hint_tokens:
            score += 1.5
        if path_norm.endswith("самокаты") and "самокат" in hint_tokens:
            score += 1.5
        if path_norm.endswith("беговелы") and "беговел" in hint_tokens:
            score += 1.5
        if score <= 0:
            continue
        ranked.append(
            {
                **row,
                "match_score": round(score, 2),
                "token_overlap": overlap,
            }
        )
    ranked.sort(
        key=lambda item: (
            -float(item.get("match_score") or 0.0),
            -int(item.get("token_overlap") or 0),
            str(item.get("full_path") or item.get("name") or ""),
        )
    )
    return ranked[: max(1, int(limit))]


def detect_best_category_for_product(
    conn: sqlite3.Connection,
    product_row: dict[str, Any],
    *,
    min_score: float = 2.5,
    limit: int = 12,
) -> dict[str, Any]:
    suggestions = suggest_categories_for_product(conn, product_row, limit=limit)
    if not suggestions:
        return {"ok": False, "message": "В кэше Detmir пока нет подходящих категорий."}
    best = suggestions[0]
    if float(best.get("match_score") or 0.0) < float(min_score):
        return {
            "ok": False,
            "message": "Автоматический матчинг Detmir-категории слишком неуверенный.",
            "suggestions": suggestions,
        }
    return {"ok": True, "category": best, "suggestions": suggestions}


def get_detmir_cache_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = {}
    for table_name in (
        "detmir_category_cache",
        "detmir_attribute_cache",
        "detmir_attribute_value_cache",
        "detmir_product_cache",
    ):
        try:
            rows[table_name] = int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] or 0)
        except Exception:
            rows[table_name] = 0
    rows["categories_with_attrs"] = int(
        conn.execute("SELECT COUNT(DISTINCT category_id) FROM detmir_attribute_cache").fetchone()[0] or 0
    )
    rows["required_attributes"] = int(
        conn.execute("SELECT COUNT(*) FROM detmir_attribute_cache WHERE IFNULL(is_required, 0) = 1").fetchone()[0] or 0
    )
    rows["dictionary_values_attributes"] = int(
        conn.execute("SELECT COUNT(DISTINCT attribute_key) FROM detmir_attribute_value_cache").fetchone()[0] or 0
    )
    rows["last_schema_sync_at"] = _get_setting(conn, DETMIR_LAST_SCHEMA_SYNC_KEY)
    rows["last_product_sync_at"] = _get_setting(conn, DETMIR_LAST_PRODUCT_SYNC_KEY)
    return rows


def _resolve_attribute_value_candidates(
    conn: sqlite3.Connection,
    *,
    attribute_key: str,
) -> list[dict[str, Any]]:
    rows = list_cached_attribute_values(conn, attribute_key=str(attribute_key).strip(), limit=100000)
    prepared: list[dict[str, Any]] = []
    for row in rows:
        prepared.append(
            {
                "value_key": str(row.get("value_key") or "").strip(),
                "value_label": str(row.get("value_label") or "").strip(),
                "label_norm": _normalize_text(row.get("value_label")),
                "key_norm": _normalize_text(row.get("value_key")),
            }
        )
    return prepared


def _resolve_dictionary_value(
    conn: sqlite3.Connection,
    *,
    attribute_key: str,
    raw_value: object,
    is_multi: bool,
) -> dict[str, Any]:
    parts = _split_multi_values(raw_value)
    if not parts:
        return {"ok": False, "values": [], "status": "missing", "raw_value": raw_value}
    candidates = _resolve_attribute_value_candidates(conn, attribute_key=str(attribute_key))
    if not candidates:
        return {"ok": False, "values": [], "status": "dictionary_cache_missing", "raw_value": raw_value}
    picked_keys: list[str] = []
    picked_labels: list[str] = []
    unresolved: list[str] = []
    for part in parts:
        part_norm = _normalize_text(part)
        matched = None
        for cand in candidates:
            if part_norm in {cand["label_norm"], cand["key_norm"]}:
                matched = cand
                break
        if matched is None:
            for cand in candidates:
                if part_norm and (part_norm in cand["label_norm"] or cand["label_norm"] in part_norm or part_norm in cand["key_norm"]):
                    matched = cand
                    break
        if matched is None:
            unresolved.append(part)
            continue
        picked_keys.append(matched["value_key"])
        picked_labels.append(matched["value_label"])
    if unresolved:
        return {
            "ok": False,
            "values": picked_keys if is_multi else (picked_keys[0] if picked_keys else None),
            "labels": picked_labels,
            "status": "dictionary_unmatched",
            "raw_value": raw_value,
            "unresolved": unresolved,
        }
    return {
        "ok": True,
        "values": picked_keys if is_multi else (picked_keys[0] if picked_keys else None),
        "labels": picked_labels,
        "status": "ok",
        "raw_value": raw_value,
    }


def analyze_product_detmir_readiness(
    conn: sqlite3.Connection,
    *,
    product_id: int,
    category_id: int | None = None,
) -> dict[str, Any]:
    from services.template_matching import build_product_value_map

    product_row = conn.execute("SELECT * FROM products WHERE id = ? LIMIT 1", (int(product_id),)).fetchone()
    if not product_row:
        return {"summary": {"status": "not_found", "readiness_pct": 0}, "rows": [], "payload": {}}
    product = dict(product_row)
    resolved_category_id = int(category_id or product.get("detmir_category_id") or 0)
    if resolved_category_id <= 0:
        return {
            "summary": {
                "status": "no_category",
                "readiness_pct": 0,
                "required_total": 0,
                "required_filled": 0,
                "blockers": 1,
                "warnings": 0,
                "photos_count": 0,
                "dictionary_unmatched": 0,
            },
            "rows": [],
            "payload": {},
        }
    category_code = _detmir_category_code(resolved_category_id)
    detmir_attrs = list_cached_attributes(conn, category_id=resolved_category_id, include_variant=True, limit=10000)
    if not detmir_attrs:
        return {
            "summary": {
                "status": "no_schema",
                "readiness_pct": 0,
                "required_total": 0,
                "required_filled": 0,
                "blockers": 1,
                "warnings": 0,
                "photos_count": 0,
                "dictionary_unmatched": 0,
            },
            "rows": [],
            "payload": {},
        }
    value_map = build_product_value_map(conn, int(product_id))
    media_gallery = value_map.get("media_gallery") or []
    if not isinstance(media_gallery, list):
        media_gallery = _split_multi_values(media_gallery)
    barcode = str(product.get("barcode") or value_map.get("barcode") or "").strip()
    vendor_product_id = (
        str(product.get("supplier_article") or "").strip()
        or str(product.get("internal_article") or "").strip()
        or str(product.get("article") or "").strip()
    )
    title = str(product.get("name") or value_map.get("name") or "").strip()

    rows: list[dict[str, Any]] = [
        {"target": "category_id", "required": 1, "status": "ok", "value": resolved_category_id, "notes": "Detmir category"},
        {"target": "barcode", "required": 1, "status": "ok" if barcode else "missing", "value": barcode or None, "notes": "Основа привязки фото и карточки"},
        {"target": "vendorProductId", "required": 1, "status": "ok" if vendor_product_id else "missing", "value": vendor_product_id or None, "notes": "Идентификатор товара поставщика"},
        {"target": "title", "required": 1, "status": "ok" if title else "missing", "value": title or None, "notes": "Название товара в Детском Мире"},
        {"target": "photos", "required": 1, "status": "ok" if media_gallery else "missing", "value": media_gallery, "notes": "Желательно минимум 3 фото"},
    ]
    payload_attributes: list[dict[str, Any]] = []
    required_total = 0
    required_filled = 0
    dictionary_unmatched = 0
    for attr in detmir_attrs:
        attribute_key = str(attr.get("attribute_key") or "").strip()
        local_code = _detmir_attribute_code(attribute_key, str(attr.get("attribute_name") or ""))
        raw_value = value_map.get(local_code)
        required = int(bool(attr.get("is_required")))
        status = "missing"
        resolved_value: Any = None
        notes = ""
        if _value_present(raw_value):
            attr_type = str(attr.get("data_type") or "").strip().upper()
            is_multi = attr_type == "SELECT_MULTIPLE"
            if attr_type in _SELECT_TYPES:
                resolved = _resolve_dictionary_value(
                    conn,
                    attribute_key=attribute_key,
                    raw_value=raw_value,
                    is_multi=is_multi,
                )
                status = str(resolved.get("status") or "missing")
                resolved_value = resolved.get("values")
                if status == "ok":
                    notes = ", ".join(resolved.get("labels") or [])
                elif status == "dictionary_unmatched":
                    dictionary_unmatched += 1
                    notes = f"Нужно сопоставить: {', '.join(resolved.get('unresolved') or [])}"
                elif status == "dictionary_cache_missing":
                    notes = "Нет кэша справочных значений"
            else:
                status = "ok"
                resolved_value = raw_value
        row = {
            "target": attribute_key,
            "attribute_code": local_code,
            "attribute_name": str(attr.get("attribute_name") or attribute_key),
            "required": required,
            "status": status,
            "value": raw_value,
            "resolved_value": resolved_value,
            "notes": notes,
            "data_type": str(attr.get("data_type") or ""),
            "is_variant_attribute": int(bool(attr.get("is_variant_attribute"))),
        }
        rows.append(row)
        if required:
            required_total += 1
            if status == "ok":
                required_filled += 1
        if status == "ok" and resolved_value not in (None, "", [], {}):
            payload_attributes.append({"key": attribute_key, "value": resolved_value})

    blockers = sum(1 for row in rows if int(row.get("required") or 0) == 1 and str(row.get("status")) != "ok")
    warnings = 0
    if len(media_gallery) < 3:
        warnings += 1
    photo_urls = [str(x).strip() for x in media_gallery if str(x).strip()]
    if any(not url.startswith(("http://", "https://")) for url in photo_urls):
        warnings += 1
    readiness_pct = round((required_filled / required_total) * 100) if required_total else 0
    payload = {
        "categoryId": resolved_category_id,
        "vendorProductId": vendor_product_id or None,
        "title": title or None,
        "barcodes": [barcode] if barcode else [],
        "photos": photo_urls,
        "attributes": payload_attributes,
    }
    return {
        "summary": {
            "status": "ok",
            "readiness_pct": int(readiness_pct),
            "required_total": int(required_total),
            "required_filled": int(required_filled),
            "blockers": int(blockers),
            "warnings": int(warnings),
            "photos_count": int(len(photo_urls)),
            "dictionary_unmatched": int(dictionary_unmatched),
        },
        "rows": rows,
        "payload": payload,
    }


def import_category_requirements_to_pim(
    conn: sqlite3.Connection,
    *,
    category_id: int,
    create_mapping_rules: bool = True,
) -> dict[str, Any]:
    rows = list_cached_attributes(conn, category_id=int(category_id), include_variant=True, limit=10000)
    if not rows:
        return {"imported": 0, "required": 0, "category_code": _detmir_category_code(int(category_id)), "mapping_saved": 0}
    category_code = _detmir_category_code(int(category_id))
    imported = 0
    required = 0
    mapping_saved = 0
    for row in rows:
        attribute_key = str(row.get("attribute_key") or "").strip()
        if not attribute_key:
            continue
        local_code = _detmir_attribute_code(attribute_key, str(row.get("attribute_name") or ""))
        data_type = _local_data_type(row.get("data_type"))
        description_parts = [
            f"Detmir key={attribute_key}",
            f"category_id={int(category_id)}",
            f"attribute_id={int(row.get('attribute_id') or 0)}",
            f"type={row.get('data_type') or '-'}",
        ]
        upsert_attribute_definition(
            conn=conn,
            code=local_code,
            name=str(row.get("attribute_name") or attribute_key),
            data_type=data_type,
            scope="master" if not local_code.startswith("detmir_attr_") else "channel",
            entity_type="product",
            is_required=1 if bool(row.get("is_required")) else 0,
            is_multi_value=1 if str(row.get("data_type") or "").strip().upper() in {"SELECT_MULTIPLE"} else 0,
            description="; ".join(description_parts),
        )
        upsert_channel_attribute_requirement(
            conn=conn,
            channel_code="detmir",
            category_code=category_code,
            attribute_code=local_code,
            is_required=1 if bool(row.get("is_required")) else 0,
            sort_order=100,
            notes=f"Detmir key={attribute_key}; data_type={row.get('data_type') or '-'}",
        )
        imported += 1
        if bool(row.get("is_required")):
            required += 1
        mapping_hint = _DETMIR_STANDARD_MAPPING_RULES.get(attribute_key)
        if create_mapping_rules and mapping_hint:
            source_type, source_name, transform_rule = mapping_hint
            upsert_channel_mapping_rule(
                conn=conn,
                channel_code="detmir",
                category_code=category_code,
                target_field=attribute_key,
                source_type=source_type,
                source_name=source_name,
                transform_rule=transform_rule,
                is_required=1 if bool(row.get("is_required")) else 0,
            )
            mapping_saved += 1
    return {
        "imported": imported,
        "required": required,
        "category_code": category_code,
        "mapping_saved": mapping_saved,
    }
