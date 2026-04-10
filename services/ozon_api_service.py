from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

import httpx


BASE_URL = "https://api-seller.ozon.ru"
DEFAULT_TIMEOUT = 30.0


def get_env_credentials() -> tuple[str | None, str | None]:
    client_id = os.getenv("OZON_CLIENT_ID") or os.getenv("OZON_CLIENTID") or None
    api_key = os.getenv("OZON_API_KEY") or os.getenv("OZON_APIKEY") or None
    return client_id, api_key


def is_configured(client_id: str | None = None, api_key: str | None = None) -> bool:
    env_client_id, env_api_key = get_env_credentials()
    return bool((client_id or env_client_id) and (api_key or env_api_key))


def _headers(client_id: str | None = None, api_key: str | None = None) -> dict[str, str]:
    env_client_id, env_api_key = get_env_credentials()
    resolved_client_id = client_id or env_client_id
    resolved_api_key = api_key or env_api_key
    if not resolved_client_id or not resolved_api_key:
        raise ValueError("Не заданы Ozon Client ID / API Key")
    return {
        "Client-Id": str(resolved_client_id),
        "Api-Key": str(resolved_api_key),
        "Content-Type": "application/json",
    }


def _post(path: str, payload: dict[str, Any], client_id: str | None = None, api_key: str | None = None) -> dict[str, Any]:
    with httpx.Client(base_url=BASE_URL, timeout=DEFAULT_TIMEOUT) as client:
        response = client.post(path, headers=_headers(client_id, api_key), json=payload)
        response.raise_for_status()
        data = response.json()
    if isinstance(data, dict) and data.get("message") and data.get("code"):
        raise RuntimeError(f"Ozon API error {data.get('code')}: {data.get('message')}")
    return data if isinstance(data, dict) else {"result": data}


def _flatten_tree(
    nodes: list[dict[str, Any]],
    parent_path: str = "",
    inherited_description_category_id: int | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for node in nodes or []:
        category_name = node.get("category_name") or ""
        current_path = f"{parent_path} / {category_name}".strip(" /") if category_name else parent_path
        effective_description_category_id = node.get("description_category_id") or inherited_description_category_id
        rows.append(
            {
                "description_category_id": effective_description_category_id,
                "category_name": category_name,
                "path": current_path,
                "type_id": node.get("type_id"),
                "type_name": node.get("type_name"),
                "disabled": int(bool(node.get("disabled"))),
                "children_count": len(node.get("children") or []),
                "raw_json": json.dumps(node, ensure_ascii=False),
            }
        )
        rows.extend(
            _flatten_tree(
                node.get("children") or [],
                current_path,
                effective_description_category_id,
            )
        )
    return rows


def sync_category_tree(
    conn: sqlite3.Connection,
    client_id: str | None = None,
    api_key: str | None = None,
    language: str = "DEFAULT",
) -> dict[str, Any]:
    payload = {"language": language}
    response = _post("/v1/description-category/tree", payload, client_id=client_id, api_key=api_key)
    rows = _flatten_tree(response.get("result") or [])

    conn.execute("DELETE FROM ozon_category_cache")
    inserted = 0
    for row in rows:
        conn.execute(
            """
            INSERT INTO ozon_category_cache (
                description_category_id,
                category_name,
                full_path,
                type_id,
                type_name,
                disabled,
                children_count,
                raw_json,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                row["description_category_id"],
                row["category_name"],
                row["path"],
                row["type_id"],
                row["type_name"],
                row["disabled"],
                row["children_count"],
                row["raw_json"],
            ),
        )
        inserted += 1
    conn.commit()
    return {"inserted": inserted, "total": inserted}


def list_cached_categories(conn: sqlite3.Connection, limit: int = 200) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM ozon_category_cache
        ORDER BY full_path, type_name
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def sync_category_attributes(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
    client_id: str | None = None,
    api_key: str | None = None,
    language: str = "DEFAULT",
) -> dict[str, Any]:
    payload = {
        "description_category_id": int(description_category_id),
        "type_id": int(type_id),
        "language": language,
    }
    response = _post("/v1/description-category/attribute", payload, client_id=client_id, api_key=api_key)
    attributes = response.get("result") or []

    conn.execute(
        "DELETE FROM ozon_attribute_cache WHERE description_category_id = ? AND type_id = ?",
        (int(description_category_id), int(type_id)),
    )

    inserted = 0
    required = 0
    for item in attributes:
        conn.execute(
            """
            INSERT INTO ozon_attribute_cache (
                description_category_id,
                type_id,
                attribute_id,
                name,
                description,
                type,
                group_id,
                group_name,
                dictionary_id,
                is_required,
                is_collection,
                max_value_count,
                category_dependent,
                raw_json,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                int(description_category_id),
                int(type_id),
                item.get("id"),
                item.get("name"),
                item.get("description"),
                item.get("type"),
                item.get("group_id"),
                item.get("group_name"),
                item.get("dictionary_id"),
                int(bool(item.get("is_required"))),
                int(bool(item.get("is_collection"))),
                item.get("max_value_count"),
                int(bool(item.get("category_dependent"))),
                json.dumps(item, ensure_ascii=False),
            ),
        )
        inserted += 1
        if item.get("is_required"):
            required += 1
    conn.commit()
    return {"inserted": inserted, "required": required, "total": inserted}


def list_cached_attributes(
    conn: sqlite3.Connection,
    description_category_id: int | None = None,
    type_id: int | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if description_category_id is not None:
        where.append("description_category_id = ?")
        params.append(int(description_category_id))
    if type_id is not None:
        where.append("type_id = ?")
        params.append(int(type_id))

    sql = "SELECT * FROM ozon_attribute_cache"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY is_required DESC, group_name, name LIMIT ?"
    params.append(int(limit))

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]



def _ozon_type_to_local(value: str | None) -> str:
    key = (value or "").strip().lower()
    if key in {"decimal", "integer", "number", "int"}:
        return "number"
    if key in {"bool", "boolean"}:
        return "boolean"
    if key in {"json", "richcontent", "rich-content"}:
        return "json"
    return "text"



def import_cached_attributes_to_pim(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
) -> dict[str, Any]:
    attributes = list_cached_attributes(
        conn,
        description_category_id=int(description_category_id),
        type_id=int(type_id),
        limit=5000,
    )
    if not attributes:
        return {"imported": 0, "required": 0, "category_code": f"ozon:{description_category_id}:{type_id}"}

    category_code = f"ozon:{int(description_category_id)}:{int(type_id)}"
    imported = 0
    required = 0

    for item in attributes:
        attribute_id = item.get("attribute_id")
        if attribute_id is None:
            continue
        code = f"ozon_attr_{int(attribute_id)}"
        data_type = _ozon_type_to_local(item.get("type"))
        description_parts = [
            f"Ozon attribute_id={attribute_id}",
            f"group={item.get('group_name') or '-'}",
            f"type={item.get('type') or '-'}",
        ]
        conn.execute(
            """
            INSERT INTO attribute_definitions
            (code, name, data_type, scope, entity_type, is_required, is_multi_value, unit, description, created_at, updated_at)
            VALUES (?, ?, ?, 'master', 'product', ?, ?, NULL, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(code) DO UPDATE SET
                name = excluded.name,
                data_type = excluded.data_type,
                is_required = excluded.is_required,
                is_multi_value = excluded.is_multi_value,
                description = excluded.description,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                code,
                item.get("name") or code,
                data_type,
                int(bool(item.get("is_required"))),
                int(bool(item.get("is_collection"))),
                "; ".join(description_parts),
            ),
        )
        conn.execute(
            """
            INSERT INTO channel_attribute_requirements
            (channel_code, category_code, attribute_code, is_required, sort_order, notes, created_at, updated_at)
            VALUES ('ozon', ?, ?, ?, 100, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(channel_code, IFNULL(category_code, ''), attribute_code) DO UPDATE SET
                is_required = excluded.is_required,
                notes = excluded.notes,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                category_code,
                code,
                int(bool(item.get("is_required"))),
                f"Импортировано из Ozon category {description_category_id}, type {type_id}",
            ),
        )
        imported += 1
        if item.get("is_required"):
            required += 1

    conn.commit()
    return {"imported": imported, "required": required, "category_code": category_code}
