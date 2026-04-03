from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def list_attribute_definitions(
    conn: sqlite3.Connection,
    scope: str | None = None,
) -> list[dict]:
    if scope:
        rows = conn.execute(
            """
            SELECT *
            FROM attribute_definitions
            WHERE scope = ?
            ORDER BY name
            """,
            (scope,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM attribute_definitions
            ORDER BY scope, name
            """
        ).fetchall()

    return [dict(r) for r in rows]


def upsert_attribute_definition(
    conn: sqlite3.Connection,
    code: str,
    name: str,
    data_type: str = "text",
    scope: str = "master",
    entity_type: str = "product",
    is_required: int = 0,
    is_multi_value: int = 0,
    unit: str | None = None,
    description: str | None = None,
) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO attribute_definitions
        (code, name, data_type, scope, entity_type, is_required, is_multi_value, unit, description, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            name = excluded.name,
            data_type = excluded.data_type,
            scope = excluded.scope,
            entity_type = excluded.entity_type,
            is_required = excluded.is_required,
            is_multi_value = excluded.is_multi_value,
            unit = excluded.unit,
            description = excluded.description,
            updated_at = excluded.updated_at
        """,
        (
            code,
            name,
            data_type,
            scope,
            entity_type,
            int(is_required),
            int(is_multi_value),
            unit,
            description,
            now,
            now,
        ),
    )
    conn.commit()


def _prepare_value_payload(data_type: str, value: Any) -> dict[str, Any]:
    payload = {
        "value_text": None,
        "value_number": None,
        "value_boolean": None,
        "value_json": None,
    }

    if value is None:
        return payload

    if data_type == "number":
        try:
            payload["value_number"] = float(value)
        except Exception:
            payload["value_text"] = str(value)

    elif data_type == "boolean":
        if isinstance(value, bool):
            payload["value_boolean"] = 1 if value else 0
        else:
            text = str(value).strip().lower()
            payload["value_boolean"] = 1 if text in ("1", "true", "yes", "да") else 0

    elif data_type == "json":
        payload["value_json"] = json.dumps(value, ensure_ascii=False)

    else:
        payload["value_text"] = str(value)

    return payload


def set_product_attribute_value(
    conn: sqlite3.Connection,
    product_id: int,
    attribute_code: str,
    value: Any,
    locale: str | None = None,
    channel_code: str | None = None,
) -> None:
    attr = conn.execute(
        """
        SELECT code, data_type
        FROM attribute_definitions
        WHERE code = ?
        """,
        (attribute_code,),
    ).fetchone()

    if not attr:
        raise ValueError(f"Attribute definition not found: {attribute_code}")

    prepared = _prepare_value_payload(attr["data_type"], value)
    now = _now()

    existing = conn.execute(
        """
        SELECT id
        FROM product_attribute_values
        WHERE product_id = ?
          AND attribute_code = ?
          AND COALESCE(channel_code, '') = COALESCE(?, '')
          AND COALESCE(locale, '') = COALESCE(?, '')
        """,
        (product_id, attribute_code, channel_code, locale),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE product_attribute_values
            SET value_text = ?,
                value_number = ?,
                value_boolean = ?,
                value_json = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                prepared["value_text"],
                prepared["value_number"],
                prepared["value_boolean"],
                prepared["value_json"],
                now,
                existing["id"],
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO product_attribute_values
            (
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
                product_id,
                attribute_code,
                prepared["value_text"],
                prepared["value_number"],
                prepared["value_boolean"],
                prepared["value_json"],
                locale,
                channel_code,
                now,
                now,
            ),
        )

    conn.commit()


def get_product_attribute_values(
    conn: sqlite3.Connection,
    product_id: int,
    channel_code: str | None = None,
) -> list[dict]:
    if channel_code:
        rows = conn.execute(
            """
            SELECT pav.*, ad.name, ad.data_type, ad.scope, ad.unit
            FROM product_attribute_values pav
            JOIN attribute_definitions ad
              ON ad.code = pav.attribute_code
            WHERE pav.product_id = ?
              AND (pav.channel_code = ? OR pav.channel_code IS NULL)
            ORDER BY ad.scope, ad.name
            """,
            (product_id, channel_code),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT pav.*, ad.name, ad.data_type, ad.scope, ad.unit
            FROM product_attribute_values pav
            JOIN attribute_definitions ad
              ON ad.code = pav.attribute_code
            WHERE pav.product_id = ?
            ORDER BY ad.scope, ad.name
            """,
            (product_id,),
        ).fetchall()

    result = []
    for row in rows:
        item = dict(row)

        value = None
        if item["value_number"] is not None:
            value = item["value_number"]
        elif item["value_boolean"] is not None:
            value = bool(item["value_boolean"])
        elif item["value_json"] is not None:
            try:
                value = json.loads(item["value_json"])
            except Exception:
                value = item["value_json"]
        else:
            value = item["value_text"]

        item["value"] = value
        result.append(item)

    return result


def delete_product_attribute_value(
    conn: sqlite3.Connection,
    product_id: int,
    attribute_code: str,
    locale: str | None = None,
    channel_code: str | None = None,
) -> None:
    conn.execute(
        """
        DELETE FROM product_attribute_values
        WHERE product_id = ?
          AND attribute_code = ?
          AND COALESCE(channel_code, '') = COALESCE(?, '')
          AND COALESCE(locale, '') = COALESCE(?, '')
        """,
        (product_id, attribute_code, channel_code, locale),
    )
    conn.commit()


def upsert_channel_attribute_requirement(
    conn: sqlite3.Connection,
    channel_code: str,
    attribute_code: str,
    category_code: str | None = None,
    is_required: int = 1,
    sort_order: int = 100,
    notes: str | None = None,
) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO channel_attribute_requirements
        (channel_code, category_code, attribute_code, is_required, sort_order, notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_code, COALESCE(category_code, ''), attribute_code) DO UPDATE SET
            is_required = excluded.is_required,
            sort_order = excluded.sort_order,
            notes = excluded.notes,
            updated_at = excluded.updated_at
        """,
        (channel_code, category_code, attribute_code, int(is_required), int(sort_order), notes, now, now),
    )
    conn.commit()


def list_channel_requirements(
    conn: sqlite3.Connection,
    channel_code: str,
    category_code: str | None = None,
) -> list[dict]:
    if category_code:
        rows = conn.execute(
            """
            SELECT car.*, ad.name, ad.data_type, ad.scope
            FROM channel_attribute_requirements car
            JOIN attribute_definitions ad
              ON ad.code = car.attribute_code
            WHERE car.channel_code = ?
              AND (car.category_code = ? OR car.category_code IS NULL)
            ORDER BY car.sort_order, ad.name
            """,
            (channel_code, category_code),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT car.*, ad.name, ad.data_type, ad.scope
            FROM channel_attribute_requirements car
            JOIN attribute_definitions ad
              ON ad.code = car.attribute_code
            WHERE car.channel_code = ?
            ORDER BY car.sort_order, ad.name
            """,
            (channel_code,),
        ).fetchall()

    return [dict(r) for r in rows]


def upsert_channel_mapping_rule(
    conn: sqlite3.Connection,
    channel_code: str,
    target_field: str,
    source_name: str,
    category_code: str | None = None,
    source_type: str = "attribute",
    transform_rule: str | None = None,
    is_required: int = 0,
) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO channel_mapping_rules
        (channel_code, category_code, target_field, source_type, source_name, transform_rule, is_required, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_code, COALESCE(category_code, ''), target_field) DO UPDATE SET
            source_type = excluded.source_type,
            source_name = excluded.source_name,
            transform_rule = excluded.transform_rule,
            is_required = excluded.is_required,
            updated_at = excluded.updated_at
        """,
        (
            channel_code,
            category_code,
            target_field,
            source_type,
            source_name,
            transform_rule,
            int(is_required),
            now,
            now,
        ),
    )
    conn.commit()


def list_channel_mapping_rules(
    conn: sqlite3.Connection,
    channel_code: str,
    category_code: str | None = None,
) -> list[dict]:
    if category_code:
        rows = conn.execute(
            """
            SELECT *
            FROM channel_mapping_rules
            WHERE channel_code = ?
              AND (category_code = ? OR category_code IS NULL)
            ORDER BY target_field
            """,
            (channel_code, category_code),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM channel_mapping_rules
            WHERE channel_code = ?
            ORDER BY target_field
            """,
            (channel_code,),
        ).fetchall()

    return [dict(r) for r in rows]
