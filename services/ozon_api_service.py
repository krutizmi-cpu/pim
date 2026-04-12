from __future__ import annotations

import json
import os
import sqlite3
from difflib import SequenceMatcher
from typing import Any

import httpx

from services.attribute_service import list_channel_mapping_rules, set_product_attribute_value, upsert_channel_mapping_rule
from services.source_tracking import save_field_source
from services.template_matching import build_product_value_map
from services.transforms import apply_transform
from utils.text_normalizer import normalize_text


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



def sync_attribute_dictionary_values(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
    attribute_id: int,
    client_id: str | None = None,
    api_key: str | None = None,
    language: str = "DEFAULT",
    page_limit: int = 2000,
) -> dict[str, Any]:
    attr_row = conn.execute(
        """
        SELECT dictionary_id, name
        FROM ozon_attribute_cache
        WHERE description_category_id = ? AND type_id = ? AND attribute_id = ?
        LIMIT 1
        """,
        (int(description_category_id), int(type_id), int(attribute_id)),
    ).fetchone()
    dictionary_id = int(attr_row["dictionary_id"]) if attr_row and attr_row["dictionary_id"] is not None else None

    conn.execute(
        "DELETE FROM ozon_attribute_value_cache WHERE description_category_id = ? AND type_id = ? AND attribute_id = ?",
        (int(description_category_id), int(type_id), int(attribute_id)),
    )

    inserted = 0
    last_value_id = 0
    while True:
        payload = {
            "attribute_id": int(attribute_id),
            "description_category_id": int(description_category_id),
            "type_id": int(type_id),
            "language": language,
            "limit": int(page_limit),
        }
        if last_value_id:
            payload["last_value_id"] = int(last_value_id)

        response = _post("/v1/description-category/attribute/values", payload, client_id=client_id, api_key=api_key)
        result = response.get("result") or []
        for item in result:
            conn.execute(
                """
                INSERT INTO ozon_attribute_value_cache (
                    description_category_id,
                    type_id,
                    attribute_id,
                    dictionary_id,
                    value_id,
                    value,
                    info,
                    picture,
                    raw_json,
                    fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    int(description_category_id),
                    int(type_id),
                    int(attribute_id),
                    dictionary_id,
                    item.get("id"),
                    item.get("value"),
                    item.get("info"),
                    item.get("picture"),
                    json.dumps(item, ensure_ascii=False),
                ),
            )
            inserted += 1
            if item.get("id") is not None:
                last_value_id = int(item.get("id"))
        has_next = bool(response.get("has_next"))
        if not has_next or not result:
            break

    conn.commit()
    return {
        "inserted": inserted,
        "attribute_id": int(attribute_id),
        "dictionary_id": dictionary_id,
        "attribute_name": attr_row["name"] if attr_row else None,
    }



def list_cached_attribute_values(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
    attribute_id: int,
    search: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    params: list[Any] = [int(description_category_id), int(type_id), int(attribute_id)]
    sql = """
        SELECT *
        FROM ozon_attribute_value_cache
        WHERE description_category_id = ?
          AND type_id = ?
          AND attribute_id = ?
    """
    if search:
        sql += " AND lower(IFNULL(value, '')) LIKE ?"
        params.append(f"%{str(search).strip().lower()}%")
    sql += " ORDER BY value LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]



def sync_all_category_dictionary_values(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
    client_id: str | None = None,
    api_key: str | None = None,
    language: str = "DEFAULT",
) -> dict[str, Any]:
    attributes = [
        row for row in list_cached_attributes(
            conn,
            description_category_id=int(description_category_id),
            type_id=int(type_id),
            limit=5000,
        )
        if int(row.get("dictionary_id") or 0) > 0
    ]
    synced_attributes = 0
    synced_values = 0
    details: list[dict[str, Any]] = []
    for row in attributes:
        result = sync_attribute_dictionary_values(
            conn,
            description_category_id=int(description_category_id),
            type_id=int(type_id),
            attribute_id=int(row["attribute_id"]),
            client_id=client_id,
            api_key=api_key,
            language=language,
        )
        synced_attributes += 1
        synced_values += int(result.get("inserted") or 0)
        details.append(result)
    return {
        "synced_attributes": synced_attributes,
        "synced_values": synced_values,
        "details": details,
    }



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


MASTER_ATTRIBUTE_DEFAULTS = [
    {"code": "hashtags", "name": "Хештеги", "data_type": "text", "description": "Ключевые хештеги для маркетплейса"},
    {"code": "rich_content_json", "name": "Rich-контент JSON", "data_type": "json", "description": "Rich-контент в JSON для маркетплейсов"},
    {"code": "side_wheels_included", "name": "Боковые колеса в комплекте", "data_type": "boolean", "description": "Есть ли боковые колёса в комплекте"},
    {"code": "factory_pack_count", "name": "Количество заводских упаковок", "data_type": "number", "description": "Сколько заводских упаковок у товара"},
    {"code": "unit_count", "name": "Количество товара в УЕИ", "data_type": "number", "description": "Количество в условной единице измерения"},
    {"code": "min_wholesale_qty", "name": "Минимальное количество оптом", "data_type": "number", "description": "Минимальное количество для опта"},
    {"code": "package_type", "name": "Тип упаковки", "data_type": "text", "description": "Тип потребительской или транспортной упаковки"},
    {"code": "delivery_form", "name": "Форма поставки", "data_type": "text", "description": "Форма поставки товара"},
    {"code": "wheel_type", "name": "Вид колес", "data_type": "text", "description": "Тип колёс велосипеда"},
    {"code": "rim_type", "name": "Вид обода", "data_type": "text", "description": "Тип обода"},
    {"code": "wheel_count", "name": "Количество колес", "data_type": "number", "description": "Количество колёс"},
    {"code": "equipment", "name": "Комплектация", "data_type": "text", "description": "Комплектация товара"},
    {"code": "bike_suspension", "name": "Амортизация велосипеда", "data_type": "text", "description": "Тип амортизации велосипеда"},
    {"code": "bike_type", "name": "Вид велосипеда", "data_type": "text", "description": "Вид велосипеда"},
    {"code": "child_bike_type", "name": "Вид детского велосипеда", "data_type": "text", "description": "Подтип детского велосипеда"},
    {"code": "protection", "name": "Защита", "data_type": "text", "description": "Защитные элементы и защита"},
    {"code": "speed_count", "name": "Количество скоростей велосипеда", "data_type": "number", "description": "Количество скоростей"},
    {"code": "fork_type", "name": "Конструкция вилки", "data_type": "text", "description": "Конструкция вилки"},
    {"code": "max_load_kg", "name": "Макс. нагрузка, кг", "data_type": "number", "description": "Максимальная нагрузка в кг"},
    {"code": "drive_type", "name": "Привод велосипеда", "data_type": "text", "description": "Тип привода велосипеда"},
    {"code": "handlebar_adjustment", "name": "Регулировка руля", "data_type": "text", "description": "Регулировка руля"},
    {"code": "seat_adjustment", "name": "Регулировка сидения", "data_type": "text", "description": "Регулировка сидения"},
    {"code": "adjustments", "name": "Регулировки и настройки", "data_type": "text", "description": "Прочие регулировки и настройки"},
    {"code": "recommended_height_cm", "name": "Рекомендуемый рост, см", "data_type": "text", "description": "Рекомендуемый рост пользователя"},
    {"code": "steering_column_type", "name": "Конструкция рулевой колонки", "data_type": "text", "description": "Конструкция рулевой колонки"},
    {"code": "handlebar_type", "name": "Конструкция руля", "data_type": "text", "description": "Тип или конструкция руля"},
    {"code": "rear_brake", "name": "Задний тормоз", "data_type": "text", "description": "Тип заднего тормоза"},
    {"code": "front_brake", "name": "Передний тормоз", "data_type": "text", "description": "Тип переднего тормоза"},
    {"code": "bike_part_type", "name": "Вид запчасти, аксессуара для велосипеда", "data_type": "text", "description": "Тип велосипедной запчасти или аксессуара"},
    {"code": "brake_system_type", "name": "Вид тормозной системы", "data_type": "text", "description": "Тип тормозной системы"},
    {"code": "drive_component_type", "name": "Вид элемента велосипедного привода", "data_type": "text", "description": "Тип элемента привода"},
    {"code": "quantity_pcs", "name": "Количество, шт", "data_type": "number", "description": "Количество штук"},
    {"code": "tool_purpose", "name": "Назначение инструмента", "data_type": "text", "description": "Назначение инструмента"},
    {"code": "compatible_with", "name": "Подходит к", "data_type": "text", "description": "Совместимость товара"},
    {"code": "shelf_life_days", "name": "Срок годности в днях", "data_type": "number", "description": "Срок годности в днях"},
    {"code": "thickness_mm", "name": "Толщина, мм", "data_type": "number", "description": "Толщина в мм"},
    {"code": "teeth_count", "name": "Число зубьев", "data_type": "number", "description": "Количество зубьев"},
    {"code": "pdf_url", "name": "Документ PDF", "data_type": "text", "description": "Ссылка на PDF-документ"},
    {"code": "pdf_file_name", "name": "Название файла PDF", "data_type": "text", "description": "Название PDF файла"},
    {"code": "merge_similar_items", "name": "Объединить в похожие товары", "data_type": "boolean", "description": "Флаг объединения в похожие товары"},
    {"code": "ozon_video_title", "name": "Ozon Видео название", "data_type": "text", "description": "Название видео для Ozon"},
    {"code": "ozon_video_url", "name": "Ozon Видео ссылка", "data_type": "text", "description": "Ссылка на видео для Ozon"},
    {"code": "ozon_video_products", "name": "Ozon Видео товары", "data_type": "text", "description": "Связанные товары на видео"},
    {"code": "ozon_video_cover_url", "name": "Ozon Видеообложка ссылка", "data_type": "text", "description": "Ссылка на обложку видео"},
    {"code": "dimensions_mm", "name": "Размеры, мм", "data_type": "text", "description": "Размеры товара в мм"},
]


KNOWN_OZON_MAPPING_RULES = [
    (("бренд",), "column", "brand", None),
    (("название модели", "модель"), "attribute", "model", None),
    (("название",), "column", "name", None),
    (("аннотация", "описание"), "column", "description", None),
    (("штрихкод",), "column", "barcode", None),
    (("код продавца",), "column", "article", None),
    (("вес с упаковкой",), "column", "gross_weight", "kg_to_g"),
    (("вес товара", "вес в собранном состоянии"), "column", "weight", "kg_to_g"),
    (("длина упаковки",), "column", "package_length", "cm_to_mm"),
    (("ширина упаковки",), "column", "package_width", "cm_to_mm"),
    (("высота упаковки",), "column", "package_height", "cm_to_mm"),
    (("длина",), "column", "length", "cm_to_mm"),
    (("ширина",), "column", "width", "cm_to_mm"),
    (("высота",), "column", "height", "cm_to_mm"),
    (("цвет",), "attribute", "color", None),
    (("материал",), "attribute", "material", None),
    (("пол",), "attribute", "gender", None),
    (("возраст",), "attribute", "age_group", None),
    (("диаметр колес", "диаметр колёс"), "attribute", "wheel_diameter_inch", None),
    (("страна производства", "страна изготовитель"), "attribute", "country_of_origin", None),
    (("тн вэд",), "column", "tnved_code", None),
    (("фото", "изображ"), "column", "media_gallery", "first_image"),
    (("тип",), "column", "subcategory", None),
    (("гарантийный срок",), "attribute", "warranty_months", None),
    (("количество заводских упаковок",), "attribute", "factory_pack_count", None),
    (("количество товара в уеи",), "attribute", "unit_count", None),
    (("минимальное количество оптом",), "attribute", "min_wholesale_qty", None),
    (("упаковка",), "attribute", "package_type", None),
    (("форма поставки",), "attribute", "delivery_form", None),
    (("вид колес",), "attribute", "wheel_type", None),
    (("вид обода",), "attribute", "rim_type", None),
    (("количество колес",), "attribute", "wheel_count", None),
    (("комплектация",), "attribute", "equipment", None),
    (("амортизация велосипеда",), "attribute", "bike_suspension", None),
    (("вид детского велосипеда",), "attribute", "child_bike_type", None),
    (("вид велосипеда",), "attribute", "bike_type", None),
    (("вид запчасти",), "attribute", "bike_part_type", None),
    (("вид тормозной системы",), "attribute", "brake_system_type", None),
    (("вид элемента велосипедного привода",), "attribute", "drive_component_type", None),
    (("защита",), "attribute", "protection", None),
    (("количество скоростей",), "attribute", "speed_count", None),
    (("конструкция вилки",), "attribute", "fork_type", None),
    (("макс. нагрузка",), "attribute", "max_load_kg", None),
    (("привод велосипеда",), "attribute", "drive_type", None),
    (("размер рамы",), "attribute", "frame_size", None),
    (("регулировка руля",), "attribute", "handlebar_adjustment", None),
    (("регулировка сидения",), "attribute", "seat_adjustment", None),
    (("регулировки и настройки",), "attribute", "adjustments", None),
    (("рекомендуемый рост",), "attribute", "recommended_height_cm", None),
    (("конструкция рулевой колонки",), "attribute", "steering_column_type", None),
    (("конструкция руля",), "attribute", "handlebar_type", None),
    (("задний тормоз",), "attribute", "rear_brake", None),
    (("передний тормоз",), "attribute", "front_brake", None),
    (("количество, шт",), "attribute", "quantity_pcs", None),
    (("назначение инструмента",), "attribute", "tool_purpose", None),
    (("подходит к",), "attribute", "compatible_with", None),
    (("срок годности в днях",), "attribute", "shelf_life_days", None),
    (("толщина, мм",), "attribute", "thickness_mm", None),
    (("число зубьев",), "attribute", "teeth_count", None),
    (("боковые колеса",), "attribute", "side_wheels_included", None),
    (("rich контент",), "attribute", "rich_content_json", None),
    (("#хештеги", "хештеги"), "attribute", "hashtags", None),
    (("документ pdf",), "attribute", "pdf_url", None),
    (("название файла pdf",), "attribute", "pdf_file_name", None),
    (("объединить в похожие товары",), "attribute", "merge_similar_items", None),
    (("озон.видео: название",), "attribute", "ozon_video_title", None),
    (("озон.видео: ссылка",), "attribute", "ozon_video_url", None),
    (("озон.видео: товары на видео",), "attribute", "ozon_video_products", None),
    (("озон.видеообложка: ссылка",), "attribute", "ozon_video_cover_url", None),
    (("размеры, мм",), "attribute", "dimensions_mm", None),
]


def _normalize_name(value: str | None) -> str:
    return " ".join((value or "").strip().lower().replace("ё", "е").replace("_", " ").replace("-", " ").split())


def _to_scalar_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        merged = ", ".join(str(v).strip() for v in value if str(v).strip())
        return merged.strip()
    return str(value).strip()


def _best_token_for_search(text: str) -> str:
    tokens = [t for t in normalize_text(text).split() if len(t) >= 3]
    if not tokens:
        return ""
    tokens.sort(key=lambda x: len(x), reverse=True)
    return tokens[0]


def _normalize_override_key(value: Any) -> str:
    return normalize_text(_to_scalar_string(value))


def list_dictionary_overrides(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
    attribute_id: int | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    params: list[Any] = [int(description_category_id), int(type_id)]
    sql = """
        SELECT *
        FROM ozon_dictionary_overrides
        WHERE description_category_id = ?
          AND type_id = ?
    """
    if attribute_id is not None:
        sql += " AND attribute_id = ?"
        params.append(int(attribute_id))
    sql += " ORDER BY updated_at DESC, created_at DESC, id DESC LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def save_dictionary_override(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
    attribute_id: int,
    raw_value: Any,
    value_id: int,
    value: str | None = None,
    comment: str | None = None,
) -> dict[str, Any]:
    raw_text = _to_scalar_string(raw_value)
    normalized_raw = _normalize_override_key(raw_text)
    if not raw_text or not normalized_raw:
        raise ValueError("Пустое raw_value для dictionary override")

    resolved_value = value
    if resolved_value in (None, ""):
        ref = conn.execute(
            """
            SELECT value
            FROM ozon_attribute_value_cache
            WHERE description_category_id = ?
              AND type_id = ?
              AND attribute_id = ?
              AND value_id = ?
            LIMIT 1
            """,
            (int(description_category_id), int(type_id), int(attribute_id), int(value_id)),
        ).fetchone()
        resolved_value = ref["value"] if ref else None

    conn.execute(
        """
        INSERT INTO ozon_dictionary_overrides (
            description_category_id,
            type_id,
            attribute_id,
            raw_value,
            normalized_raw_value,
            value_id,
            value,
            comment,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(description_category_id, type_id, attribute_id, normalized_raw_value) DO UPDATE SET
            value_id = excluded.value_id,
            value = excluded.value,
            comment = excluded.comment,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            int(description_category_id),
            int(type_id),
            int(attribute_id),
            raw_text,
            normalized_raw,
            int(value_id),
            resolved_value,
            comment,
        ),
    )
    conn.commit()
    return {
        "description_category_id": int(description_category_id),
        "type_id": int(type_id),
        "attribute_id": int(attribute_id),
        "raw_value": raw_text,
        "normalized_raw_value": normalized_raw,
        "value_id": int(value_id),
        "value": resolved_value,
        "comment": comment,
    }


def _get_dictionary_override(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
    attribute_id: int,
    raw_value: Any,
) -> dict[str, Any] | None:
    normalized_raw = _normalize_override_key(raw_value)
    if not normalized_raw:
        return None
    row = conn.execute(
        """
        SELECT value_id, value
        FROM ozon_dictionary_overrides
        WHERE description_category_id = ?
          AND type_id = ?
          AND attribute_id = ?
          AND normalized_raw_value = ?
        LIMIT 1
        """,
        (int(description_category_id), int(type_id), int(attribute_id), normalized_raw),
    ).fetchone()
    if not row:
        return None
    return {
        "value_id": int(row["value_id"]),
        "value": row["value"],
        "score": 1.0,
        "matched_by": "override",
    }


def _find_dictionary_candidates(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
    attribute_id: int,
    raw_value: Any,
    top_n: int = 5,
) -> list[dict[str, Any]]:
    source_text = _to_scalar_string(raw_value)
    normalized_source = normalize_text(source_text)
    if not normalized_source:
        return []

    token = _best_token_for_search(source_text)
    params: list[Any] = [int(description_category_id), int(type_id), int(attribute_id)]
    sql = """
        SELECT value_id, value
        FROM ozon_attribute_value_cache
        WHERE description_category_id = ?
          AND type_id = ?
          AND attribute_id = ?
    """
    if token:
        sql += " AND lower(IFNULL(value, '')) LIKE ?"
        params.append(f"%{token.lower()}%")
    sql += " LIMIT 1200"

    candidates = conn.execute(sql, params).fetchall()
    if not candidates:
        return []

    scored: list[dict[str, Any]] = []
    for row in candidates:
        candidate_value = row["value"] or ""
        normalized_candidate = normalize_text(candidate_value)
        if not normalized_candidate:
            continue

        if normalized_candidate == normalized_source:
            score = 0.99
            mode = "normalized_exact"
        elif normalized_candidate in normalized_source or normalized_source in normalized_candidate:
            ratio = SequenceMatcher(None, normalized_source, normalized_candidate).ratio()
            score = max(0.90, ratio)
            mode = "contains"
        else:
            score = SequenceMatcher(None, normalized_source, normalized_candidate).ratio()
            mode = "fuzzy"

        scored.append(
            {
                "value_id": int(row["value_id"]),
                "value": row["value"],
                "score": round(float(score), 4),
                "matched_by": mode,
            }
        )

    scored.sort(key=lambda item: item["score"], reverse=True)
    return scored[: max(1, int(top_n))]


def _find_best_dictionary_value(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
    attribute_id: int,
    raw_value: Any,
    min_score: float = 0.78,
) -> dict[str, Any] | None:
    source_text = _to_scalar_string(raw_value)
    if not source_text:
        return None

    override = _get_dictionary_override(
        conn=conn,
        description_category_id=int(description_category_id),
        type_id=int(type_id),
        attribute_id=int(attribute_id),
        raw_value=source_text,
    )
    if override:
        return override

    exact = conn.execute(
        """
        SELECT value_id, value
        FROM ozon_attribute_value_cache
        WHERE description_category_id = ?
          AND type_id = ?
          AND attribute_id = ?
          AND lower(trim(IFNULL(value, ''))) = lower(trim(?))
        LIMIT 1
        """,
        (int(description_category_id), int(type_id), int(attribute_id), source_text),
    ).fetchone()
    if exact:
        return {
            "value_id": int(exact["value_id"]),
            "value": exact["value"],
            "score": 1.0,
            "matched_by": "exact",
        }

    candidates = _find_dictionary_candidates(
        conn=conn,
        description_category_id=int(description_category_id),
        type_id=int(type_id),
        attribute_id=int(attribute_id),
        raw_value=source_text,
        top_n=1,
    )
    if not candidates:
        return None

    best = candidates[0]
    if float(best.get("score") or 0) < float(min_score):
        return None

    return best



def ensure_ozon_master_attributes(conn: sqlite3.Connection) -> dict[str, Any]:
    inserted = 0
    for item in MASTER_ATTRIBUTE_DEFAULTS:
        existed = conn.execute("SELECT 1 FROM attribute_definitions WHERE code = ?", (item["code"],)).fetchone()
        conn.execute(
            """
            INSERT INTO attribute_definitions
            (code, name, data_type, scope, entity_type, is_required, is_multi_value, unit, description, created_at, updated_at)
            VALUES (?, ?, ?, 'master', 'product', 0, 0, NULL, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(code) DO UPDATE SET
                name = excluded.name,
                data_type = excluded.data_type,
                description = excluded.description,
                updated_at = CURRENT_TIMESTAMP
            """,
            (item["code"], item["name"], item["data_type"], item.get("description")),
        )
        if not existed:
            inserted += 1
    conn.commit()
    return {"inserted": inserted, "total": len(MASTER_ATTRIBUTE_DEFAULTS)}



def _product_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(products)").fetchall()
    return {str(r[1]) for r in rows} | {"media_gallery"}



def _source_exists(conn: sqlite3.Connection, source_type: str | None, source_name: str | None) -> bool:
    if not source_type or not source_name:
        return False
    if source_type == "column":
        return source_name in _product_columns(conn)
    if source_type == "attribute":
        row = conn.execute("SELECT 1 FROM attribute_definitions WHERE code = ?", (str(source_name),)).fetchone()
        return bool(row)
    return False



def suggest_mappings_for_cached_attributes(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
) -> list[dict[str, Any]]:
    ensure_ozon_master_attributes(conn)
    attributes = list_cached_attributes(conn, description_category_id=int(description_category_id), type_id=int(type_id), limit=5000)
    category_code = f"ozon:{int(description_category_id)}:{int(type_id)}"
    existing_rules = {
        row["target_field"]: row
        for row in list_channel_mapping_rules(conn, channel_code="ozon", category_code=category_code)
    }

    suggestions: list[dict[str, Any]] = []
    for item in attributes:
        target_field = f"ozon_attr_{int(item['attribute_id'])}"
        attr_name = item.get("name") or target_field
        normalized = _normalize_name(attr_name)
        source_type = None
        source_name = None
        transform_rule = None
        matched_by = None

        existing = existing_rules.get(target_field)
        if existing and _source_exists(conn, existing.get("source_type"), existing.get("source_name")):
            source_type = existing.get("source_type")
            source_name = existing.get("source_name")
            transform_rule = existing.get("transform_rule")
            matched_by = "saved_rule"
        else:
            for needles, candidate_source_type, candidate_source_name, candidate_transform in KNOWN_OZON_MAPPING_RULES:
                if any(needle in normalized for needle in needles) and _source_exists(conn, candidate_source_type, candidate_source_name):
                    source_type = candidate_source_type
                    source_name = candidate_source_name
                    transform_rule = candidate_transform
                    matched_by = "heuristic"
                    break

        suggestions.append(
            {
                "attribute_id": item.get("attribute_id"),
                "dictionary_id": item.get("dictionary_id"),
                "target_field": target_field,
                "name": attr_name,
                "group_name": item.get("group_name"),
                "is_required": int(bool(item.get("is_required"))),
                "source_type": source_type,
                "source_name": source_name,
                "transform_rule": transform_rule,
                "matched_by": matched_by,
                "status": "matched" if source_name else "unmatched",
            }
        )
    return suggestions



def save_suggested_mappings(
    conn: sqlite3.Connection,
    description_category_id: int,
    type_id: int,
) -> dict[str, Any]:
    category_code = f"ozon:{int(description_category_id)}:{int(type_id)}"
    suggestions = suggest_mappings_for_cached_attributes(conn, description_category_id, type_id)
    saved = 0
    for row in suggestions:
        if not row.get("source_name"):
            continue
        upsert_channel_mapping_rule(
            conn=conn,
            channel_code="ozon",
            category_code=category_code,
            target_field=row["target_field"],
            source_type=row["source_type"],
            source_name=row["source_name"],
            transform_rule=row.get("transform_rule"),
            is_required=int(bool(row.get("is_required"))),
        )
        saved += 1
    return {"saved": saved, "category_code": category_code}



def analyze_product_ozon_coverage(
    conn: sqlite3.Connection,
    product_id: int,
    description_category_id: int,
    type_id: int,
    dictionary_min_score: float = 0.78,
) -> dict[str, Any]:
    payload_rows = build_product_ozon_payload(
        conn,
        product_id=int(product_id),
        description_category_id=int(description_category_id),
        type_id=int(type_id),
        required_only=True,
        dictionary_min_score=float(dictionary_min_score),
    )
    total_required = int(len(payload_rows))
    covered_required = int(sum(1 for row in payload_rows if row.get("status") == "ready"))
    dictionary_unmatched_required = int(sum(1 for row in payload_rows if row.get("status") == "dictionary_unmatched"))
    rows: list[dict[str, Any]] = [
        {
            "Ozon атрибут": row.get("name"),
            "attribute_id": row.get("attribute_id"),
            "Источник": f"{row.get('source_type') or ''}:{row.get('source_name') or ''}".strip(':'),
            "Transform": row.get("transform_rule") or "",
            "Статус": row.get("status"),
            "Значение": row.get("value"),
            "dict_value_id": row.get("dictionary_value_id"),
            "dict_score": row.get("dictionary_match_score"),
        }
        for row in payload_rows
    ]
    readiness = round((covered_required / total_required) * 100) if total_required else 0
    return {
        "summary": {
            "required_total": total_required,
            "required_covered": covered_required,
            "required_missing": total_required - covered_required,
            "required_dictionary_unmatched": dictionary_unmatched_required,
            "readiness_pct": readiness,
        },
        "rows": rows,
    }



def build_product_ozon_payload(
    conn: sqlite3.Connection,
    product_id: int,
    description_category_id: int,
    type_id: int,
    required_only: bool = False,
    dictionary_min_score: float = 0.78,
) -> list[dict[str, Any]]:
    suggestions = suggest_mappings_for_cached_attributes(conn, description_category_id, type_id)
    value_map = build_product_value_map(conn, int(product_id))
    rows: list[dict[str, Any]] = []
    for row in suggestions:
        if required_only and not row.get("is_required"):
            continue
        if row.get("status") != "matched":
            continue
        source_name = row.get("source_name")
        value = apply_transform(value_map.get(source_name), row.get("transform_rule")) if source_name else None
        dictionary_id = int(row.get("dictionary_id") or 0)
        dict_match = None
        if dictionary_id > 0 and value not in (None, "", [], {}):
            dict_match = _find_best_dictionary_value(
                conn=conn,
                description_category_id=int(description_category_id),
                type_id=int(type_id),
                attribute_id=int(row.get("attribute_id")),
                raw_value=value,
                min_score=float(dictionary_min_score),
            )
            if dict_match:
                value = dict_match.get("value")

        if value in (None, "", [], {}):
            status = "empty"
        elif dictionary_id > 0 and not dict_match:
            status = "dictionary_unmatched"
        else:
            status = "ready"
        rows.append(
            {
                "target_field": row.get("target_field"),
                "attribute_id": row.get("attribute_id"),
                "dictionary_id": dictionary_id if dictionary_id > 0 else None,
                "name": row.get("name"),
                "is_required": int(bool(row.get("is_required"))),
                "source_type": row.get("source_type"),
                "source_name": source_name,
                "transform_rule": row.get("transform_rule"),
                "value": value,
                "status": status,
                "dictionary_value_id": dict_match.get("value_id") if dict_match else None,
                "dictionary_match_score": dict_match.get("score") if dict_match else None,
                "dictionary_match_by": dict_match.get("matched_by") if dict_match else None,
            }
        )
    return rows



def materialize_product_ozon_attributes(
    conn: sqlite3.Connection,
    product_id: int,
    description_category_id: int,
    type_id: int,
    required_only: bool = False,
    dictionary_min_score: float = 0.78,
) -> dict[str, Any]:
    import_cached_attributes_to_pim(conn, description_category_id, type_id)
    payload_rows = build_product_ozon_payload(
        conn,
        product_id,
        description_category_id,
        type_id,
        required_only=required_only,
        dictionary_min_score=float(dictionary_min_score),
    )
    category_code = f"ozon:{int(description_category_id)}:{int(type_id)}"
    applied = 0
    skipped_empty = 0
    skipped_dictionary = 0
    for row in payload_rows:
        value = row.get("value")
        if row.get("status") == "dictionary_unmatched":
            skipped_dictionary += 1
            continue
        if value in (None, "", [], {}):
            skipped_empty += 1
            continue
        set_product_attribute_value(
            conn,
            product_id=int(product_id),
            attribute_code=str(row["target_field"]),
            value=value,
            channel_code="ozon",
        )
        save_field_source(
            conn=conn,
                product_id=int(product_id),
                field_name=f"attr:{row['target_field']}",
                source_type="ozon_autofill",
                source_value_raw=json.dumps(
                    {
                        "value": value,
                        "dictionary_value_id": row.get("dictionary_value_id"),
                        "dictionary_match_score": row.get("dictionary_match_score"),
                        "dictionary_match_by": row.get("dictionary_match_by"),
                    },
                    ensure_ascii=False,
                ),
                source_url=category_code,
                confidence=0.85,
                is_manual=False,
            )
        applied += 1
    conn.commit()
    return {
        "applied": applied,
        "skipped_empty": skipped_empty,
        "skipped_dictionary": skipped_dictionary,
        "category_code": category_code,
        "rows": payload_rows,
    }


def preview_product_ozon_dictionary_gaps(
    conn: sqlite3.Connection,
    product_id: int,
    description_category_id: int,
    type_id: int,
    top_n: int = 3,
    dictionary_min_score: float = 0.78,
) -> list[dict[str, Any]]:
    payload_rows = build_product_ozon_payload(
        conn=conn,
        product_id=int(product_id),
        description_category_id=int(description_category_id),
        type_id=int(type_id),
        required_only=False,
        dictionary_min_score=float(dictionary_min_score),
    )
    gaps: list[dict[str, Any]] = []
    for row in payload_rows:
        if row.get("status") != "dictionary_unmatched":
            continue
        suggestions = _find_dictionary_candidates(
            conn=conn,
            description_category_id=int(description_category_id),
            type_id=int(type_id),
            attribute_id=int(row.get("attribute_id")),
            raw_value=row.get("value"),
            top_n=int(top_n),
        )
        gaps.append(
            {
                "attribute_id": row.get("attribute_id"),
                "name": row.get("name"),
                "source_name": row.get("source_name"),
                "raw_value": row.get("value"),
                "suggestions": suggestions,
                "suggestion_values": " | ".join(
                    [f"{item.get('value')} (id={item.get('value_id')}, s={item.get('score')})" for item in suggestions]
                ),
            }
        )
    return gaps


def build_product_ozon_api_attributes(
    conn: sqlite3.Connection,
    product_id: int,
    description_category_id: int,
    type_id: int,
    required_only: bool = False,
    dictionary_min_score: float = 0.78,
) -> dict[str, Any]:
    rows = build_product_ozon_payload(
        conn=conn,
        product_id=int(product_id),
        description_category_id=int(description_category_id),
        type_id=int(type_id),
        required_only=required_only,
        dictionary_min_score=float(dictionary_min_score),
    )
    attributes: list[dict[str, Any]] = []
    skipped = 0
    for row in rows:
        if row.get("status") != "ready":
            skipped += 1
            continue

        value = row.get("value")
        value_item: dict[str, Any]
        if row.get("dictionary_value_id") is not None:
            value_item = {"dictionary_value_id": int(row.get("dictionary_value_id"))}
        else:
            value_item = {"value": value}

        attributes.append(
            {
                "id": int(row.get("attribute_id")),
                "complex_id": 0,
                "values": [value_item],
            }
        )

    return {
        "product_id": int(product_id),
        "description_category_id": int(description_category_id),
        "type_id": int(type_id),
        "attributes": attributes,
        "included": int(len(attributes)),
        "skipped": int(skipped),
    }


def build_bulk_ozon_api_payloads(
    conn: sqlite3.Connection,
    product_ids: list[int],
    description_category_id: int,
    type_id: int,
    required_only: bool = False,
    dictionary_min_score: float = 0.78,
) -> dict[str, Any]:
    products_payload: list[dict[str, Any]] = []
    total_included = 0
    total_skipped = 0

    for pid in product_ids:
        item = build_product_ozon_api_attributes(
            conn=conn,
            product_id=int(pid),
            description_category_id=int(description_category_id),
            type_id=int(type_id),
            required_only=required_only,
            dictionary_min_score=float(dictionary_min_score),
        )
        products_payload.append(item)
        total_included += int(item.get("included") or 0)
        total_skipped += int(item.get("skipped") or 0)

    return {
        "description_category_id": int(description_category_id),
        "type_id": int(type_id),
        "products": products_payload,
        "summary": {
            "products_total": int(len(products_payload)),
            "attributes_included": int(total_included),
            "attributes_skipped": int(total_skipped),
        },
    }
