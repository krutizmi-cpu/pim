from __future__ import annotations

import json
import hashlib
from io import BytesIO
import math
import re
from pathlib import Path
import threading
from datetime import datetime, timezone
from urllib.parse import quote, urlparse

import pandas as pd
import sqlite3
import streamlit as st
from openpyxl import load_workbook

from db import get_connection, init_db
try:
    from db import get_active_db_path as _get_active_db_path
except Exception:
    def _get_active_db_path():
        return None
from services.attribute_service import (
    get_product_attribute_values,
    list_attribute_definitions,
    list_channel_mapping_rules,
    list_channel_requirements,
    set_product_attribute_value,
    upsert_attribute_definition,
    upsert_channel_attribute_requirement,
    upsert_channel_mapping_rule,
)
from services.catalog_service import import_catalog_from_excel
from services.duplicate_service import refresh_duplicates_for_product
from services.source_tracking import get_field_sources, save_field_source, get_latest_field_source, field_is_manual
from services.source_priority import can_overwrite_field
try:
    from services import supplier_parser as _supplier_parser
except Exception:
    _supplier_parser = None


def parse_supplier_product_page(url: str, hints: list[str] | None = None, timeout: float = 8.0, max_hops: int = 1) -> dict:
    if _supplier_parser is not None and hasattr(_supplier_parser, "parse_supplier_product_page"):
        return _supplier_parser.parse_supplier_product_page(url, hints=hints, timeout=timeout, max_hops=max_hops)
    if _supplier_parser is not None and all(hasattr(_supplier_parser, x) for x in ("fetch_supplier_page", "extract_supplier_data", "normalize_supplier_data")):
        html = _supplier_parser.fetch_supplier_page(url, timeout=timeout)
        raw = _supplier_parser.extract_supplier_data(html, url)
        parsed = _supplier_parser.normalize_supplier_data(raw)
        parsed["resolved_url"] = url
        parsed["resolved_from_listing"] = False
        return parsed
    raise RuntimeError("Supplier parser module is unavailable")


def has_meaningful_supplier_data(parsed: dict) -> bool:
    if _supplier_parser is not None and hasattr(_supplier_parser, "has_meaningful_supplier_data"):
        return bool(_supplier_parser.has_meaningful_supplier_data(parsed))
    if not parsed:
        return False
    for key in ("description", "image_url", "weight", "length", "width", "height", "gross_weight"):
        if parsed.get(key) not in (None, "", 0, 0.0):
            return True
    attrs = parsed.get("attributes") or {}
    return bool(attrs)


def fallback_search_product_data(
    query: str,
    timeout: float = 8.0,
    max_results: int = 3,
    hints: list[str] | None = None,
    preferred_domain: str | None = None,
) -> dict:
    if _supplier_parser is not None and hasattr(_supplier_parser, "fallback_search_product_data"):
        return _supplier_parser.fallback_search_product_data(
            query,
            timeout=timeout,
            max_results=max_results,
            hints=hints,
            preferred_domain=preferred_domain,
        )
    return {}
from services.template_matching import auto_match_template_columns, apply_saved_mapping_rules, fill_template_dataframe, apply_client_validated_values, fill_template_workbook_bytes, dataframe_to_excel_bytes, detect_template_data_start_row, sanitize_template_xlsx_bytes
from services.template_profiles import save_template_profile, list_template_profiles, get_template_profile_columns
from services.readiness_service import analyze_template_readiness
from services.supplier_profiles import list_supplier_profiles, upsert_supplier_profile, ensure_default_supplier_profiles
from services.ozon_api_service import is_configured, sync_category_tree, list_cached_categories, list_cached_category_pairs, get_ozon_cache_stats, get_ozon_sync_coverage, sync_missing_category_attributes, sync_category_attributes, list_cached_attributes, sync_attribute_dictionary_values, sync_all_category_dictionary_values, list_cached_attribute_values, import_cached_attributes_to_pim, import_all_cached_attributes_to_pim, suggest_mappings_for_cached_attributes, save_suggested_mappings, analyze_product_ozon_coverage, ensure_ozon_master_attributes, build_product_ozon_payload, materialize_product_ozon_attributes, preview_product_ozon_dictionary_gaps, build_product_ozon_api_attributes, build_bulk_ozon_api_payloads, build_ozon_attributes_update_request, submit_ozon_attributes_update, list_ozon_update_jobs, get_ozon_update_job, retry_ozon_update_job, list_ozon_update_job_items, save_dictionary_override, list_dictionary_overrides, delete_dictionary_override, sync_all_categories_and_attributes
from services.ozon_category_match import bulk_assign_ozon_categories
from services.dimension_fallback import infer_category_fields, infer_dimensions_from_catalog, infer_dimensions_from_category_defaults, is_dimension_payload_suspicious

st.set_page_config(page_title="PIM", page_icon="📦", layout="wide")
OZON_OFFER_ID_OPTIONS = ["article", "internal_article", "supplier_article"]
TEMPLATE_TRANSFORM_OPTIONS = [
    "",
    "cm_to_mm",
    "mm_to_cm",
    "m_to_cm",
    "m_to_mm",
    "cm_to_m",
    "mm_to_m",
    "kg_to_g",
    "g_to_kg",
    "kg_to_lb",
    "lb_to_kg",
    "inch_to_cm",
    "cm_to_inch",
    "lower",
    "upper",
    "strip",
    "first_image",
    "join_images",
    "join_images_semicolon",
    "image_1",
    "image_2",
    "image_3",
    "image_4",
    "image_5",
]
OZON_CATEGORY_MIN_SCORE = 0.42
_OZON_SYNC_BG_LOCK = threading.Lock()
_OZON_SYNC_BG_THREAD: threading.Thread | None = None
_OZON_SYNC_BG_STATE: dict[str, object] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "last_error": None,
    "result": None,
}


def get_db():
    conn = get_connection()
    init_db(conn)
    ensure_default_supplier_profiles(conn)
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _set_ozon_bg_state(**kwargs) -> None:
    with _OZON_SYNC_BG_LOCK:
        _OZON_SYNC_BG_STATE.update(kwargs)


def _get_ozon_bg_state() -> dict:
    with _OZON_SYNC_BG_LOCK:
        state = dict(_OZON_SYNC_BG_STATE)
    state["thread_alive"] = bool(_OZON_SYNC_BG_THREAD and _OZON_SYNC_BG_THREAD.is_alive())
    return state


def _ozon_bg_worker(db_path: str, client_id: str, api_key: str) -> None:
    conn = None
    try:
        conn = get_connection(Path(db_path))
        init_db(conn)
        result = sync_all_categories_and_attributes(
            conn,
            client_id=client_id or None,
            api_key=api_key or None,
            max_pairs=None,
            import_to_pim=True,
            only_leaf=True,
            include_disabled=False,
        )
        _set_ozon_bg_state(
            running=False,
            finished_at=_now_iso(),
            result=result,
            last_error=None,
        )
    except Exception as e:
        _set_ozon_bg_state(
            running=False,
            finished_at=_now_iso(),
            result=None,
            last_error=str(e)[:1000],
        )
    finally:
        if conn is not None:
            conn.close()


def _start_ozon_bg_sync(client_id: str, api_key: str) -> tuple[bool, str]:
    global _OZON_SYNC_BG_THREAD
    state = _get_ozon_bg_state()
    if state.get("running") and state.get("thread_alive"):
        return False, "Фоновая синхронизация Ozon уже выполняется."
    active_db = _get_active_db_path() or str(Path("data/catalog.db"))
    _set_ozon_bg_state(
        running=True,
        started_at=_now_iso(),
        finished_at=None,
        result=None,
        last_error=None,
        db_path=active_db,
    )
    _OZON_SYNC_BG_THREAD = threading.Thread(
        target=_ozon_bg_worker,
        args=(str(active_db), str(client_id or ""), str(api_key or "")),
        daemon=True,
        name="ozon-full-sync-bg",
    )
    _OZON_SYNC_BG_THREAD.start()
    return True, f"Фоновая синхронизация Ozon запущена. База: {active_db}"


def to_attribute_code(name: str) -> str:
    clean = str(name or "").strip().lower()
    clean = "_".join("".join(ch if ch.isalnum() else " " for ch in clean).split())
    return clean[:120]


def list_distinct_values(conn, column_name: str) -> list[str]:
    rows = conn.execute(
        f"""
        SELECT DISTINCT TRIM({column_name}) AS value
        FROM products
        WHERE {column_name} IS NOT NULL
          AND TRIM({column_name}) <> ''
        ORDER BY value
        """
    ).fetchall()
    return [str(r["value"]) for r in rows if r["value"]]


def list_catalog_categories(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT category, base_category, subcategory, ozon_category_path
        FROM products
        """
    ).fetchall()
    if not rows:
        return []

    def split_ozon_path(path: str | None) -> list[str]:
        text = " ".join(str(path or "").strip().split())
        if not text:
            return []
        chunks = [x.strip() for x in re.split(r"\s*(?:/|>|»|→|\|)\s*", text) if str(x).strip()]
        return chunks if chunks else [text]

    ozon_values: set[str] = set()
    legacy_values: set[str] = set()
    for row in rows:
        category = str(row["category"] or "").strip()
        base_category = str(row["base_category"] or "").strip()
        subcategory = str(row["subcategory"] or "").strip()
        ozon_path = str(row["ozon_category_path"] or "").strip()

        if ozon_path:
            parts = split_ozon_path(ozon_path)
            ozon_values.add(ozon_path)
            if parts:
                ozon_values.add(parts[-1])
            if len(parts) >= 2:
                ozon_values.add(parts[-2])

        for value in (category, base_category, subcategory):
            if value:
                legacy_values.add(value)

    preferred = sorted([v for v in ozon_values if v], key=lambda x: x.lower())
    legacy_only = sorted([v for v in legacy_values if v and v not in ozon_values], key=lambda x: x.lower())
    # Ozon — эталон категорий. Legacy используем только если Ozon-категорий пока нет.
    return preferred if preferred else legacy_only


def _split_ozon_path_parts(path: str | None) -> list[str]:
    text = " ".join(str(path or "").strip().split())
    if not text:
        return []
    parts = [x.strip() for x in re.split(r"\s*(?:/|>|»|→|\|)\s*", text) if str(x).strip()]
    return parts if parts else [text]


def list_ozon_category_filters(conn) -> tuple[list[str], list[str]]:
    rows = conn.execute(
        """
        SELECT DISTINCT ozon_category_path
        FROM products
        WHERE ozon_category_path IS NOT NULL
          AND TRIM(ozon_category_path) <> ''
        """
    ).fetchall()
    categories: set[str] = set()
    subcategories: set[str] = set()
    for row in rows:
        path = str(row["ozon_category_path"] or "").strip()
        parts = _split_ozon_path_parts(path)
        if not parts:
            continue
        subcategories.add(parts[-1])
        categories.add(parts[-2] if len(parts) >= 2 else parts[-1])
    return (
        sorted([x for x in categories if x], key=lambda x: x.lower()),
        sorted([x for x in subcategories if x], key=lambda x: x.lower()),
    )


RU_COLUMN_MAP: dict[str, str] = {
    "id": "ID",
    "product_id": "ID товара",
    "article": "Артикул",
    "internal_article": "Внутренний артикул",
    "supplier_article": "Артикул поставщика",
    "name": "Название",
    "brand": "Бренд",
    "barcode": "Штрихкод",
    "category": "Категория",
    "base_category": "Базовая категория",
    "subcategory": "Подкатегория",
    "supplier_name": "Поставщик",
    "supplier_url": "Ссылка поставщика",
    "description": "Описание",
    "image_url": "Фото",
    "weight": "Вес, кг",
    "gross_weight": "Вес брутто, кг",
    "length": "Длина, см",
    "width": "Ширина, см",
    "height": "Высота, см",
    "package_length": "Длина упаковки, см",
    "package_width": "Ширина упаковки, см",
    "package_height": "Высота упаковки, см",
    "uom": "Ед. изм.",
    "tnved_code": "ТН ВЭД",
    "wheel_diameter_inch": "Диаметр колеса, inch",
    "updated_at": "Обновлено",
    "created_at": "Создано",
    "import_batch_id": "Партия импорта",
    "supplier_parse_status": "Статус парсинга",
    "supplier_parse_comment": "Комментарий парсинга",
    "ozon_description_category_id": "ID категории Ozon",
    "ozon_type_id": "ID типа Ozon",
    "ozon_category_path": "Ozon категория",
    "ozon_category_confidence": "Уверенность Ozon",
    "description_category_id": "ID категории Ozon",
    "type_id": "ID типа Ozon",
    "type_name": "Тип категории Ozon",
    "category_name": "Название категории Ozon",
    "full_path": "Путь категории Ozon",
    "children_count": "Дочерних категорий",
    "disabled": "Отключена",
    "fetched_at": "Загружено",
    "nodes": "Узлов",
    "attribute_id": "ID атрибута",
    "attribute_code": "Код атрибута",
    "code": "Код атрибута",
    "data_type": "Тип данных",
    "scope": "Область",
    "entity_type": "Сущность",
    "channel_code": "Канал",
    "locale": "Локаль",
    "is_required": "Обязательный",
    "is_required_for_category": "Обязательный для категории",
    "is_collection": "Множественный",
    "is_multi_value": "Множественный",
    "dictionary_id": "ID справочника",
    "group_name": "Группа",
    "max_value_count": "Макс. значений",
    "value": "Значение",
    "value_text": "Текстовое значение",
    "value_number": "Числовое значение",
    "value_boolean": "Булево значение",
    "value_json": "JSON значение",
    "value_id": "ID значения",
    "info": "Инфо",
    "picture": "Картинка",
    "field_name": "Поле",
    "source_type": "Источник",
    "source_url": "URL источника",
    "source_value_raw": "Сырое значение",
    "confidence": "Уверенность",
    "source_name": "Источник значения",
    "transform_rule": "Правило трансформации",
    "matched_by": "Метод сопоставления",
    "status": "Статус",
}

ATTRIBUTE_CODE_RU_OVERRIDES: dict[str, str] = {
    "main_image": "Главное изображение",
    "gallery_images": "Галерея изображений",
    "article": "Артикул",
    "supplier_article": "Артикул поставщика",
    "internal_article": "Внутренний артикул",
    "image_url": "Ссылка на фото",
}


def with_ru_columns(df: pd.DataFrame, extra_map: dict[str, str] | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    mapping = dict(RU_COLUMN_MAP)
    if extra_map:
        mapping.update(extra_map)
    return df.rename(columns={c: mapping.get(c, c) for c in df.columns})


def humanize_attribute_code(code: str | None) -> str:
    text = str(code or "").strip()
    if not text:
        return ""
    if text in ATTRIBUTE_CODE_RU_OVERRIDES:
        return ATTRIBUTE_CODE_RU_OVERRIDES[text]
    if text.startswith("ozon_attr_"):
        attr_id = text.replace("ozon_attr_", "", 1)
        return f"Ozon атрибут ID {attr_id}"
    return " ".join(text.replace("_", " ").split()).capitalize()


def _build_ozon_scope_labels(conn) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT DISTINCT category_code
        FROM channel_attribute_requirements
        WHERE channel_code = 'ozon'
          AND category_code IS NOT NULL
          AND TRIM(category_code) <> ''
        ORDER BY category_code
        """
    ).fetchall()
    labels: dict[str, str] = {}
    for row in rows:
        code = str(row["category_code"])
        labels[code] = code
        if not code.startswith("ozon:"):
            continue
        parts = code.split(":")
        if len(parts) != 3:
            continue
        try:
            desc_id = int(parts[1])
            type_id = int(parts[2])
        except Exception:
            continue
        cat = conn.execute(
            """
            SELECT MAX(full_path) AS full_path, MAX(type_name) AS type_name
            FROM ozon_category_cache
            WHERE description_category_id = ? AND type_id = ?
            """,
            (desc_id, type_id),
        ).fetchone()
        full_path = str(cat["full_path"] or "").strip() if cat else ""
        type_name = str(cat["type_name"] or "").strip() if cat else ""
        if full_path or type_name:
            labels[code] = f"{full_path or '-'} | {type_name or '-'} | cat={desc_id}, type={type_id}"
        else:
            fallback = conn.execute(
                """
                SELECT MAX(category_name) AS category_name
                FROM ozon_category_cache
                WHERE description_category_id = ?
                """,
                (desc_id,),
            ).fetchone()
            category_name = str(fallback["category_name"] or "").strip() if fallback else ""
            if category_name:
                labels[code] = f"{category_name} | тип={type_id} | cat={desc_id}"
            else:
                labels[code] = f"Ozon категория {desc_id} | тип {type_id}"
    return labels


def _build_ozon_template_category_options(
    conn,
    channel_code: str | None = None,
    limit: int = 5000,
) -> tuple[list[str], dict[str, str]]:
    options: list[str] = [""]
    labels: dict[str, str] = {"": "-- без категории --"}
    seen: set[str] = {""}

    try:
        pairs = list_cached_category_pairs(conn, limit=max(200, int(limit)))
    except Exception:
        pairs = []

    for row in pairs:
        desc_id = row.get("description_category_id")
        type_id = row.get("type_id")
        if desc_id is None or type_id is None:
            continue
        code = f"ozon:{int(desc_id)}:{int(type_id)}"
        if code in seen:
            continue
        full_path = str(row.get("full_path") or row.get("category_name") or "").strip()
        type_name = str(row.get("type_name") or "").strip()
        labels[code] = f"{full_path or '-'} | {type_name or '-'} | {code}"
        options.append(code)
        seen.add(code)

    # Добавляем legacy-категории из сохранённых профилей канала, чтобы не потерять совместимость.
    profile_categories: set[str] = set()
    if channel_code:
        for profile in list_template_profiles(conn, channel_code=channel_code):
            raw = str(profile.get("category_code") or "").strip()
            if raw:
                profile_categories.add(raw)
    if profile_categories:
        scope_labels = _build_ozon_scope_labels(conn)
        for code in sorted(profile_categories):
            if code in seen:
                continue
            labels[code] = scope_labels.get(code, code)
            options.append(code)
            seen.add(code)

    return options, labels


def _is_blank_value(value: object) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() == "none"


def render_supplier_url(url_template: str, row: dict) -> str | None:
    if not url_template:
        return None
    article = str(row.get("article") or "").strip()
    supplier_article = str(row.get("supplier_article") or "").strip()
    name = str(row.get("name") or "").strip()
    category = str(row.get("category") or "").strip()
    code = str(row.get("article") or row.get("supplier_article") or "").strip()
    rendered = str(url_template)
    rendered = rendered.replace("{article}", article)
    rendered = rendered.replace("{article_q}", quote(article, safe=""))
    rendered = rendered.replace("{supplier_article}", supplier_article)
    rendered = rendered.replace("{supplier_article_q}", quote(supplier_article, safe=""))
    rendered = rendered.replace("{name}", name)
    rendered = rendered.replace("{name_q}", quote(name, safe=""))
    rendered = rendered.replace("{category}", category)
    rendered = rendered.replace("{category_q}", quote(category, safe=""))
    rendered = rendered.replace("{code}", code)
    rendered = rendered.replace("{code_q}", quote(code, safe=""))
    rendered = rendered.strip()
    if not rendered:
        return None
    if rendered.startswith("http://") or rendered.startswith("https://"):
        return rendered
    if "." in rendered and " " not in rendered:
        return f"https://{rendered}"
    return None


def load_product_ids(
    conn,
    search: str = "",
    category: str = "",
    supplier: str = "",
    import_batch_id: str = "",
    parse_filter: str = "Все",
    limit: int | None = None,
    offset: int = 0,
) -> list[int]:
    where, params = _build_product_filters(
        search=search,
        category=category,
        supplier=supplier,
        import_batch_id=import_batch_id,
        parse_filter=parse_filter,
    )
    sql = "SELECT id FROM products"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset)])
    rows = conn.execute(sql, params).fetchall()
    return [int(r["id"]) for r in rows]


def load_product_ids_with_supplier_url(
    conn,
    search: str = "",
    category: str = "",
    supplier: str = "",
    import_batch_id: str = "",
    parse_filter: str = "Все",
    limit: int | None = None,
    offset: int = 0,
) -> list[int]:
    where, params = _build_product_filters(
        search=search,
        category=category,
        supplier=supplier,
        import_batch_id=import_batch_id,
        parse_filter=parse_filter,
    )
    where.append("supplier_url IS NOT NULL AND TRIM(supplier_url) <> ''")
    sql = "SELECT id FROM products"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset)])
    rows = conn.execute(sql, params).fetchall()
    return [int(r["id"]) for r in rows]


def apply_mass_product_updates(
    conn,
    product_ids: list[int],
    updates: dict[str, str],
    supplier_url_template: str | None = None,
    only_empty: bool = False,
) -> dict:
    if not product_ids:
        return {"updated_products": 0, "updated_fields": 0}
    tracked_fields = ["supplier_name", "supplier_url", "category", "base_category", "subcategory", "brand"]
    updated_products = 0
    updated_fields = 0
    for pid in product_ids:
        row = conn.execute(
            """
            SELECT id, article, supplier_article, name, category, supplier_name, supplier_url, base_category, subcategory, brand
                 , ozon_description_category_id, ozon_type_id
            FROM products
            WHERE id = ?
            LIMIT 1
            """,
            (int(pid),),
        ).fetchone()
        if not row:
            continue
        current = dict(row)
        row_updates: dict[str, str] = {}
        ozon_locked = bool(int(current.get("ozon_description_category_id") or 0) > 0 and int(current.get("ozon_type_id") or 0) > 0)
        for field, value in updates.items():
            if value is None:
                continue
            if ozon_locked and field in {"category", "base_category", "subcategory"}:
                continue
            if only_empty and not _is_blank_value(current.get(field)):
                continue
            row_updates[field] = str(value).strip()

        if supplier_url_template:
            generated = render_supplier_url(supplier_url_template, current)
            if generated:
                if (not only_empty) or _is_blank_value(current.get("supplier_url")):
                    row_updates["supplier_url"] = generated

        if not row_updates:
            continue

        set_clause = ", ".join([f"{k} = ?" for k in row_updates.keys()])
        params = list(row_updates.values()) + [int(pid)]
        conn.execute(
            f"UPDATE products SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            params,
        )
        for field_name, value in row_updates.items():
            if field_name in tracked_fields:
                save_field_source(
                    conn=conn,
                    product_id=int(pid),
                    field_name=field_name,
                    source_type="manual",
                    source_value_raw=value,
                    source_url=None,
                    confidence=1.0,
                    is_manual=True,
                )
                updated_fields += 1
        updated_products += 1
    conn.commit()
    return {"updated_products": updated_products, "updated_fields": updated_fields}


def render_section_help() -> None:
    with st.expander("Инструкция по разделам и кнопкам", expanded=False):
        st.markdown(
            """
**Импорт**
- `Скачать шаблон импорта поставщика`: скачать эталонный Excel для загрузки каталога.
- `Профиль поставщика`: выбрать поставщика из базы.
- `Сохранить профиль`: сохранить/обновить поставщика, сайт и URL-шаблон.
- `Импортировать`: загрузить файл в мастер-каталог.

**Каталог**
- `Поиск`: быстрый поиск по названию/артикулу/штрихкоду.
- `Категория` / `Поставщик`: фильтры из выпадающих меню; категория в приоритете берётся из Ozon-эталона.
- `Размер страницы` / `Страница` / `◀ Назад` / `Вперед ▶`: постраничная навигация.
- `Обновить дубли`: пересчитать дубли по текущей странице.
- `Обогатить поставщика`: массовый парсинг supplier_url по текущей странице.
- `Автопривязать Ozon категории`: назначить эталонную Ozon-категорию.

**Карточка**
- `Поиск товара / Категория / Подкатегория / Поставщик`: выбрать нужный товар прямо в разделе Карточка.
- `Спарсить поставщика`: мягкое обогащение (не перетирает сильные значения).
- `Перезаполнить из поставщика`: жесткое обогащение с перезаписью.
- `Подобрать Ozon категорию`: автоподбор эталонной Ozon-категории.
- `Перепривязать Ozon категорию (force)`: повторный подбор с перезаписью.
- `Атрибуты для заполнения`: редактирование Ozon и клиентских атрибутов по выбранному каналу/категории.
- `Сохранить карточку`: сохранить ручные изменения в мастер-карточке.

**Клиентский шаблон**
- `Авторегистрация шаблона`: колонки автоматически добавляются в атрибуты и требования канала/категории.
- `Сохранить mapping rules`: сохранить правила соответствия колонок.
- `Сохранить профиль шаблона`: сохранить тип шаблона клиента для повторной выгрузки.
- `Добавить несматченные в master-атрибуты`: расширить мастер-карточку новыми полями.
- `Подтвердить значения как client_validated`: зафиксировать проверенные значения.
- `Скачать заполненный шаблон`: выгрузка результата в формате клиента.
            """
        )


def _build_product_filters(
    search: str = "",
    category: str = "",
    supplier: str = "",
    import_batch_id: str = "",
    parse_filter: str = "Все",
) -> tuple[list[str], list[object]]:
    where = []
    params: list[object] = []

    if search:
        where.append("(name LIKE ? OR article LIKE ? OR barcode LIKE ? OR supplier_article LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s, s])

    if category:
        where.append(
            "("
            "LOWER(TRIM(category)) = LOWER(TRIM(?)) "
            "OR LOWER(TRIM(base_category)) = LOWER(TRIM(?)) "
            "OR LOWER(TRIM(subcategory)) = LOWER(TRIM(?)) "
            "OR LOWER(TRIM(IFNULL(ozon_category_path, ''))) = LOWER(TRIM(?)) "
            "OR LOWER(IFNULL(ozon_category_path, '')) LIKE LOWER(?)"
            ")"
        )
        params.extend([category, category, category, category, f"%{category}%"])

    if supplier:
        where.append("LOWER(TRIM(supplier_name)) = LOWER(TRIM(?))")
        params.append(supplier)

    if import_batch_id:
        where.append("import_batch_id = ?")
        params.append(import_batch_id)

    if parse_filter == "Есть supplier_url":
        where.append("supplier_url IS NOT NULL AND TRIM(supplier_url) <> ''")
    elif parse_filter == "Не парсено":
        where.append("(supplier_parse_status IS NULL OR TRIM(supplier_parse_status) = '')")
    elif parse_filter == "Ошибка":
        where.append("supplier_parse_status = 'error'")
    elif parse_filter == "Успех":
        where.append("supplier_parse_status = 'success'")

    return where, params


def count_products(
    conn,
    search: str = "",
    category: str = "",
    supplier: str = "",
    import_batch_id: str = "",
    parse_filter: str = "Все",
) -> int:
    where, params = _build_product_filters(
        search=search,
        category=category,
        supplier=supplier,
        import_batch_id=import_batch_id,
        parse_filter=parse_filter,
    )
    sql = "SELECT COUNT(*) AS total FROM products"
    if where:
        sql += " WHERE " + " AND ".join(where)
    row = conn.execute(sql, params).fetchone()
    return int(row["total"]) if row and row["total"] is not None else 0


def load_products(
    conn,
    search: str = "",
    category: str = "",
    supplier: str = "",
    limit: int = 200,
    import_batch_id: str = "",
    parse_filter: str = "Все",
    offset: int = 0,
) -> pd.DataFrame:
    where, params = _build_product_filters(
        search=search,
        category=category,
        supplier=supplier,
        import_batch_id=import_batch_id,
        parse_filter=parse_filter,
    )

    sql = """
        SELECT
            id,
            article,
            internal_article,
            supplier_article,
            name,
            brand,
            supplier_name,
            supplier_url,
            barcode,
            category,
            base_category,
            subcategory,
            wheel_diameter_inch,
            weight,
            length,
            width,
            height,
            package_length,
            package_width,
            package_height,
            gross_weight,
            enrichment_status,
            enrichment_comment,
            supplier_parse_status,
            duplicate_status,
            ozon_description_category_id,
            ozon_type_id,
            ozon_category_path,
            ozon_category_confidence,
            import_batch_id,
            updated_at
        FROM products
    """

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.append(int(limit))
    params.append(int(offset))

    rows = conn.execute(sql, params).fetchall()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


def get_product(conn, product_id: int):
    return conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()


def find_products_for_card(
    conn,
    search: str = "",
    ozon_category: str = "",
    ozon_subcategory: str = "",
    supplier: str = "",
    limit: int = 5000,
) -> list[dict]:
    where: list[str] = []
    params: list[object] = []
    if search:
        where.append("(name LIKE ? OR article LIKE ? OR internal_article LIKE ? OR supplier_article LIKE ?)")
        s = f"%{search.strip()}%"
        params.extend([s, s, s, s])
    if supplier and supplier != "Все":
        where.append("LOWER(TRIM(supplier_name)) = LOWER(TRIM(?))")
        params.append(supplier)

    sql = """
        SELECT
            id, article, internal_article, supplier_article, name,
            category, base_category, subcategory, supplier_name, ozon_category_path,
            ozon_description_category_id, ozon_type_id
        FROM products
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, params).fetchall()
    out: list[dict] = []
    category_filter = str(ozon_category or "").strip().lower()
    subcategory_filter = str(ozon_subcategory or "").strip().lower()
    for row in rows:
        item = dict(row)
        parts = _split_ozon_path_parts(item.get("ozon_category_path"))
        item["ozon_subcategory"] = parts[-1] if parts else ""
        item["ozon_category"] = parts[-2] if len(parts) >= 2 else item["ozon_subcategory"]
        if category_filter and category_filter != "все":
            if str(item.get("ozon_category") or "").strip().lower() != category_filter:
                continue
        if subcategory_filter and subcategory_filter != "все":
            if str(item.get("ozon_subcategory") or "").strip().lower() != subcategory_filter:
                continue
        out.append(item)
        if len(out) >= int(limit):
            break
    return out


def list_channel_codes(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT channel_code
        FROM (
            SELECT channel_code FROM channel_profiles
            UNION ALL
            SELECT channel_code FROM channel_attribute_requirements
            UNION ALL
            SELECT channel_code FROM channel_mapping_rules
        )
        WHERE channel_code IS NOT NULL
          AND TRIM(channel_code) <> ''
        ORDER BY channel_code
        """
    ).fetchall()
    return [str(r["channel_code"]) for r in rows if r["channel_code"]]


def list_channel_category_codes(conn, channel_code: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT category_code
        FROM (
            SELECT category_code
            FROM channel_attribute_requirements
            WHERE channel_code = ?
            UNION ALL
            SELECT category_code
            FROM channel_mapping_rules
            WHERE channel_code = ?
        )
        WHERE category_code IS NOT NULL
          AND TRIM(category_code) <> ''
        ORDER BY category_code
        """,
        (channel_code, channel_code),
    ).fetchall()
    return [str(r["category_code"]) for r in rows if r["category_code"]]


def ensure_template_columns_registered(
    conn,
    channel_code: str,
    category_code: str | None,
    template_columns: list[object],
) -> dict[str, int]:
    if not channel_code:
        return {"attributes": 0, "requirements": 0, "rules": 0}

    created_attributes = 0
    created_requirements = 0
    created_rules = 0

    for idx, col in enumerate(template_columns):
        col_name = str(col or "").strip()
        if not col_name:
            continue
        code = to_attribute_code(col_name)
        if not code:
            continue

        existed_attr = conn.execute(
            "SELECT 1 FROM attribute_definitions WHERE code = ?",
            (code,),
        ).fetchone()
        upsert_attribute_definition(
            conn=conn,
            code=code,
            name=col_name,
            data_type="text",
            scope="master",
            unit=None,
            description=f"Автосоздано из клиентского шаблона: {col_name}",
        )
        if not existed_attr:
            created_attributes += 1

        existed_req = conn.execute(
            """
            SELECT 1
            FROM channel_attribute_requirements
            WHERE channel_code = ?
              AND IFNULL(category_code, '') = IFNULL(?, '')
              AND attribute_code = ?
            """,
            (channel_code, category_code, code),
        ).fetchone()
        upsert_channel_attribute_requirement(
            conn=conn,
            channel_code=channel_code,
            category_code=category_code or None,
            attribute_code=code,
            is_required=0,
            sort_order=1000 + int(idx),
            notes="Автодобавлено из клиентского шаблона",
        )
        if not existed_req:
            created_requirements += 1

        existed_rule = conn.execute(
            """
            SELECT 1
            FROM channel_mapping_rules
            WHERE channel_code = ?
              AND IFNULL(category_code, '') = IFNULL(?, '')
              AND target_field = ?
            """,
            (channel_code, category_code, col_name),
        ).fetchone()
        if not existed_rule:
            upsert_channel_mapping_rule(
                conn=conn,
                channel_code=channel_code,
                category_code=category_code or None,
                target_field=col_name,
                source_type="attribute",
                source_name=code,
                transform_rule=None,
                is_required=0,
            )
            created_rules += 1

    return {
        "attributes": created_attributes,
        "requirements": created_requirements,
        "rules": created_rules,
    }


def save_product(conn, product_id: int, payload: dict):
    conn.execute(
        """
        UPDATE products
        SET
            article = ?,
            internal_article = ?,
            supplier_article = ?,
            name = ?,
            brand = ?,
            supplier_name = ?,
            barcode = ?,
            barcode_source = ?,
            category = ?,
            base_category = ?,
            subcategory = ?,
            wheel_diameter_inch = ?,
            supplier_url = ?,
            ozon_description_category_id = ?,
            ozon_type_id = ?,
            ozon_category_path = ?,
            ozon_category_confidence = ?,
            uom = ?,
            weight = ?,
            length = ?,
            width = ?,
            height = ?,
            package_length = ?,
            package_width = ?,
            package_height = ?,
            gross_weight = ?,
            image_url = ?,
            description = ?,
            tnved_code = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            payload.get("article"),
            payload.get("internal_article"),
            payload.get("supplier_article"),
            payload.get("name"),
            payload.get("brand"),
            payload.get("supplier_name"),
            payload.get("barcode"),
            payload.get("barcode_source"),
            payload.get("category"),
            payload.get("base_category"),
            payload.get("subcategory"),
            payload.get("wheel_diameter_inch"),
            payload.get("supplier_url"),
            payload.get("ozon_description_category_id"),
            payload.get("ozon_type_id"),
            payload.get("ozon_category_path"),
            payload.get("ozon_category_confidence"),
            payload.get("uom"),
            payload.get("weight"),
            payload.get("length"),
            payload.get("width"),
            payload.get("height"),
            payload.get("package_length"),
            payload.get("package_width"),
            payload.get("package_height"),
            payload.get("gross_weight"),
            payload.get("image_url"),
            payload.get("description"),
            payload.get("tnved_code"),
            product_id,
        ),
    )
    tracked_fields = [
        "article", "internal_article", "supplier_article", "name", "brand", "supplier_name", "barcode",
        "category", "base_category", "subcategory", "wheel_diameter_inch", "supplier_url",
        "ozon_description_category_id", "ozon_type_id", "ozon_category_path", "ozon_category_confidence", "uom",
        "weight", "length", "width", "height", "package_length", "package_width", "package_height",
        "gross_weight", "image_url", "description", "tnved_code"
    ]
    for field_name in tracked_fields:
        value = payload.get(field_name)
        if value not in (None, ""):
            save_field_source(
                conn=conn,
                product_id=product_id,
                field_name=field_name,
                source_type="manual",
                source_value_raw=value,
                source_url=None,
                confidence=1.0,
                is_manual=True,
            )
    conn.commit()


def export_current_df(df: pd.DataFrame):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="products")
    return output.getvalue()


def dataframes_to_excel_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_name = str(sheet_name)[:31] if sheet_name else "sheet"
            frame = df if df is not None else pd.DataFrame()
            frame.to_excel(writer, index=False, sheet_name=safe_name)
    return output.getvalue()


def build_supplier_catalog_template_excel() -> bytes:
    df = pd.DataFrame(
        {
            "Артикул": ["SUP-001", "SUP-002"],
            "Артикул поставщика": ["RB_0001", "RB_0002"],
            "Номенклатура": ["Велофара Rockbros", "Насос SKS"],
            "Штрихкод": ["4600000000011", "4600000000012"],
            "Категория": ["Вело", "Вело"],
            "Поставщик": ["Rockbros", "SKS"],
            "Ссылка на товар": ["https://example.com/item1", "https://example.com/item2"],
            "Вес": [0.35, 0.42],
            "Длина": [15, 21],
            "Ширина": [9, 6],
            "Высота": [5, 4],
            "Длина упаковки": [17, 23],
            "Ширина упаковки": [10, 7],
            "Высота упаковки": [6, 5],
            "Вес брутто": [0.41, 0.48],
            "Фото": ["https://example.com/img1.jpg", "https://example.com/img2.jpg"],
            "Описание": ["Яркая велофара", "Легкий насос"],
        }
    )
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Каталог")
    return output.getvalue()


def inspect_excel_sheets(file_bytes: bytes) -> dict:
    try:
        xls = pd.ExcelFile(BytesIO(file_bytes))
    except Exception as e:
        return {"ok": False, "message": str(e)}

    sheets = xls.sheet_names
    header_keywords = {
        "артикул",
        "номенклатура",
        "наименование",
        "название",
        "категория",
        "бренд",
        "штрихкод",
        "код",
        "поставщик",
        "ссылка",
    }
    preview_rows = []
    for sheet in sheets[:10]:
        try:
            probe = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=None, nrows=12)
            row_scores = []
            for i in range(len(probe)):
                values = [str(v).strip() for v in probe.iloc[i].tolist() if str(v).strip() and str(v).strip().lower() != "nan"]
                lower_values = [v.lower() for v in values]
                keyword_hits = sum(1 for v in lower_values if v in header_keywords)
                text_like = sum(1 for v in values if any(ch.isalpha() for ch in v))
                score = len(values) + keyword_hits * 3 + text_like
                row_scores.append({"row": i + 1, "non_empty": len(values), "keywords": keyword_hits, "sample": ", ".join(values[:6]), "score": score})
            recommended = 1
            if row_scores:
                best = max(row_scores, key=lambda r: r["score"])
                recommended = int(best["row"])
            preview_rows.append({"sheet": sheet, "rows": row_scores, "recommended_header_row": recommended})
        except Exception:
            preview_rows.append({"sheet": sheet, "rows": [], "recommended_header_row": 1})
    return {"ok": True, "sheets": sheets, "preview": preview_rows}


def build_ozon_product_list_template_excel() -> bytes:
    df = pd.DataFrame(
        {
            "article": ["ART-001", "ART-002"],
            "internal_article": ["INT-001", "INT-002"],
            "supplier_article": ["SUP-001", "SUP-002"],
            "id": [1, 2],
        }
    )
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="products")
    return output.getvalue()


def build_ozon_dictionary_overrides_template_excel() -> bytes:
    df = pd.DataFrame(
        {
            "attribute_id": [85, 8229],
            "raw_value": ["stels", "bike"],
            "value_id": [123456, 654321],
            "value": ["Stels", "Велосипед"],
            "comment": ["Бренд нормализован", "Тип товара"],
        }
    )
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="overrides")
    return output.getvalue()


def build_ozon_retry_jobs_template_excel() -> bytes:
    df = pd.DataFrame({"job_id": [101, 102, 103]})
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="jobs")
    return output.getvalue()


def _cell_to_lookup_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value).strip()
    return str(value).strip()


def resolve_product_ids_from_excel(
    conn,
    file_bytes: bytes,
    lookup_field: str,
    sheet_name: str | None = None,
    column_name: str | None = None,
) -> dict:
    try:
        if sheet_name:
            df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name)
        else:
            df = pd.read_excel(BytesIO(file_bytes))
    except Exception as e:
        return {"ok": False, "message": f"Не удалось прочитать Excel: {e}"}

    if df.empty:
        return {"ok": False, "message": "Excel пустой"}

    lookup_aliases = {
        "id": ["id", "product_id", "товар id", "id товара"],
        "article": ["article", "артикул", "sku", "vendor code", "код товара"],
        "internal_article": ["internal_article", "внутренний артикул", "артикул 1с"],
        "supplier_article": ["supplier_article", "артикул поставщика"],
    }
    field = str(lookup_field or "article").strip().lower()
    aliases = lookup_aliases.get(field, lookup_aliases["article"])

    normalized_columns = {str(c).strip().lower(): str(c) for c in df.columns}
    selected_column = None
    manual_column = (column_name or "").strip()
    if manual_column:
        manual_norm = manual_column.lower()
        selected_column = normalized_columns.get(manual_norm)
        if not selected_column:
            return {"ok": False, "message": f"Колонка '{manual_column}' не найдена в Excel."}
    for alias in aliases:
        if selected_column:
            break
        if alias in normalized_columns:
            selected_column = normalized_columns[alias]
            break
    if not selected_column:
        selected_column = str(df.columns[0])

    raw_values = [_cell_to_lookup_text(v) for v in df[selected_column].tolist()]
    values = []
    seen = set()
    for v in raw_values:
        if not v:
            continue
        key = v.lower() if field != "id" else v
        if key in seen:
            continue
        seen.add(key)
        values.append(v)

    resolved_ids = []
    not_found = []
    for v in values:
        row = None
        if field == "id":
            try:
                row = conn.execute("SELECT id FROM products WHERE id = ? LIMIT 1", (int(float(v)),)).fetchone()
            except Exception:
                row = None
        elif field in {"article", "internal_article", "supplier_article"}:
            row = conn.execute(f"SELECT id FROM products WHERE lower(IFNULL({field}, '')) = lower(?) LIMIT 1", (v,)).fetchone()
        if row:
            resolved_ids.append(int(row["id"]))
        else:
            not_found.append(v)

    return {
        "ok": True,
        "lookup_field": field,
        "used_column": selected_column,
        "input_values": int(len(values)),
        "resolved_ids": sorted(list(set(resolved_ids))),
        "resolved_count": int(len(set(resolved_ids))),
        "not_found": not_found,
        "not_found_count": int(len(not_found)),
    }


def import_dictionary_overrides_from_excel(
    conn,
    file_bytes: bytes,
    description_category_id: int,
    type_id: int,
) -> dict:
    try:
        df = pd.read_excel(BytesIO(file_bytes))
    except Exception as e:
        return {"ok": False, "message": f"Не удалось прочитать Excel: {e}"}

    required_cols = {"attribute_id", "raw_value", "value_id"}
    actual_cols = {str(c).strip().lower(): str(c) for c in df.columns}
    missing = [c for c in required_cols if c not in actual_cols]
    if missing:
        return {"ok": False, "message": f"В Excel нет обязательных колонок: {', '.join(missing)}"}

    applied = 0
    skipped = 0
    errors = []
    for idx, row in df.iterrows():
        try:
            attribute_id_raw = row[actual_cols["attribute_id"]]
            raw_value = _cell_to_lookup_text(row[actual_cols["raw_value"]])
            value_id_raw = row[actual_cols["value_id"]]
            value = row[actual_cols["value"]] if "value" in actual_cols else None
            comment = row[actual_cols["comment"]] if "comment" in actual_cols else None

            if not raw_value:
                skipped += 1
                continue

            attribute_id = int(float(attribute_id_raw))
            value_id = int(float(value_id_raw))
            save_dictionary_override(
                conn=conn,
                description_category_id=int(description_category_id),
                type_id=int(type_id),
                attribute_id=attribute_id,
                raw_value=raw_value,
                value_id=value_id,
                value=_cell_to_lookup_text(value) if value is not None else None,
                comment=_cell_to_lookup_text(comment) if comment is not None else None,
            )
            applied += 1
        except Exception as e:
            skipped += 1
            errors.append({"row": int(idx) + 2, "error": str(e)})

    return {
        "ok": True,
        "applied": int(applied),
        "skipped": int(skipped),
        "errors": errors[:100],
    }


def resolve_job_ids_from_excel(file_bytes: bytes, column_name: str | None = None) -> dict:
    try:
        df = pd.read_excel(BytesIO(file_bytes))
    except Exception as e:
        return {"ok": False, "message": f"Не удалось прочитать Excel: {e}"}

    if df.empty:
        return {"ok": False, "message": "Excel пустой"}

    columns = {str(c).strip().lower(): str(c) for c in df.columns}
    selected_column = None
    if column_name:
        selected_column = columns.get(str(column_name).strip().lower())
        if not selected_column:
            return {"ok": False, "message": f"Колонка '{column_name}' не найдена"}
    if not selected_column:
        for alias in ["job_id", "id", "job"]:
            if alias in columns:
                selected_column = columns[alias]
                break
    if not selected_column:
        selected_column = str(df.columns[0])

    job_ids = []
    errors = []
    seen = set()
    for idx, value in enumerate(df[selected_column].tolist(), start=2):
        text = _cell_to_lookup_text(value)
        if not text:
            continue
        try:
            job_id = int(float(text))
            if job_id in seen:
                continue
            seen.add(job_id)
            job_ids.append(job_id)
        except Exception:
            errors.append({"row": idx, "value": text, "error": "Не удалось распознать job_id"})

    return {
        "ok": True,
        "used_column": selected_column,
        "job_ids": job_ids,
        "count": len(job_ids),
        "errors": errors[:100],
    }


def render_template_readiness(filled_df: pd.DataFrame, manual_rows: list[dict]) -> None:
    readiness = analyze_template_readiness(filled_df, manual_rows)
    summary = readiness["summary"]
    avg_readiness = int(summary["avg_readiness"])
    unmatched_columns = int(summary["unmatched_columns"])
    blocked_rows = int(summary["blocked_rows"])
    rows_to_fix = int(summary["partial_rows"] + summary["blocked_rows"])

    st.markdown("### Готовность шаблона")

    if avg_readiness >= 95 and unmatched_columns == 0 and blocked_rows == 0:
        st.success("Шаблон выглядит почти готовым, критичных дыр не видно.")
    elif avg_readiness >= 80:
        st.warning("Шаблон уже в рабочем состоянии, но часть полей и строк ещё нужно добить.")
    else:
        st.error("Шаблон пока сырой, сначала лучше закрыть пробелы в матчинге и данных.")

    st.progress(max(0, min(avg_readiness, 100)) / 100)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Средняя готовность", f"{avg_readiness}%")
    c2.metric("Matched", summary["matched_columns"])
    c3.metric("Unmatched", unmatched_columns)
    c4.metric("Готовых строк", summary["ready_rows"])
    c5.metric("Частично готовы", summary["partial_rows"])
    c6.metric("Блокеры", blocked_rows)

    coverage_df = pd.DataFrame(readiness["column_coverage"])
    row_df = pd.DataFrame(readiness["row_readiness"])

    if not coverage_df.empty:
        weak_columns = coverage_df[coverage_df["Покрытие, %"] < 100]
        if not weak_columns.empty:
            st.caption(f"Колонки, которые требуют внимания: {len(weak_columns)}")
            st.dataframe(weak_columns.head(15), use_container_width=True, hide_index=True)

    if rows_to_fix > 0:
        st.caption(f"Строки, которые ещё нужно добить: {rows_to_fix}")

    tab_cov, tab_rows, tab_all = st.tabs(["Проблемные колонки", "Проблемные строки", "Все колонки"])
    with tab_cov:
        if not coverage_df.empty:
            problem_df = coverage_df[coverage_df["Покрытие, %"] < 100]
            if problem_df.empty:
                st.success("Все колонки шаблона заполнены на 100%.")
            else:
                st.dataframe(problem_df, use_container_width=True, hide_index=True)
        else:
            st.info("Пока нет данных для оценки колонок.")
    with tab_rows:
        if not row_df.empty:
            st.dataframe(row_df.head(200), use_container_width=True, hide_index=True)
        else:
            st.success("Проблемных строк не найдено.")
    with tab_all:
        if not coverage_df.empty:
            st.dataframe(coverage_df, use_container_width=True, hide_index=True)
        else:
            st.info("Пока нет данных для полной сводки.")


def show_import_tab():
    st.subheader("Импорт каталога")
    st.caption("Загрузи Excel поставщика или общий каталог, система создаст или обновит мастер-товары и покажет последнюю партию отдельно.")
    with st.expander("Инструкция по кнопкам раздела Импорт", expanded=False):
        st.markdown(
            """
- `Скачать шаблон импорта поставщика (Excel)`: эталонный формат для поставщиков.
- `Сохранить профиль`: записывает профиль поставщика (имя, сайт, URL template) в БД.
- `Импортировать`: запускает импорт файла в мастер-каталог.
- `После импорта автоматически привязывать товары к Ozon категориям`: применяет Ozon-эталон сразу после загрузки.
            """
        )
    st.download_button(
        "Скачать шаблон импорта поставщика (Excel)",
        data=build_supplier_catalog_template_excel(),
        file_name="supplier_catalog_import_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="supplier_import_template",
    )
    profiles_conn = get_db()
    profiles = list_supplier_profiles(profiles_conn, only_active=True)
    existing_suppliers = list_distinct_values(profiles_conn, "supplier_name")
    profiles_conn.close()
    profile_map = {p["supplier_name"]: p for p in profiles}
    selected_profile_name = st.selectbox(
        "Профиль поставщика",
        options=[""] + sorted(profile_map.keys()),
        key="import_supplier_profile_name",
        help="Выбери профиль, чтобы автоматически подставить поставщика и URL-шаблон.",
    )
    if selected_profile_name:
        profile = profile_map[selected_profile_name]
        st.session_state["import_default_supplier_name"] = profile.get("supplier_name") or ""
        st.session_state["import_default_supplier_url_template"] = profile.get("url_template") or profile.get("base_url") or ""
        if profile.get("base_url"):
            st.caption(f"Базовый сайт поставщика: {profile.get('base_url')}")
    uploaded = st.file_uploader("Excel файл", type=["xlsx", "xls"])
    s1, s2 = st.columns(2)
    with s1:
        supplier_options = [""] + sorted(set(existing_suppliers + list(profile_map.keys())))
        session_supplier = st.session_state.get("import_default_supplier_name", "")
        supplier_index = supplier_options.index(session_supplier) if session_supplier in supplier_options else 0
        default_supplier_name = st.selectbox(
            "Поставщик по умолчанию (из базы)",
            options=supplier_options,
            index=supplier_index,
            help="Если в файле нет колонки Поставщик, будет выбран этот поставщик из базы.",
        )
        st.session_state["import_default_supplier_name"] = default_supplier_name
    with s2:
        default_supplier_url_template = st.text_input(
            "Шаблон URL поставщика (опционально)",
            value=st.session_state.get("import_default_supplier_url_template", ""),
            placeholder="https://site.ru/product/{supplier_article}",
            help="Поддерживает {article}, {supplier_article}, {code}, {name}, а также *_q для URL-encoding.",
        )
        st.session_state["import_default_supplier_url_template"] = default_supplier_url_template
    with st.expander("Профили поставщиков", expanded=False):
        sp1, sp2, sp3 = st.columns([2, 2, 1])
        with sp1:
            profile_name_input = st.text_input("Имя профиля", value=default_supplier_name or "", key="supplier_profile_name_input")
        with sp2:
            profile_base_url = st.text_input("Базовый URL", value="", key="supplier_profile_base_url")
        with sp3:
            save_profile_btn = st.button("Сохранить профиль", help="Сохранить/обновить профиль поставщика в базе")
        profile_url_template = st.text_input(
            "URL template профиля",
            value=default_supplier_url_template or "",
            key="supplier_profile_url_template",
        )
        if save_profile_btn and profile_name_input.strip():
            conn = get_db()
            profile_id = upsert_supplier_profile(
                conn=conn,
                supplier_name=profile_name_input.strip(),
                base_url=profile_base_url.strip() or None,
                url_template=profile_url_template.strip() or None,
                notes="Сохранено из вкладки Импорт",
                is_active=1,
            )
            conn.close()
            st.success(f"Профиль поставщика сохранён: #{profile_id}")
    auto_match_ozon_after_import = st.checkbox(
        "После импорта автоматически привязывать товары к Ozon категориям (эталон)",
        value=True,
        help="Работает, если кэш категорий Ozon уже синхронизирован во вкладке Ozon.",
        key="import_auto_ozon_match",
    )

    if uploaded is not None:
        uploaded_bytes = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
        if not uploaded_bytes:
            st.error("Файл прочитан пустым. Перезагрузи файл и повтори импорт.")
            return
        temp_path = Path("data/_import_temp.xlsx")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_bytes(uploaded_bytes)

        excel_info = inspect_excel_sheets(uploaded_bytes)
        import_mode = st.radio(
            "Режим определения структуры Excel",
            options=["Автоопределение", "Ручной выбор листа и строки заголовка"],
            horizontal=True,
            key="import_mode",
        )

        selected_sheet = None
        selected_header_row_zero = None
        if excel_info.get("ok"):
            sheets = excel_info.get("sheets") or []
            preview = excel_info.get("preview") or []
            recommended_by_sheet = {item.get("sheet"): int(item.get("recommended_header_row") or 1) for item in preview}
            if sheets:
                preview_df_rows = []
                for item in preview:
                    rows = item.get("rows") or []
                    recommended = int(item.get("recommended_header_row") or 1)
                    row_obj = next((r for r in rows if int(r.get("row", 0)) == recommended), {})
                    preview_df_rows.append(
                        {
                            "Лист": item.get("sheet"),
                            "Строки-превью": len(rows),
                            "Рекоменд. строка заголовка": recommended,
                            "Sample рекоменд. строки": row_obj.get("sample", ""),
                        }
                    )
                if preview_df_rows:
                    st.dataframe(pd.DataFrame(preview_df_rows), use_container_width=True, hide_index=True)

            if import_mode == "Ручной выбор листа и строки заголовка" and sheets:
                c1, c2 = st.columns(2)
                with c1:
                    selected_sheet = st.selectbox("Лист для импорта", options=sheets, index=0, key="manual_import_sheet")
                with c2:
                    default_header_row = int(recommended_by_sheet.get(selected_sheet, 2))
                    header_row_human = st.number_input(
                        "Строка заголовка (1 = первая строка)",
                        min_value=1,
                        max_value=50,
                        value=default_header_row,
                        step=1,
                        key=f"manual_import_header_row_{selected_sheet}",
                    )
                    selected_header_row_zero = int(header_row_human) - 1
        else:
            st.warning(f"Не удалось прочитать структуру Excel: {excel_info.get('message')}")

        if st.button("Импортировать", type="primary", help="Импортировать текущий Excel в мастер-каталог"):
            conn = get_db()
            try:
                if import_mode == "Ручной выбор листа и строки заголовка":
                    result = import_catalog_from_excel(
                        conn,
                        temp_path,
                        sheet_name=selected_sheet,
                        header_row=selected_header_row_zero,
                        default_supplier_name=default_supplier_name or None,
                        default_supplier_url_template=default_supplier_url_template or None,
                    )
                else:
                    result = import_catalog_from_excel(
                        conn,
                        temp_path,
                        default_supplier_name=default_supplier_name or None,
                        default_supplier_url_template=default_supplier_url_template or None,
                    )
                if auto_match_ozon_after_import and result.batch_id:
                    batch_rows = conn.execute(
                        "SELECT id FROM products WHERE import_batch_id = ? ORDER BY id DESC LIMIT 20000",
                        (result.batch_id,),
                    ).fetchall()
                    batch_ids = [int(r["id"]) for r in batch_rows]
                    if batch_ids:
                        ozon_match_result = bulk_assign_ozon_categories(conn, batch_ids, min_score=OZON_CATEGORY_MIN_SCORE, force=False)
                        if ozon_match_result.get("message"):
                            st.info(str(ozon_match_result["message"]))
                        else:
                            st.caption(
                                f"Ozon автопривязка: обработано {ozon_match_result['processed']}, "
                                f"привязано {ozon_match_result['assigned']}, пропущено {ozon_match_result['skipped']}"
                            )
                batch_df = load_products(conn, limit=1000, import_batch_id=result.batch_id)
                missing_supplier_count = conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM products
                    WHERE import_batch_id = ?
                      AND (supplier_name IS NULL OR TRIM(supplier_name) = '')
                    """,
                    (result.batch_id,),
                ).fetchone()["c"]
            except sqlite3.OperationalError as e:
                conn.close()
                st.error(f"Ошибка базы при импорте: {e}")
                st.info("Попробуй автоопределение или выбери другой лист/строку заголовка. Если ошибка повторяется, база требует миграции.")
                return
            except Exception as e:
                conn.close()
                st.error(f"Ошибка импорта: {e}")
                return
            conn.close()
            st.session_state["last_import_batch_id"] = result.batch_id
            st.success(
                f"Импорт завершён. Всего: {result.imported}, создано: {result.created}, обновлено: {result.updated}, дублей: {len(result.duplicates)}"
            )
            if int(missing_supplier_count or 0) > 0:
                st.warning(
                    f"У {int(missing_supplier_count)} товаров в этой партии не назначен поставщик. "
                    "Назначь его массово во вкладке Каталог."
                )
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Импортировано", int(result.imported))
            c2.metric("Создано", int(result.created))
            c3.metric("Обновлено", int(result.updated))
            c4.metric("Дублей", int(len(result.duplicates)))

            st.markdown("### Последняя загруженная партия")
            if not batch_df.empty:
                st.dataframe(with_ru_columns(batch_df), use_container_width=True, hide_index=True)
            else:
                st.info("В текущей партии нет отображаемых записей. Попробуй ручной выбор листа и строки заголовка.")

            if result.duplicates:
                st.dataframe(with_ru_columns(pd.DataFrame(result.duplicates)), use_container_width=True)


def show_catalog_tab():
    conn = get_db()
    st.subheader("Каталог")
    st.caption("Здесь быстрый контроль по каталогу: поиск, последняя загрузка, статус supplier enrichment и переход в карточку товара.")
    st.info("Фильтр `Категория` учитывает Ozon-эталон в приоритете (ozon_category_path), затем категории из каталога.")
    with st.expander("Инструкция по кнопкам раздела Каталог", expanded=False):
        st.markdown(
            """
- `◀ Назад` / `Вперед ▶`: постраничный переход по каталогу.
- `Скачать текущую страницу Excel`: выгружает только отображаемую страницу.
- `Обновить дубли по текущей выборке`: пересчет дублей по текущей странице.
- `Обогатить поставщика по текущей странице`: парсинг supplier_url батчами с лимитом и таймаутом.
- `Обогатить поставщика по всей выборке фильтра`: запуск enrichment не только по странице, а по всей текущей фильтрации.
- `Автопривязать Ozon категории`: автоподбор Ozon категории.
- `Перепривязать Ozon категории (force)`: автоподбор с перезаписью.
            """
        )

    category_values = list_catalog_categories(conn)
    supplier_values = list_distinct_values(conn, "supplier_name")
    supplier_profile_values = [str(p["supplier_name"]) for p in list_supplier_profiles(conn, only_active=True)]
    supplier_values = sorted(set(supplier_values + supplier_profile_values))
    c1, c2, c3, c4, c5, c6 = st.columns([2, 1, 1, 1, 1, 1])
    with c1:
        search = st.text_input("Поиск", placeholder="Название / артикул / штрихкод")
    with c2:
        category_option = st.selectbox("Категория", options=["Все"] + category_values, index=0)
    with c3:
        supplier_option = st.selectbox("Поставщик", options=["Все"] + supplier_values, index=0)
    with c4:
        page_size = st.selectbox("Размер страницы", options=[50, 100, 200, 500], index=1)
    with c5:
        only_last_batch = st.checkbox("Только последняя загрузка", value=False)
    with c6:
        parse_filter = st.selectbox("Парсинг", ["Все", "Есть supplier_url", "Не парсено", "Ошибка", "Успех"], index=0)
    category = "" if category_option == "Все" else category_option
    supplier = "" if supplier_option == "Все" else supplier_option

    batch_id = st.session_state.get("last_import_batch_id") if only_last_batch else ""
    total_rows = count_products(
        conn,
        search=search,
        category=category,
        supplier=supplier,
        import_batch_id=batch_id or "",
        parse_filter=parse_filter,
    )
    total_pages = max(1, int(math.ceil(total_rows / max(int(page_size), 1))))
    page_options = list(range(1, total_pages + 1))
    current_page = int(st.session_state.get("catalog_page_current", st.session_state.get("catalog_page", 1)))
    if current_page > total_pages:
        current_page = 1
    p1, p2, p3 = st.columns([1, 1, 3])
    with p1:
        page = st.selectbox("Страница", options=page_options, index=page_options.index(current_page), key="catalog_page_widget")
        st.session_state["catalog_page_current"] = int(page)
    with p2:
        st.metric("Всего страниц", total_pages)
    with p3:
        st.caption(f"Всего товаров по фильтру: {total_rows}")
    offset = (int(page) - 1) * int(page_size)
    df = load_products(
        conn,
        search=search,
        category=category,
        supplier=supplier,
        limit=int(page_size),
        offset=int(offset),
        import_batch_id=batch_id or "",
        parse_filter=parse_filter,
    )

    if df.empty:
        st.info("Нет товаров")
        conn.close()
        return

    if batch_id:
        st.caption("Показана только последняя загруженная партия")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Товаров на странице", int(len(df)))
    m2.metric("Товаров всего", int(total_rows))
    m3.metric("С supplier_url", int((df["supplier_url"].fillna("").astype(str).str.strip() != "").sum()) if "supplier_url" in df.columns else 0)
    m4.metric("Парсинг ок", int((df["supplier_parse_status"] == "success").sum()) if "supplier_parse_status" in df.columns else 0)
    m5.metric("С Ozon категорией", int((df["ozon_description_category_id"].notna()).sum()) if "ozon_description_category_id" in df.columns else 0)
    st.caption(f"Показана страница {int(page)} из {total_pages}.")

    st.download_button(
        "Скачать текущую страницу Excel",
        data=export_current_df(df),
        file_name=f"pim_products_page_{int(page)}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    ids = df["id"].tolist()
    selected_id = st.selectbox("Открыть карточку товара", ids, format_func=lambda x: f"ID {x}")
    supplier_candidate_ids = [
        int(row["id"])
        for _, row in df.iterrows()
        if str(row.get("supplier_url") or "").strip()
    ]
    filtered_supplier_candidate_ids = load_product_ids_with_supplier_url(
        conn,
        search=search,
        category=category,
        supplier=supplier,
        import_batch_id=batch_id or "",
        parse_filter=parse_filter,
        limit=None,
        offset=0,
    )
    bc1, bc2, bc3 = st.columns(3)
    with bc1:
        max_bulk_enrich_page = st.number_input(
            "Лимит (текущая страница)",
            min_value=1,
            max_value=1000,
            value=min(20, max(1, len(supplier_candidate_ids))),
            step=1,
            help="Сколько товаров обогащать за один запуск по текущей странице.",
            key="catalog_max_bulk_enrich_page",
        )
    with bc2:
        max_bulk_enrich_filtered = st.number_input(
            "Лимит (вся выборка фильтра)",
            min_value=1,
            max_value=10000,
            value=min(300, max(1, len(filtered_supplier_candidate_ids))),
            step=10,
            help="Сколько товаров обогащать за один запуск по всей выборке фильтров.",
            key="catalog_max_bulk_enrich_filtered",
        )
    with bc3:
        supplier_timeout_seconds = st.number_input(
            "Таймаут supplier_url, сек",
            min_value=2,
            max_value=30,
            value=8,
            step=1,
            help="Максимальное время ожидания ответа от сайта поставщика для одного товара.",
            key="catalog_supplier_timeout_seconds",
        )
    enrich_force = st.checkbox(
        "Перезаписывать значения (force, кроме manual)",
        value=False,
        help="Если включено, enrichment сможет перезаписывать не пустые значения, но manual-поля останутся защищены.",
        key="catalog_enrich_force",
    )
    st.caption(
        f"Кандидатов: страница {len(supplier_candidate_ids)}, вся выборка {len(filtered_supplier_candidate_ids)}. "
        f"Лимиты: страница {int(max_bulk_enrich_page)}, выборка {int(max_bulk_enrich_filtered)}."
    )

    def run_supplier_enrichment_batch(candidate_ids: list[int], run_limit: int, run_label: str) -> None:
        if not candidate_ids:
            st.info(f"Для режима `{run_label}` нет товаров с supplier_url.")
            return
        target_ids = candidate_ids[: int(run_limit)]
        progress = st.progress(0)
        processed = 0
        success = 0
        failed = 0
        used_fallback = 0
        resolved_from_listing = 0
        for i, pid in enumerate(target_ids, start=1):
            current_row = get_product(conn, int(pid))
            current_supplier_url = str(current_row["supplier_url"] or "").strip() if current_row and "supplier_url" in current_row.keys() else ""
            try:
                result = enrich_product_from_supplier(
                    conn,
                    int(pid),
                    force=bool(enrich_force),
                    timeout_seconds=float(supplier_timeout_seconds),
                )
                if result.get("ok"):
                    success += 1
                    if str(result.get("source_type") or "") == "web_search_fallback":
                        used_fallback += 1
                    if str(result.get("source_url") or "").strip() and str(result.get("source_url") or "").strip() != current_supplier_url:
                        resolved_from_listing += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
            processed += 1
            progress.progress(i / len(target_ids))
        skipped_by_limit = max(0, len(candidate_ids) - len(target_ids))
        st.success(
            f"[{run_label}] Обогащение завершено: обработано {processed}, успешно {success}, ошибок {failed}, "
            f"fallback {used_fallback}, listing->product {resolved_from_listing}, отложено по лимиту {skipped_by_limit}."
        )

    b1, b2, b3 = st.columns(3)
    with b1:
        if st.button("Обновить дубли по текущей выборке", help="Пересчитать кандидатов дублей только для товаров на текущей странице"):
            total = 0
            progress = st.progress(0)
            for i, pid in enumerate(ids, start=1):
                refresh_duplicates_for_product(conn, int(pid))
                total += 1
                progress.progress(i / len(ids))
            st.success(f"Проверка дублей завершена: {total} товаров")
    with b2:
        if st.button("Обогатить поставщика по текущей странице", help="Запустить supplier parsing для товаров текущей страницы, где заполнен supplier_url"):
            run_supplier_enrichment_batch(
                candidate_ids=supplier_candidate_ids,
                run_limit=int(max_bulk_enrich_page),
                run_label="Текущая страница",
            )
    with b3:
        if st.button("Обогатить поставщика по всей выборке фильтра", help="Запустить supplier parsing для всех товаров текущих фильтров, где заполнен supplier_url"):
            run_supplier_enrichment_batch(
                candidate_ids=filtered_supplier_candidate_ids,
                run_limit=int(max_bulk_enrich_filtered),
                run_label="Вся выборка фильтра",
            )
    cextra1, cextra2 = st.columns(2)
    with cextra1:
        if st.button("Автопривязать Ozon категории (текущая страница)", help="Автоподбор эталонной Ozon категории для товаров этой страницы"):
            res = bulk_assign_ozon_categories(conn, [int(x) for x in ids], min_score=OZON_CATEGORY_MIN_SCORE, force=False)
            if res.get("message"):
                st.info(str(res["message"]))
            else:
                st.success(f"Ozon автопривязка: обработано {res['processed']}, привязано {res['assigned']}, пропущено {res['skipped']}")
            st.rerun()
    with cextra2:
        if st.button("Перепривязать Ozon категории (force, текущая страница)", help="Повторный подбор Ozon категории с возможной перезаписью текущей привязки"):
            res = bulk_assign_ozon_categories(conn, [int(x) for x in ids], min_score=OZON_CATEGORY_MIN_SCORE, force=True)
            if res.get("message"):
                st.info(str(res["message"]))
            else:
                st.success(f"Ozon force-привязка: обработано {res['processed']}, привязано {res['assigned']}, пропущено {res['skipped']}")
            st.rerun()

    st.dataframe(with_ru_columns(df), use_container_width=True, hide_index=True)

    if selected_id:
        st.session_state["selected_product_id"] = int(selected_id)

    with st.expander("Массовое изменение данных", expanded=False):
        st.caption("Здесь можно массово назначить поставщика, URL, категории и бренд.")
        mm1, mm2, mm3 = st.columns(3)
        with mm1:
            scope = st.selectbox(
                "Область применения",
                options=["Текущая страница", "Вся выборка по фильтру"],
                key="mass_edit_scope",
                help="Текущая страница: только видимые товары. Вся выборка: все товары по текущим фильтрам.",
            )
            only_empty = st.checkbox(
                "Заполнять только пустые поля",
                value=True,
                key="mass_edit_only_empty",
                help="Если включено, заполнит только пустые значения.",
            )
        with mm2:
            mass_supplier = st.selectbox(
                "Поставщик",
                options=[""] + supplier_values,
                index=0,
                key="mass_edit_supplier",
                help="Назначить поставщика выбранным товарам.",
            )
            mass_category = st.selectbox(
                "Категория",
                options=[""] + category_values,
                index=0,
                key="mass_edit_category",
            )
            mass_base_category = st.selectbox(
                "Базовая категория",
                options=[""] + category_values,
                index=0,
                key="mass_edit_base_category",
            )
            mass_subcategory = st.selectbox(
                "Подкатегория",
                options=[""] + category_values,
                index=0,
                key="mass_edit_subcategory",
            )
        with mm3:
            mass_brand = st.text_input("Бренд", value="", key="mass_edit_brand")
            profile_for_url = st.selectbox(
                "Профиль URL поставщика",
                options=[""] + supplier_profile_values,
                index=0,
                key="mass_edit_profile_for_url",
                help="Можно выбрать профиль, чтобы автоматически подставить URL template.",
            )
            profile_map = {p["supplier_name"]: p for p in list_supplier_profiles(conn, only_active=True)}
            mass_supplier_url_template = st.text_input(
                "URL template",
                value=st.session_state.get("mass_edit_supplier_url_template", ""),
                key="mass_edit_supplier_url_template",
                help="Поддержка плейсхолдеров: {article}, {supplier_article}, {code}, {name} и *_q.",
            )
            if st.button("Подставить URL template из профиля", key="mass_edit_apply_profile_template"):
                if profile_for_url and profile_for_url in profile_map:
                    st.session_state["mass_edit_supplier_url_template"] = profile_map[profile_for_url].get("url_template") or ""
                    st.rerun()

        apply_mass = st.button(
            "Применить массовые изменения",
            type="primary",
            help="Применить изменения к выбранной области товаров.",
            key="mass_edit_apply_btn",
        )
        if apply_mass:
            if scope == "Текущая страница":
                target_ids = [int(x) for x in ids]
            else:
                target_ids = load_product_ids(
                    conn,
                    search=search,
                    category=category,
                    supplier=supplier,
                    import_batch_id=batch_id or "",
                    parse_filter=parse_filter,
                    limit=None,
                    offset=0,
                )
            updates = {
                "supplier_name": mass_supplier.strip() if mass_supplier else None,
                "category": mass_category.strip() if mass_category else None,
                "base_category": mass_base_category.strip() if mass_base_category else None,
                "subcategory": mass_subcategory.strip() if mass_subcategory else None,
                "brand": mass_brand.strip() if mass_brand else None,
            }
            result = apply_mass_product_updates(
                conn=conn,
                product_ids=target_ids,
                updates=updates,
                supplier_url_template=mass_supplier_url_template.strip() or None,
                only_empty=bool(only_empty),
            )
            st.success(
                f"Обновлено товаров: {result['updated_products']}, обновлено полей: {result['updated_fields']}"
            )
            st.rerun()

    nav1, nav2, nav3 = st.columns([1, 1, 4])
    with nav1:
        if st.button("◀ Назад", disabled=int(page) <= 1, help="Перейти на предыдущую страницу каталога", key="catalog_nav_prev_bottom"):
            st.session_state["catalog_page_current"] = int(page) - 1
            st.rerun()
    with nav2:
        if st.button("Вперед ▶", disabled=int(page) >= total_pages, help="Перейти на следующую страницу каталога", key="catalog_nav_next_bottom"):
            st.session_state["catalog_page_current"] = int(page) + 1
            st.rerun()
    with nav3:
        st.caption(f"Навигация по каталогу: страница {int(page)} из {int(total_pages)}")

    conn.close()


def enrich_product_from_supplier(
    conn,
    product_id: int,
    force: bool = False,
    timeout_seconds: float = 8.0,
) -> dict:
    product = get_product(conn, product_id)
    if not product:
        return {"ok": False, "message": "Товар не найден"}
    product_row = dict(product)

    supplier_url = (product["supplier_url"] or "").strip() if product["supplier_url"] else ""

    try:
        parse_hints = [
            str(product["article"] or ""),
            str(product["supplier_article"] or ""),
            str(product["name"] or ""),
            str(product["brand"] or ""),
        ]
        parsed: dict = {}
        source_url = supplier_url
        source_type = "supplier_page"
        used_fallback = False
        used_stats_fallback = False
        used_category_defaults = False
        field_source_types: dict[str, str] = {}

        if supplier_url:
            # Support supplier search pages like https://velocitygroup.ru/catalog/?q=
            effective_supplier_url = supplier_url
            low_url = effective_supplier_url.lower()
            if ("?q=" in low_url) and low_url.rstrip().endswith("?q="):
                query_candidate = str(product.get("supplier_article") or product.get("article") or product.get("name") or "").strip()
                if query_candidate:
                    effective_supplier_url = f"{effective_supplier_url}{quote(query_candidate, safe='')}"
            try:
                parsed = parse_supplier_product_page(
                    effective_supplier_url,
                    hints=parse_hints,
                    timeout=float(timeout_seconds),
                    max_hops=1,
                )
                source_url = parsed.get("resolved_url") or effective_supplier_url
            except Exception as parse_error:
                parsed = {}
                source_type = "web_search_fallback"
                source_url = effective_supplier_url
                # Keep flow alive: fallback to internet search below.

        dim_fields = [
            "weight",
            "gross_weight",
            "length",
            "width",
            "height",
            "package_length",
            "package_width",
            "package_height",
        ]
        has_dims = any(parsed.get(k) not in (None, "", 0, 0.0) for k in dim_fields)
        need_fallback = (not has_meaningful_supplier_data(parsed)) or bool(parsed.get("listing_only")) or (not has_dims) or is_dimension_payload_suspicious(parsed) or (not supplier_url)

        if need_fallback:
            preferred_domain = ""
            try:
                preferred_domain = (urlparse(str(source_url or supplier_url)).netloc or "").lower().replace("www.", "")
            except Exception:
                preferred_domain = ""
            fallback_query_parts = [
                str(product["name"] or "").strip(),
                str(product["article"] or "").strip(),
                str(product["supplier_article"] or "").strip(),
                str(product["brand"] or "").strip(),
                "габариты",
            ]
            fallback_query = " ".join([p for p in fallback_query_parts if p])
            fallback = fallback_search_product_data(
                fallback_query,
                timeout=float(timeout_seconds),
                max_results=4,
                hints=parse_hints,
                preferred_domain=preferred_domain or None,
            )
            if fallback and has_meaningful_supplier_data(fallback):
                for key in [
                    "name", "brand", "category", "description", "image_url", "weight", "gross_weight",
                    "length", "width", "height", "package_length", "package_width", "package_height"
                ]:
                    if parsed.get(key) in (None, "", 0, 0.0):
                        parsed[key] = fallback.get(key)
                merged_attrs = dict(parsed.get("attributes") or {})
                for k, v in (fallback.get("attributes") or {}).items():
                    if k not in merged_attrs:
                        merged_attrs[k] = v
                parsed["attributes"] = merged_attrs
                if not parsed.get("image_urls"):
                    parsed["image_urls"] = fallback.get("image_urls") or []
                source_url = fallback.get("fallback_url") or source_url
                source_type = "web_search_fallback"
                used_fallback = True
                for key in [
                    "name",
                    "brand",
                    "category",
                    "description",
                    "image_url",
                    "weight",
                    "gross_weight",
                    "length",
                    "width",
                    "height",
                    "package_length",
                    "package_width",
                    "package_height",
                ]:
                    if fallback.get(key) not in (None, "", 0, 0.0):
                        field_source_types[key] = "web_search_fallback"
            elif not supplier_url:
                source_type = "web_search_fallback"
                source_url = source_url or ""

        category_inferred = infer_category_fields(
            {
                "name": parsed.get("name") or product_row.get("name"),
                "category": parsed.get("category") or product_row.get("category"),
                "base_category": product_row.get("base_category"),
                "subcategory": product_row.get("subcategory"),
            }
        )
        weak_categories = {"товары", "каталог", "продукция", "все товары", "catalog", "products", "shop"}
        if (
            category_inferred.get("category")
            and (
                parsed.get("category") in (None, "")
                or str(parsed.get("category") or "").strip().lower() in weak_categories
            )
        ):
            parsed["category"] = category_inferred.get("category")
            field_source_types["category"] = "name_category_inference"
        if category_inferred.get("base_category"):
            parsed["base_category"] = category_inferred.get("base_category")
            field_source_types["base_category"] = "name_category_inference"
        if category_inferred.get("subcategory"):
            parsed["subcategory"] = category_inferred.get("subcategory")
            field_source_types["subcategory"] = "name_category_inference"
        if category_inferred.get("wheel_diameter_inch") is not None:
            parsed["wheel_diameter_inch"] = category_inferred.get("wheel_diameter_inch")
            field_source_types["wheel_diameter_inch"] = "name_category_inference"

        has_dims_after_web = any(parsed.get(k) not in (None, "", 0, 0.0) for k in dim_fields)
        if (not has_dims_after_web) or is_dimension_payload_suspicious(parsed):
            stats_fallback = infer_dimensions_from_catalog(conn, product_row, min_samples=4)
            if stats_fallback.get("found"):
                for key, value in (stats_fallback.get("values") or {}).items():
                    if parsed.get(key) in (None, "", 0, 0.0):
                        parsed[key] = value
                        field_source_types[key] = "category_stats_fallback"
                used_stats_fallback = True
            else:
                defaults_fallback = infer_dimensions_from_category_defaults(
                    conn,
                    {
                        "category": parsed.get("category") or product_row.get("category"),
                        "base_category": parsed.get("base_category") or product_row.get("base_category"),
                        "subcategory": parsed.get("subcategory") or product_row.get("subcategory"),
                        "wheel_diameter_inch": parsed.get("wheel_diameter_inch") or product_row.get("wheel_diameter_inch"),
                    },
                )
                if defaults_fallback.get("found"):
                    for key, value in (defaults_fallback.get("values") or {}).items():
                        if parsed.get(key) in (None, "", 0, 0.0):
                            parsed[key] = value
                            field_source_types[key] = "category_defaults_fallback"
                    used_category_defaults = True

        if not has_meaningful_supplier_data(parsed):
            has_any_dims_after_fallback = any(parsed.get(k) not in (None, "", 0, 0.0) for k in dim_fields)
            if not (used_stats_fallback or used_category_defaults or has_any_dims_after_fallback):
                conn.execute(
                    """
                    UPDATE products
                    SET supplier_parse_status = ?,
                        supplier_parse_comment = ?,
                        supplier_last_parsed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    ("error", "Не удалось получить полезные данные: supplier_url не содержит карточку товара", product_id),
                )
                conn.commit()
                return {"ok": False, "message": "Не удалось получить полезные данные с supplier_url или через web fallback"}

        updates = {}
        skipped_manual_fields = []
        has_ozon_priority = bool(int(product.get("ozon_description_category_id") or 0) > 0 and int(product.get("ozon_type_id") or 0) > 0)
        fields = [
            "name",
            "brand",
            "category",
            "base_category",
            "subcategory",
            "wheel_diameter_inch",
            "description",
            "image_url",
            "weight",
            "length",
            "width",
            "height",
            "package_length",
            "package_width",
            "package_height",
            "gross_weight",
        ]
        for field in fields:
            new_value = parsed.get(field)
            old_value = product[field] if field in product.keys() else None
            if new_value is None:
                continue
            if has_ozon_priority and field in {"category", "base_category", "subcategory"} and not force:
                skipped_manual_fields.append(f"{field}:ozon_priority")
                continue
            if field == "category" and str(new_value).strip().lower() in weak_categories and not force:
                continue
            if field_is_manual(conn, product_id, field) and not force:
                skipped_manual_fields.append(field)
                continue
            row_source_type = field_source_types.get(field, source_type)
            if not can_overwrite_field(conn, product_id, field, row_source_type, force=force):
                skipped_manual_fields.append(field)
                continue
            if old_value not in (None, "", 0, 0.0) and not force:
                continue
            updates[field] = new_value

        attributes_saved = 0
        skipped_attribute_fields = []
        for attr_name, attr_value in (parsed.get("attributes") or {}).items():
            clean_code = str(attr_name).strip().lower()
            clean_code = "_".join("".join(ch if ch.isalnum() else " " for ch in clean_code).split())
            if not clean_code:
                continue
            attr_field_name = f"attr:{clean_code}"
            if not can_overwrite_field(conn, product_id, attr_field_name, source_type, force=force):
                skipped_attribute_fields.append(clean_code)
                continue
            existing_def = conn.execute(
                "SELECT code FROM attribute_definitions WHERE code = ?",
                (clean_code,),
            ).fetchone()
            if not existing_def:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO attribute_definitions
                    (code, name, data_type, scope, entity_type, is_required, is_multi_value, unit, description, created_at, updated_at)
                    VALUES (?, ?, 'text', 'master', 'product', 0, 0, NULL, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (clean_code, str(attr_name).strip(), f"Автосоздано из source: {source_url}"),
                )
            set_product_attribute_value(conn, product_id, clean_code, str(attr_value))
            save_field_source(
                conn=conn,
                product_id=product_id,
                field_name=attr_field_name,
                source_type=source_type,
                source_value_raw=attr_value,
                source_url=source_url,
                confidence=0.6 if source_type == "supplier_page" else 0.45,
            )
            attributes_saved += 1

        image_urls = [str(x).strip() for x in (parsed.get("image_urls") or []) if str(x).strip()]
        if image_urls:
            set_product_attribute_value(conn, product_id, "main_image", image_urls[0])
            save_field_source(
                conn=conn,
                product_id=product_id,
                field_name="attr:main_image",
                source_type=source_type,
                source_value_raw=image_urls[0],
                source_url=source_url,
                confidence=0.75 if source_type == "supplier_page" else 0.5,
            )
            if len(image_urls) > 1:
                set_product_attribute_value(conn, product_id, "gallery_images", json.dumps(image_urls, ensure_ascii=False))
                save_field_source(
                    conn=conn,
                    product_id=product_id,
                    field_name="attr:gallery_images",
                    source_type=source_type,
                    source_value_raw=json.dumps(image_urls, ensure_ascii=False),
                    source_url=source_url,
                    confidence=0.7 if source_type == "supplier_page" else 0.45,
                )

        parse_comment = f"source={source_type}; url={source_url}"
        if parsed.get("resolved_from_listing"):
            parse_comment += "; listing->product resolved"
        if used_fallback:
            parse_comment += "; web_fallback=1"
        if used_stats_fallback:
            parse_comment += "; category_stats_fallback=1"
        if used_category_defaults:
            parse_comment += "; category_defaults_fallback=1"

        if updates:
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            params = list(updates.values()) + ["success", parse_comment[:500], product_id]
            conn.execute(
                f"""
                UPDATE products
                SET {set_clause},
                    supplier_parse_status = ?,
                    supplier_parse_comment = ?,
                    supplier_last_parsed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                params,
            )
            for field_name, value in updates.items():
                row_source_type = field_source_types.get(field_name, source_type)
                save_field_source(
                    conn=conn,
                    product_id=product_id,
                    field_name=field_name,
                    source_type=row_source_type,
                    source_value_raw=value,
                    source_url=source_url,
                    confidence=(
                        0.72
                        if row_source_type == "supplier_page"
                        else 0.58
                        if row_source_type == "web_search_fallback"
                        else 0.38
                        if row_source_type == "category_stats_fallback"
                        else 0.33
                        if row_source_type == "category_defaults_fallback"
                        else 0.65
                        if row_source_type == "name_category_inference"
                        else 0.45
                    ),
                )
        else:
            conn.execute(
                """
                UPDATE products
                SET supplier_parse_status = ?,
                    supplier_parse_comment = ?,
                    supplier_last_parsed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                ("success", f"Новых данных для записи не найдено; {parse_comment}"[:500], product_id),
            )

        conn.commit()
        ozon_match = bulk_assign_ozon_categories(conn, [int(product_id)], min_score=OZON_CATEGORY_MIN_SCORE, force=False)
        skipped_msg = f", пропущено ручных полей: {len(skipped_manual_fields)}" if skipped_manual_fields else ""
        skipped_attr_msg = f", пропущено атрибутов по приоритету: {len(skipped_attribute_fields)}" if skipped_attribute_fields else ""
        ozon_msg = f", Ozon category match: {ozon_match.get('assigned', 0)}" if ozon_match.get("processed") else ""
        fallback_msg = ", использован web fallback" if used_fallback else ""
        stats_msg = ", использован category-stats fallback" if used_stats_fallback else ""
        defaults_msg = ", использованы category defaults" if used_category_defaults else ""
        return {
            "ok": True,
            "message": f"Обогащение завершено, обновлено полей: {len(updates)}, атрибутов сохранено: {attributes_saved}{fallback_msg}{stats_msg}{defaults_msg}{skipped_msg}{skipped_attr_msg}{ozon_msg}",
            "updates": updates,
            "attributes": parsed.get("attributes", {}),
            "image_urls": parsed.get("image_urls", []),
            "skipped_manual_fields": skipped_manual_fields,
            "skipped_attribute_fields": skipped_attribute_fields,
            "source_url": source_url,
            "source_type": source_type,
        }
    except Exception as e:
        conn.execute(
            """
            UPDATE products
            SET supplier_parse_status = ?,
                supplier_parse_comment = ?,
                supplier_last_parsed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            ("error", str(e)[:500], product_id),
        )
        conn.commit()
        return {"ok": False, "message": str(e)}


def show_product_tab():
    conn = get_db()
    st.subheader("Карточка товара")
    st.caption("Мастер-карточка должна быть единым источником правды. Здесь можно вручную поправить данные или обогатить их с сайта поставщика.")
    st.markdown("### Выбор товара для редактирования")
    fs1, fs2, fs3, fs4 = st.columns([3, 2, 2, 2])
    with fs1:
        card_search = st.text_input(
            "Поиск товара",
            value=st.session_state.get("card_product_search", ""),
            placeholder="Артикул или наименование товара",
            key="card_product_search",
        )
    ozon_category_values, ozon_subcategory_values = list_ozon_category_filters(conn)
    supplier_values = list_distinct_values(conn, "supplier_name")
    with fs2:
        card_category = st.selectbox(
            "Категория Ozon",
            options=["Все"] + ozon_category_values,
            index=0,
            key="card_product_category_filter",
        )
    with fs3:
        card_subcategory = st.selectbox(
            "Подкатегория Ozon",
            options=["Все"] + ozon_subcategory_values,
            index=0,
            key="card_product_subcategory_filter",
        )
    with fs4:
        card_supplier = st.selectbox(
            "Поставщик",
            options=["Все"] + supplier_values,
            index=0,
            key="card_product_supplier_filter",
        )

    filtered_products = find_products_for_card(
        conn,
        search=card_search or "",
        ozon_category=card_category or "",
        ozon_subcategory=card_subcategory or "",
        supplier=card_supplier or "",
        limit=5000,
    )
    if not filtered_products:
        st.warning("По фильтрам не найдено товаров. Измени фильтр или очисти поиск.")
        conn.close()
        return

    product_options = [int(r["id"]) for r in filtered_products]
    current_product_id = int(st.session_state.get("selected_product_id") or 0)
    default_product_id = current_product_id if current_product_id in product_options else int(product_options[0])
    selected_product_id = st.selectbox(
        "Товар",
        options=product_options,
        index=product_options.index(default_product_id),
        format_func=lambda x: next(
            (
                f"ID {int(row['id'])} | {str(row.get('article') or row.get('supplier_article') or '-')} | "
                f"{str(row.get('name') or '-')} | "
                f"{str(row.get('ozon_category') or row.get('category') or row.get('base_category') or '-')} / "
                f"{str(row.get('ozon_subcategory') or row.get('subcategory') or '-')} | "
                f"{str(row.get('supplier_name') or '-')}"
                for row in filtered_products
                if int(row["id"]) == int(x)
            ),
            f"ID {x}",
        ),
        key="card_selected_product_id",
    )
    st.session_state["selected_product_id"] = int(selected_product_id)
    product_id = int(selected_product_id)
    product = get_product(conn, product_id)

    if not product:
        st.warning("Товар не найден")
        conn.close()
        return

    st.subheader(f"Редактирование: товар #{product['id']}")
    with st.expander("Инструкция по кнопкам раздела Карточка", expanded=False):
        st.markdown(
            """
- `Поиск товара (артикул/наименование) / Категория Ozon / Подкатегория Ozon / Поставщик`: фильтр выбора товара для редактирования.
- `Спарсить поставщика`: аккуратное обогащение карточки с сайта поставщика.
- `Перезаполнить из поставщика`: force-перезапись полей из supplier page.
- `Подобрать Ozon категорию`: автоподбор эталонной категории Ozon.
- `Перепривязать Ozon категорию (force)`: повторный подбор Ozon категории с перезаписью.
- `Сохранить карточку`: сохранение ручных изменений.
            """
        )
    category_values = list_catalog_categories(conn)
    supplier_profiles = list_supplier_profiles(conn, only_active=True)
    supplier_profile_map = {str(p["supplier_name"]): p for p in supplier_profiles}

    with st.expander("Источник данных поставщика (URL template / профиль)", expanded=False):
        sp1, sp2, sp3 = st.columns([2, 3, 1])
        with sp1:
            profile_name = st.selectbox(
                "Профиль поставщика",
                options=[""] + sorted(supplier_profile_map.keys()),
                index=(sorted(supplier_profile_map.keys()).index(str(product["supplier_name"])) + 1) if (product["supplier_name"] and str(product["supplier_name"]) in supplier_profile_map) else 0,
                key=f"product_supplier_profile_{int(product_id)}",
            )
        with sp2:
            selected_profile_template = (
                supplier_profile_map.get(profile_name, {}).get("url_template")
                if profile_name
                else ""
            )
            supplier_url_template = st.text_input(
                "URL template для товара",
                value=selected_profile_template or "",
                placeholder="https://site.ru/catalog/?q={supplier_article_q}",
                key=f"product_supplier_url_template_{int(product_id)}",
                help="Поддерживаются плейсхолдеры: {article}, {supplier_article}, {name}, {code} и *_q.",
            )
        with sp3:
            if st.button("Подставить URL", key=f"product_apply_supplier_url_{int(product_id)}"):
                render_payload = {
                    "article": product["article"],
                    "supplier_article": product["supplier_article"],
                    "name": product["name"],
                    "category": product["category"],
                    "code": product["supplier_article"] or product["article"],
                }
                generated_url = render_supplier_url(supplier_url_template, render_payload) if supplier_url_template else None
                if generated_url:
                    save_product(
                        conn,
                        int(product_id),
                        {
                            "supplier_name": profile_name or product["supplier_name"] or None,
                            "supplier_url": generated_url,
                        },
                    )
                    save_field_source(
                        conn=conn,
                        product_id=int(product_id),
                        field_name="supplier_url",
                        source_type="manual",
                        source_value_raw=generated_url,
                        source_url=None,
                        confidence=1.0,
                        is_manual=True,
                    )
                    st.success("URL поставщика подставлен из шаблона профиля.")
                    st.rerun()
                else:
                    st.warning("Не удалось собрать URL. Проверь шаблон и поля товара.")

    top1, top2, top3, top4 = st.columns(4)
    top1.metric("Артикул", product["article"] or "-")
    top2.metric("Бренд", product["brand"] or "-")
    top3.metric("Категория", product["ozon_category_path"] or product["base_category"] or product["category"] or "-")
    top4.metric("Поставщик", product["supplier_name"] or "-")

    ctop1, ctop2 = st.columns([1, 1])
    with ctop1:
        if st.button("Спарсить поставщика", type="primary", help="Обогатить карточку с сайта поставщика без жесткой перезаписи ручных значений"):
            result = enrich_product_from_supplier(conn, int(product_id), force=False)
            if result["ok"]:
                st.success(result["message"])
                if result.get("updates"):
                    st.json(result["updates"])
                st.rerun()
            else:
                st.error(result["message"])
    with ctop2:
        if st.button("Перезаполнить из поставщика", help="Жесткая перезапись значений из supplier page (force-режим)"):
            result = enrich_product_from_supplier(conn, int(product_id), force=True)
            if result["ok"]:
                st.success(result["message"])
                if result.get("updates"):
                    st.json(result["updates"])
                st.rerun()
            else:
                st.error(result["message"])
    ctop3, ctop4 = st.columns([1, 1])
    with ctop3:
        if st.button("Подобрать Ozon категорию", help="Подобрать эталонную Ozon категорию автоматически"):
            res = bulk_assign_ozon_categories(conn, [int(product_id)], min_score=OZON_CATEGORY_MIN_SCORE, force=False)
            if res.get("message"):
                st.info(str(res["message"]))
            else:
                st.success(f"Ozon автопривязка: обработано {res['processed']}, привязано {res['assigned']}, пропущено {res['skipped']}")
            st.rerun()
    with ctop4:
        if st.button("Перепривязать Ozon категорию (force)", help="Повторно назначить Ozon категорию с перезаписью текущей привязки"):
            res = bulk_assign_ozon_categories(conn, [int(product_id)], min_score=OZON_CATEGORY_MIN_SCORE, force=True)
            if res.get("message"):
                st.info(str(res["message"]))
            else:
                st.success(f"Ozon force-привязка: обработано {res['processed']}, привязано {res['assigned']}, пропущено {res['skipped']}")
            st.rerun()

    parse_status = product["supplier_parse_status"] if "supplier_parse_status" in product.keys() else None
    parse_comment = product["supplier_parse_comment"] if "supplier_parse_comment" in product.keys() else None
    parsed_at = product["supplier_last_parsed_at"] if "supplier_last_parsed_at" in product.keys() else None
    if parse_status == "success":
        st.success(f"Парсинг поставщика прошёл успешно. Последний запуск: {parsed_at or '-'}")
    elif parse_status == "error":
        st.error(f"Есть ошибка парсинга поставщика. Последний запуск: {parsed_at or '-'}")
    elif parsed_at:
        st.info(f"Парсинг поставщика запускался. Последний запуск: {parsed_at}")
    if parse_comment:
        st.caption(f"Комментарий: {parse_comment}")

    ozon_desc_id = int(product["ozon_description_category_id"] or 0)
    ozon_type_id = int(product["ozon_type_id"] or 0)
    if ozon_desc_id > 0 and ozon_type_id > 0:
        st.markdown("### Ozon-атрибуты выбранной категории")
        oz1, oz2 = st.columns([1, 1])
        with oz1:
            if st.button("Подтянуть Ozon-атрибуты категории в справочник", key=f"product_import_ozon_attrs_{int(product_id)}"):
                import_result = import_cached_attributes_to_pim(
                    conn,
                    description_category_id=ozon_desc_id,
                    type_id=ozon_type_id,
                )
                st.success(
                    f"Импорт в справочник выполнен: {int(import_result.get('imported') or 0)} атрибутов, "
                    f"обязательных: {int(import_result.get('required') or 0)}."
                )
                st.rerun()
        with oz2:
            if st.button("Синхронизировать атрибуты категории из Ozon API", key=f"product_sync_ozon_attrs_{int(product_id)}"):
                st.info("Для синхронизации по API используй вкладку Ozon (там задаются Ozon Client ID / API Key для сессии).")

        category_code = f"ozon:{ozon_desc_id}:{ozon_type_id}"
        ozon_req_rows = conn.execute(
            """
            SELECT
                car.attribute_code,
                car.is_required,
                ad.name AS attribute_name,
                ad.data_type,
                ad.unit,
                COALESCE(pav.value_text, CAST(pav.value_number AS TEXT), CAST(pav.value_boolean AS TEXT), pav.value_json) AS current_value
            FROM channel_attribute_requirements car
            JOIN attribute_definitions ad
              ON ad.code = car.attribute_code
            LEFT JOIN product_attribute_values pav
              ON pav.product_id = ?
             AND pav.attribute_code = car.attribute_code
             AND IFNULL(pav.channel_code, '') = ''
            WHERE car.channel_code = 'ozon'
              AND car.category_code = ?
            ORDER BY car.is_required DESC, ad.name
            """,
            (int(product_id), category_code),
        ).fetchall()
        if ozon_req_rows:
            req_df = pd.DataFrame([dict(r) for r in ozon_req_rows])
            req_df["attribute_code_ru"] = req_df["attribute_code"].map(humanize_attribute_code)
            req_df["filled"] = req_df["current_value"].map(lambda x: 0 if x in (None, "", "None") else 1)
            r1, r2, r3 = st.columns(3)
            r1.metric("Всего атрибутов категории", int(len(req_df)))
            r2.metric("Обязательных", int(req_df["is_required"].fillna(0).astype(int).sum()))
            r3.metric("Заполнено значениями", int(req_df["filled"].sum()))
            st.dataframe(
                with_ru_columns(
                    req_df[
                        [
                            "attribute_code",
                            "attribute_code_ru",
                            "attribute_name",
                            "data_type",
                            "unit",
                            "is_required",
                            "current_value",
                        ]
                    ],
                    extra_map={
                        "attribute_code_ru": "Код атрибута (рус.)",
                        "attribute_name": "Название атрибута",
                        "current_value": "Текущее значение",
                    },
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Для этой Ozon-категории в PIM пока нет импортированных attribute requirements.")

    st.markdown("### Атрибуты для заполнения (Ozon и клиентские шаблоны)")
    channel_codes = list_channel_codes(conn)
    if channel_codes:
        product_ozon_scope = f"ozon:{ozon_desc_id}:{ozon_type_id}" if ozon_desc_id > 0 and ozon_type_id > 0 else ""
        default_channel = str(st.session_state.get("card_attr_channel") or "")
        if default_channel not in channel_codes:
            if "onlinetrade" in channel_codes:
                default_channel = "onlinetrade"
            elif product_ozon_scope and "ozon" in channel_codes:
                default_channel = "ozon"
            else:
                default_channel = str(channel_codes[0])
        ch1, ch2, ch3 = st.columns([2, 2, 1])
        with ch1:
            selected_channel = st.selectbox(
                "Канал атрибутов",
                options=channel_codes,
                index=channel_codes.index(default_channel),
                key=f"card_attr_channel_{int(product_id)}",
            )
        st.session_state["card_attr_channel"] = selected_channel

        category_scopes = list_channel_category_codes(conn, selected_channel)
        if selected_channel == "ozon" and product_ozon_scope and product_ozon_scope not in category_scopes:
            category_scopes = [product_ozon_scope] + category_scopes
        category_scopes = list(dict.fromkeys(category_scopes))
        scope_options = [""] + category_scopes
        scope_labels = _build_ozon_scope_labels(conn)
        if selected_channel != "ozon":
            for code in category_scopes:
                scope_labels.setdefault(code, str(code))

        default_scope = str(st.session_state.get("card_attr_category_scope") or "")
        if default_scope not in scope_options:
            default_scope = ""
        if selected_channel == "ozon" and product_ozon_scope:
            default_scope = product_ozon_scope
        elif not default_scope:
            product_scope_candidates = [
                str(product["subcategory"] or "").strip(),
                str(product["category"] or "").strip(),
                str(product["base_category"] or "").strip(),
            ]
            options_lc = {str(opt).strip().lower(): opt for opt in scope_options if str(opt).strip()}
            for cand in product_scope_candidates:
                if cand and cand.lower() in options_lc:
                    default_scope = str(options_lc[cand.lower()])
                    break
        with ch2:
            selected_scope = st.selectbox(
                "Категория атрибутов",
                options=scope_options,
                index=(scope_options.index(default_scope) if default_scope in scope_options else 0),
                format_func=lambda x: "Все категории канала" if x == "" else scope_labels.get(x, str(x)),
                key=f"card_attr_scope_{int(product_id)}_{selected_channel}",
            )
        st.session_state["card_attr_category_scope"] = selected_scope
        with ch3:
            save_as_channel = st.checkbox(
                "Сохранять как канал",
                value=False,
                key=f"card_attr_save_as_channel_{int(product_id)}_{selected_channel}_{selected_scope or 'all'}",
                help="Если выключено, значения сохраняются в мастер-карточку.",
            )

        req_rows = list_channel_requirements(
            conn,
            channel_code=selected_channel,
            category_code=selected_scope or None,
        )
        rule_rows = list_channel_mapping_rules(
            conn,
            channel_code=selected_channel,
            category_code=selected_scope or None,
        )
        required_map = {str(r["attribute_code"]): int(r.get("is_required") or 0) for r in req_rows}
        attribute_codes = set(required_map.keys())
        for rule in rule_rows:
            if str(rule.get("source_type") or "") == "attribute" and rule.get("source_name"):
                attribute_codes.add(str(rule["source_name"]))

        defs = list_attribute_definitions(conn)
        defs_map = {str(d["code"]): d for d in defs}
        type_map_ru = {"text": "Текст", "number": "Число", "boolean": "Да/Нет", "json": "JSON"}

        attr_values = get_product_attribute_values(conn, int(product_id), channel_code=selected_channel)
        value_by_code: dict[str, dict] = {}
        for row in attr_values:
            code = str(row.get("attribute_code") or "")
            if not code:
                continue
            priority = 2 if str(row.get("channel_code") or "").strip() == str(selected_channel).strip() else 1
            existing = value_by_code.get(code)
            if (not existing) or priority > int(existing.get("_priority") or 0):
                value_by_code[code] = {"value": row.get("value"), "_priority": priority}

        editor_rows = []
        for code in sorted(attribute_codes, key=lambda x: (0 if required_map.get(x, 0) else 1, humanize_attribute_code(x).lower())):
            attr_def = defs_map.get(code, {})
            current_value = value_by_code.get(code, {}).get("value")
            current_text = ""
            if current_value is not None:
                if isinstance(current_value, (dict, list)):
                    current_text = json.dumps(current_value, ensure_ascii=False)
                else:
                    current_text = str(current_value)
            editor_rows.append(
                {
                    "attribute_code": code,
                    "attribute_code_ru": humanize_attribute_code(code),
                    "name": str(attr_def.get("name") or humanize_attribute_code(code)),
                    "data_type": type_map_ru.get(str(attr_def.get("data_type") or "text"), str(attr_def.get("data_type") or "text")),
                    "is_required": int(required_map.get(code, 0)),
                    "current_value": current_text,
                    "new_value": current_text,
                }
            )

        if editor_rows:
            ed_df = pd.DataFrame(editor_rows)
            st.caption("Здесь собраны атрибуты категории канала и атрибуты из mapping rules (source_type=attribute).")
            edited_df = st.data_editor(
                ed_df,
                use_container_width=True,
                hide_index=True,
                key=f"card_attr_editor_{int(product_id)}_{selected_channel}_{selected_scope or 'all'}",
                disabled=["attribute_code", "attribute_code_ru", "name", "data_type", "is_required", "current_value"],
                column_config={
                    "attribute_code_ru": st.column_config.TextColumn("Код атрибута (рус.)"),
                    "attribute_code": st.column_config.TextColumn("Технический код"),
                    "name": st.column_config.TextColumn("Название"),
                    "data_type": st.column_config.TextColumn("Тип данных"),
                    "is_required": st.column_config.NumberColumn("Обязательный", format="%d"),
                    "current_value": st.column_config.TextColumn("Текущее значение"),
                    "new_value": st.column_config.TextColumn("Новое значение"),
                },
            )
            if st.button("Сохранить атрибуты этого блока", type="primary", key=f"card_attr_save_btn_{int(product_id)}_{selected_channel}_{selected_scope or 'all'}"):
                updated_count = 0
                for _, row in edited_df.iterrows():
                    code = str(row.get("attribute_code") or "").strip()
                    if not code:
                        continue
                    old_text = str(row.get("current_value") or "").strip()
                    new_text = str(row.get("new_value") or "").strip()
                    if old_text == new_text:
                        continue
                    value_to_save = new_text if new_text else None
                    try:
                        set_product_attribute_value(
                            conn=conn,
                            product_id=int(product_id),
                            attribute_code=code,
                            value=value_to_save,
                            channel_code=(selected_channel if save_as_channel else None),
                        )
                        save_field_source(
                            conn=conn,
                            product_id=int(product_id),
                            field_name=code,
                            source_type="manual",
                            source_value_raw=value_to_save,
                            source_url=None,
                            confidence=1.0,
                            is_manual=True,
                        )
                        updated_count += 1
                    except Exception as e:
                        st.error(f"Не удалось сохранить атрибут `{code}`: {e}")
                st.success(f"Сохранено атрибутов: {updated_count}")
                st.rerun()
        else:
            st.info("Для выбранного канала и категории пока нет атрибутов. Загрузите клиентский шаблон и сохраните mapping rules.")
    else:
        st.info("Каналы пока не настроены. Добавь канал во вкладке Каналы, затем загрузи клиентский шаблон.")

    with st.form("product_form"):
        c1, c2, c3 = st.columns(3)

        with c1:
            article = st.text_input("Артикул", value=product["article"] or "")
            internal_article = st.text_input("Внутренний артикул", value=product["internal_article"] or "")
            supplier_article = st.text_input("Артикул поставщика", value=product["supplier_article"] or "")
            name = st.text_input("Название", value=product["name"] or "")
            brand = st.text_input("Бренд", value=product["brand"] or "")
            supplier_options = [""] + sorted(set(supplier_values + ([str(product["supplier_name"])] if product["supplier_name"] else [])))
            supplier_default = str(product["supplier_name"] or "")
            supplier_idx = supplier_options.index(supplier_default) if supplier_default in supplier_options else 0
            supplier_name = st.selectbox("Поставщик (из базы)", options=supplier_options, index=supplier_idx)
            barcode = st.text_input("Штрихкод", value=product["barcode"] or "")
            barcode_source = st.text_input("Источник штрихкода", value=product["barcode_source"] or "")

        with c2:
            category_options = [""] + sorted(set(category_values + ([str(product["category"])] if product["category"] else [])))
            category_default = str(product["category"] or "")
            category_idx = category_options.index(category_default) if category_default in category_options else 0
            category = st.selectbox("Категория (приоритет Ozon)", options=category_options, index=category_idx)

            base_options = [""] + sorted(set(category_values + ([str(product["base_category"])] if product["base_category"] else [])))
            base_default = str(product["base_category"] or "")
            base_idx = base_options.index(base_default) if base_default in base_options else 0
            base_category = st.selectbox("Базовая категория (приоритет Ozon)", options=base_options, index=base_idx)

            sub_options = [""] + sorted(set(category_values + ([str(product["subcategory"])] if product["subcategory"] else [])))
            sub_default = str(product["subcategory"] or "")
            sub_idx = sub_options.index(sub_default) if sub_default in sub_options else 0
            subcategory = st.selectbox("Подкатегория (приоритет Ozon)", options=sub_options, index=sub_idx)
            wheel_diameter_inch = st.number_input(
                "Диаметр колеса, inch",
                value=float(product["wheel_diameter_inch"] or 0.0),
                step=0.5,
            )
            uom = st.text_input("Ед. изм.", value=product["uom"] or "")
            supplier_url = st.text_input("URL поставщика", value=product["supplier_url"] or "")
            ozon_description_category_id = st.number_input(
                "Ozon description_category_id",
                min_value=0,
                value=int(product["ozon_description_category_id"] or 0),
                step=1,
            )
            ozon_type_id = st.number_input(
                "Ozon type_id",
                min_value=0,
                value=int(product["ozon_type_id"] or 0),
                step=1,
            )
            ozon_category_path = st.text_input("Ozon категория (path)", value=product["ozon_category_path"] or "")
            ozon_category_confidence = st.number_input(
                "Уверенность Ozon категории (0..1)",
                min_value=0.0,
                max_value=1.0,
                value=float(product["ozon_category_confidence"] or 0.0),
                step=0.01,
            )
            tnved_code = st.text_input("ТН ВЭД", value=product["tnved_code"] or "")

        with c3:
            weight = st.number_input("Вес, кг", value=float(product["weight"] or 0.0), step=0.1)
            length = st.number_input("Длина, см", value=float(product["length"] or 0.0), step=1.0)
            width = st.number_input("Ширина, см", value=float(product["width"] or 0.0), step=1.0)
            height = st.number_input("Высота, см", value=float(product["height"] or 0.0), step=1.0)
            package_length = st.number_input("Длина упаковки", value=float(product["package_length"] or 0.0), step=1.0)
            package_width = st.number_input("Ширина упаковки", value=float(product["package_width"] or 0.0), step=1.0)
            package_height = st.number_input("Высота упаковки", value=float(product["package_height"] or 0.0), step=1.0)
            gross_weight = st.number_input("Вес брутто", value=float(product["gross_weight"] or 0.0), step=0.1)

        image_url = st.text_input("Фото", value=product["image_url"] or "")
        description = st.text_area("Описание", value=product["description"] or "", height=180)

        submitted = st.form_submit_button("Сохранить карточку", type="primary")

        if submitted:
            payload = {
                "article": article or None,
                "internal_article": internal_article or None,
                "supplier_article": supplier_article or None,
                "name": name or None,
                "brand": brand or None,
                "supplier_name": supplier_name or None,
                "barcode": barcode or None,
                "barcode_source": barcode_source or None,
                "category": category or None,
                "base_category": base_category or None,
                "subcategory": subcategory or None,
                "wheel_diameter_inch": wheel_diameter_inch or None,
                "supplier_url": supplier_url or None,
                "ozon_description_category_id": int(ozon_description_category_id) if int(ozon_description_category_id) > 0 else None,
                "ozon_type_id": int(ozon_type_id) if int(ozon_type_id) > 0 else None,
                "ozon_category_path": ozon_category_path or None,
                "ozon_category_confidence": float(ozon_category_confidence) if float(ozon_category_confidence) > 0 else None,
                "uom": uom or None,
                "weight": weight or None,
                "length": length or None,
                "width": width or None,
                "height": height or None,
                "package_length": package_length or None,
                "package_width": package_width or None,
                "package_height": package_height or None,
                "gross_weight": gross_weight or None,
                "image_url": image_url or None,
                "description": description or None,
                "tnved_code": tnved_code or None,
            }
            save_product(conn, int(product_id), payload)
            refresh_duplicates_for_product(conn, int(product_id))
            st.success("Сохранено")
            st.rerun()

    st.markdown("### Источники ключевых полей")
    st.caption("Важно видеть, что пришло руками, что от поставщика, и какие значения ещё слабые по источнику.")
    key_fields = ["name", "brand", "description", "weight", "length", "width", "height", "package_length", "package_width", "package_height", "gross_weight", "image_url"]
    source_summary = []
    for field_name in key_fields:
        src = get_latest_field_source(conn, int(product_id), field_name)
        source_summary.append({
            "field_name": field_name,
            "source_type": src.get("source_type") if src else None,
            "is_manual": bool(src.get("is_manual")) if src else False,
            "confidence": src.get("confidence") if src else None,
            "created_at": src.get("created_at") if src else None,
        })
    st.dataframe(with_ru_columns(pd.DataFrame(source_summary)), use_container_width=True, hide_index=True)

    st.markdown("### Все источники данных")
    sources = get_field_sources(conn, int(product_id))
    if sources:
        src_df = pd.DataFrame(sources)
        if not src_df.empty and "field_name" in src_df.columns:
            src_df["field_name_ru"] = src_df["field_name"].map(humanize_attribute_code)
        st.dataframe(
            with_ru_columns(
                src_df,
                extra_map={"field_name_ru": "Поле (рус.)"},
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("Источники данных пока не записаны")

    conn.close()


def show_attributes_tab():
    conn = get_db()
    product_id = st.session_state.get("selected_product_id")
    st.caption("Справочник атрибутов можно фильтровать по категории Ozon, чтобы работать с большим количеством полей без шума.")

    scope_labels = _build_ozon_scope_labels(conn)
    category_scope_options = ["Все"] + sorted(scope_labels.keys())
    if product_id:
        selected_product = conn.execute(
            """
            SELECT ozon_description_category_id, ozon_type_id
            FROM products
            WHERE id = ?
            LIMIT 1
            """,
            (int(product_id),),
        ).fetchone()
        if selected_product:
            scoped_code = None
            try:
                desc_id = int(selected_product["ozon_description_category_id"] or 0)
                type_id = int(selected_product["ozon_type_id"] or 0)
                if desc_id > 0 and type_id > 0:
                    scoped_code = f"ozon:{desc_id}:{type_id}"
            except Exception:
                scoped_code = None
            marker = st.session_state.get("attrs_scope_product_marker")
            if scoped_code and scoped_code in category_scope_options and marker != int(product_id):
                current_scope = st.session_state.get("attrs_category_scope")
                if current_scope in (None, "", "Все"):
                    st.session_state["attrs_category_scope"] = scoped_code
            st.session_state["attrs_scope_product_marker"] = int(product_id)
    current_scope_value = st.session_state.get("attrs_category_scope")
    if current_scope_value not in category_scope_options:
        st.session_state["attrs_category_scope"] = "Все"
        current_scope_value = "Все"
    scope_index = category_scope_options.index(current_scope_value) if current_scope_value in category_scope_options else 0

    f1, f2, f3 = st.columns([2, 2, 2])
    with f1:
        attr_search = st.text_input("Поиск атрибута", value="", placeholder="Название, код, описание", key="attrs_search")
    with f2:
        attr_source_filter = st.selectbox("Источник атрибута", options=["Все", "Ozon", "Кастомные"], index=0, key="attrs_source_filter")
    with f3:
        category_scope = st.selectbox(
            "Категория (область)",
            options=category_scope_options,
            index=scope_index,
            key="attrs_category_scope",
            format_func=lambda x: "Все" if x == "Все" else scope_labels.get(x, x),
        )

    required_map: dict[str, int] = {}
    if category_scope != "Все":
        req_rows = conn.execute(
            """
            SELECT attribute_code, is_required
            FROM channel_attribute_requirements
            WHERE channel_code = 'ozon'
              AND category_code = ?
            """,
            (str(category_scope),),
        ).fetchall()
        required_map = {str(r["attribute_code"]): int(r["is_required"] or 0) for r in req_rows}

    left, right = st.columns([1, 1])

    with left:
        st.subheader("Справочник атрибутов")
        defs = list_attribute_definitions(conn)
        defs_df = pd.DataFrame(defs) if defs else pd.DataFrame()
        if not defs_df.empty:
            if attr_source_filter == "Ozon":
                defs_df = defs_df[defs_df["code"].astype(str).str.startswith("ozon_attr_")]
            elif attr_source_filter == "Кастомные":
                defs_df = defs_df[~defs_df["code"].astype(str).str.startswith("ozon_attr_")]

            if category_scope != "Все":
                allowed_codes = set(required_map.keys())
                defs_df = defs_df[defs_df["code"].astype(str).isin(allowed_codes)]
                defs_df["is_required_for_category"] = defs_df["code"].map(lambda c: int(required_map.get(str(c), 0)))

            if attr_search:
                q = str(attr_search).strip().lower()
                mask = (
                    defs_df["code"].astype(str).str.lower().str.contains(q, na=False)
                    | defs_df["name"].astype(str).str.lower().str.contains(q, na=False)
                    | defs_df["description"].astype(str).str.lower().str.contains(q, na=False)
                )
                defs_df = defs_df[mask]

            data_type_ru = {"text": "Текст", "number": "Число", "boolean": "Да/Нет", "json": "JSON"}
            scope_ru = {"master": "Мастер", "channel": "Канал"}
            if "data_type" in defs_df.columns:
                defs_df["data_type"] = defs_df["data_type"].map(lambda x: data_type_ru.get(str(x), x))
            if "scope" in defs_df.columns:
                defs_df["scope"] = defs_df["scope"].map(lambda x: scope_ru.get(str(x), x))
            entity_type_ru = {"product": "Товар", "channel": "Канал", "category": "Категория"}
            if "entity_type" in defs_df.columns:
                defs_df["entity_type"] = defs_df["entity_type"].map(lambda x: entity_type_ru.get(str(x), x))

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Атрибутов", int(len(defs_df)))
            m2.metric("Атрибутов Ozon (уникальных)", int(defs_df["code"].astype(str).str.startswith("ozon_attr_").sum()))
            required_count = int(defs_df["is_required_for_category"].fillna(0).astype(int).sum()) if "is_required_for_category" in defs_df.columns else 0
            m3.metric("Обязательных (по категории)", required_count)
            total_requirements = conn.execute(
                "SELECT COUNT(*) FROM channel_attribute_requirements WHERE channel_code = 'ozon'"
            ).fetchone()[0]
            m4.metric("Требований Ozon (категорийных)", int(total_requirements or 0))
            if category_scope == "Все":
                st.caption("Чтобы увидеть обязательные атрибуты конкретной Ozon-категории, выбери `Категория (область)`.")
            custom_count = int(len(defs_df) - int(defs_df["code"].astype(str).str.startswith("ozon_attr_").sum()))
            if custom_count > 0:
                st.caption(f"Дополнительно в справочнике есть {custom_count} кастомных атрибутов (вне Ozon).")
            defs_df = defs_df.copy()
            defs_df["code_ru"] = defs_df["code"].map(humanize_attribute_code)
            st.dataframe(
                with_ru_columns(
                    defs_df,
                    extra_map={
                        "code_ru": "Код атрибута (рус.)",
                    },
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Справочник атрибутов пока пуст.")

        with st.form("new_attribute_def"):
            code = st.text_input("Код атрибута")
            name = st.text_input("Название атрибута")
            data_type = st.selectbox("Тип", ["text", "number", "boolean", "json"])
            scope = st.selectbox(
                "Область",
                ["master", "channel"],
                format_func=lambda x: {"master": "Мастер", "channel": "Канал"}.get(str(x), str(x)),
            )
            unit = st.text_input("Ед. изм.")
            description = st.text_input("Описание")
            add_def = st.form_submit_button("Добавить / обновить атрибут")

            if add_def and code and name:
                upsert_attribute_definition(
                    conn=conn,
                    code=code.strip(),
                    name=name.strip(),
                    data_type=data_type,
                    scope=scope,
                    unit=unit or None,
                    description=description or None,
                )
                st.success("Атрибут сохранён")
                st.rerun()

    with right:
        if not product_id:
            st.subheader("Атрибуты товара")
            st.info("Сначала выбери товар во вкладке Каталог, чтобы редактировать его значения атрибутов.")
        else:
            st.subheader(f"Атрибуты товара #{product_id}")
            values = get_product_attribute_values(conn, int(product_id))
            values_df = pd.DataFrame(values) if values else pd.DataFrame()
            if not values_df.empty and category_scope != "Все":
                values_df = values_df[values_df["attribute_code"].astype(str).isin(set(required_map.keys()))]
            if not values_df.empty:
                values_df = values_df.copy()
                values_df["attribute_code_ru"] = values_df["attribute_code"].map(humanize_attribute_code)
                if "data_type" in values_df.columns:
                    values_df["data_type"] = values_df["data_type"].map(lambda x: {"text": "Текст", "number": "Число", "boolean": "Да/Нет", "json": "JSON"}.get(str(x), x))
                if "scope" in values_df.columns:
                    values_df["scope"] = values_df["scope"].map(lambda x: {"master": "Мастер", "channel": "Канал"}.get(str(x), x))
                value_columns = [
                    c
                    for c in [
                        "id",
                        "product_id",
                        "attribute_code_ru",
                        "attribute_code",
                        "name",
                        "value",
                        "value_text",
                        "value_number",
                        "value_boolean",
                        "value_json",
                        "data_type",
                        "scope",
                        "unit",
                        "locale",
                        "channel_code",
                        "updated_at",
                    ]
                    if c in values_df.columns
                ]
                st.dataframe(
                    with_ru_columns(
                        values_df[value_columns],
                        extra_map={
                            "attribute_code_ru": "Код атрибута (рус.)",
                            "attribute_code": "Технический код",
                            "value": "Значение",
                        },
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.caption("По текущему фильтру значения атрибутов не найдены.")

            defs = list_attribute_definitions(conn)
            if category_scope != "Все":
                allowed_codes = set(required_map.keys())
                defs = [d for d in defs if str(d.get("code")) in allowed_codes]
            def_codes = [d["code"] for d in defs] if defs else []
            def_labels = {code: humanize_attribute_code(code) for code in def_codes}

            with st.form("set_product_attr"):
                attribute_code = (
                    st.selectbox("Атрибут", def_codes, format_func=lambda x: f"{def_labels.get(x, x)} ({x})")
                    if def_codes
                    else st.text_input("Атрибут")
                )
                value = st.text_input("Значение")
                locale = st.text_input("Локаль", value="")
                channel_code = st.text_input("Код канала", value="")
                save_attr = st.form_submit_button("Сохранить значение")

                if save_attr and attribute_code:
                    set_product_attribute_value(
                        conn=conn,
                        product_id=int(product_id),
                        attribute_code=attribute_code,
                        value=value,
                        locale=locale or None,
                        channel_code=channel_code or None,
                    )
                    st.success("Значение сохранено")
                    st.rerun()

    conn.close()


def show_template_tab():
    st.subheader("Клиентский шаблон")
    st.caption("Здесь должен быть понятный сценарий: загрузили шаблон, увидели матчинг, поняли дыры, добили данные, скачали готовый файл.")
    with st.expander("Инструкция по кнопкам раздела Клиентский шаблон", expanded=False):
        st.markdown(
            """
- `Сохранить mapping rules`: сохранить карту соответствия колонок.
- `Сохранить профиль шаблона`: запомнить тип шаблона клиента.
- `Добавить несматченные в master-атрибуты`: автоматически создать недостающие атрибуты.
- `Подтвердить значения как client_validated`: отметить значения как проверенные.
- `Скачать заполненный шаблон`: выгрузка результата в формате клиента.
- `Обогатить товар из supplier` (в Gap): быстрый переход к автодозаполнению.
            """
        )
    conn = get_db()
    product_df = load_products(conn, limit=5000)

    t1, t2 = st.columns([1, 1])
    with t1:
        channel_code = st.text_input("Код клиента / канала", value="onlinetrade", key="template_channel_code")
    existing_profiles = list_template_profiles(conn, channel_code=channel_code or None)
    category_options, category_labels = _build_ozon_template_category_options(conn, channel_code=channel_code, limit=5000)
    with t2:
        category_code = st.selectbox(
            "Категория шаблона/профиля (Ozon-каталог)",
            options=category_options,
            index=0,
            format_func=lambda x: category_labels.get(str(x), str(x)),
            key="template_category_select",
        )

    p1, p2 = st.columns([1, 1])
    with p1:
        profile_name = st.text_input("Имя профиля шаблона", value=f"{channel_code}_default")
    with p2:
        profile_options = [None] + [p["id"] for p in existing_profiles]
        selected_profile_id = st.selectbox(
            "Загрузить сохранённый профиль",
            options=profile_options,
            format_func=lambda x: "-- нет --" if x is None else next((f"{p['profile_name']} (#{p['id']})" for p in existing_profiles if p['id'] == x), str(x)),
        )

    st.caption("Категория профиля берётся из Ozon-эталона (`ozon:description_category_id:type_id`).")

    uploaded = st.file_uploader("Загрузить Excel-шаблон клиента", type=["xlsx", "xls"], key="client_template")
    if uploaded is None:
        st.info("После загрузки файла станет доступна кнопка `Сохранить профиль шаблона (текущая схема)` — система запомнит тип шаблона клиента.")

    if uploaded is not None:
        uploaded_bytes = uploaded.getvalue()
        safe_uploaded_bytes = sanitize_template_xlsx_bytes(uploaded_bytes)
        workbook = load_workbook(BytesIO(safe_uploaded_bytes), read_only=True, data_only=False)
        template_sheet_options = workbook.sheetnames
        workbook.close()

        template_sheet_name = st.selectbox(
            "Лист шаблона",
            options=template_sheet_options,
            index=(template_sheet_options.index("Товары") if "Товары" in template_sheet_options else 0),
            key="template_sheet_name",
        )
        suggested_data_start_row = detect_template_data_start_row(safe_uploaded_bytes, sheet_name=template_sheet_name)

        tcfg1, tcfg2 = st.columns(2)
        with tcfg1:
            template_data_start_row = st.number_input(
                "Строка начала данных",
                min_value=2,
                max_value=100000,
                value=int(suggested_data_start_row),
                step=1,
                key="template_data_start_row",
            )
        with tcfg2:
            preserve_template_workbook = st.checkbox(
                "Сохранять исходную структуру Excel",
                value=True,
                help="Оставляет исходные листы, шапку, справочники и форматирование клиентского файла.",
                key="preserve_template_workbook",
            )

        template_df = pd.read_excel(BytesIO(safe_uploaded_bytes), sheet_name=template_sheet_name)
        template_signature = hashlib.md5(safe_uploaded_bytes).hexdigest()
        autoreg_key = f"{channel_code}|{category_code}|{template_sheet_name}|{template_signature}"
        if st.session_state.get("template_autoreg_key") != autoreg_key:
            reg = ensure_template_columns_registered(
                conn=conn,
                channel_code=channel_code,
                category_code=category_code or None,
                template_columns=list(template_df.columns),
            )
            st.session_state["template_autoreg_key"] = autoreg_key
            if (reg["attributes"] + reg["requirements"] + reg["rules"]) > 0:
                st.success(
                    f"Шаблон зарегистрирован: атрибутов {reg['attributes']}, "
                    f"требований {reg['requirements']}, правил {reg['rules']}."
                )
            else:
                st.caption("Атрибуты и требования этого шаблона уже были зарегистрированы ранее.")

        defs = list_attribute_definitions(conn)
        source_options = [("column", c) for c in [
            "article", "internal_article", "supplier_article", "name", "barcode", "brand", "description",
            "weight", "length", "width", "height", "package_length", "package_width", "package_height",
            "gross_weight", "image_url", "ozon_category_path", "ozon_description_category_id", "ozon_type_id",
            "category", "base_category", "supplier_name", "supplier_url",
            "uom", "tnved_code", "media_gallery"
        ]] + [("attribute", d["code"]) for d in defs]
        matches = auto_match_template_columns(conn, list(template_df.columns))
        matches = apply_saved_mapping_rules(conn, matches, channel_code=channel_code, category_code=category_code or None)
        if selected_profile_id:
            profile_columns = get_template_profile_columns(conn, int(selected_profile_id))
            profile_map = {c["template_column"]: c for c in profile_columns}
            matches = [
                {
                    "template_column": m["template_column"],
                    "status": "matched" if profile_map.get(m["template_column"], {}).get("source_name") else m["status"],
                    "source_type": profile_map.get(m["template_column"], {}).get("source_type", m["source_type"]),
                    "source_name": profile_map.get(m["template_column"], {}).get("source_name", m["source_name"]),
                    "matched_by": "template_profile" if profile_map.get(m["template_column"]) else m["matched_by"],
                    "transform_rule": profile_map.get(m["template_column"], {}).get("transform_rule"),
                }
                for m in matches
            ]

        # Применяем отложенные overrides до отрисовки tmpl_* виджетов,
        # чтобы не ловить StreamlitAPIException при записи в ключ уже созданного виджета.
        pending_manual_overrides = st.session_state.pop("template_manual_overrides", None)
        if isinstance(pending_manual_overrides, dict):
            for raw_idx, payload in pending_manual_overrides.items():
                try:
                    idx = int(raw_idx)
                except Exception:
                    continue
                if idx < 0 or idx >= len(matches):
                    continue
                source_type = str(payload.get("source_type") or "attribute")
                source_name = str(payload.get("source_name") or "")
                transform_rule = str(payload.get("transform_rule") or "")
                st.session_state[f"tmpl_type_{idx}"] = source_type
                st.session_state[f"tmpl_name_{idx}"] = source_name
                if transform_rule:
                    st.session_state[f"tmpl_transform_{idx}"] = transform_rule

        match_df = pd.DataFrame(matches)
        matched_count = int((match_df["status"] == "matched").sum()) if not match_df.empty else 0
        unmatched_count = int((match_df["status"] != "matched").sum()) if not match_df.empty else 0

        st.markdown("### Сводка по шаблону")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Колонок в шаблоне", len(template_df.columns))
        c2.metric("Сматчено", matched_count)
        c3.metric("Не сматчено", unmatched_count)
        c4.metric("Профилей найдено", len(existing_profiles))

        if unmatched_count == 0:
            st.success("По матчингу всё хорошо, можно переходить к товарам и preview.")
        else:
            st.warning("Есть несматченные колонки. Лучше сначала добить их, чтобы потом не ловить пустоты в выгрузке.")

        save_ready_rows = [
            {
                "template_column": m.get("template_column"),
                "status": m.get("status"),
                "source_type": m.get("source_type"),
                "source_name": m.get("source_name"),
                "matched_by": m.get("matched_by"),
                "transform_rule": m.get("transform_rule"),
            }
            for m in matches
            if str(m.get("template_column") or "").strip()
        ]
        save_col1, save_col2 = st.columns([1, 3])
        with save_col1:
            if st.button("Сохранить профиль шаблона (текущая схема)", key="template_save_profile_top", type="primary"):
                profile_id = save_template_profile(
                    conn=conn,
                    profile_name=profile_name,
                    channel_code=channel_code,
                    category_code=category_code or None,
                    file_name=getattr(uploaded, "name", None),
                    columns=save_ready_rows,
                )
                st.success(f"Профиль шаблона сохранён: #{profile_id}")
        with save_col2:
            st.caption("Эта кнопка сохраняет тип шаблона клиента для повторного использования без повторной ручной настройки.")

        tab_match, tab_fill, tab_gap = st.tabs(["1. Матчинг", "2. Заполнение и preview", "3. Gap и действия"])

        with tab_match:
            st.markdown("### Колонки шаблона")
            st.dataframe(pd.DataFrame({"template_column": list(template_df.columns)}), use_container_width=True, hide_index=True)

            st.markdown("### Автоматический матчинг")
            st.dataframe(match_df, use_container_width=True, hide_index=True)
            st.caption("Единицы измерения конвертируются автоматически по заголовку колонки (например, см→мм, кг→г). Поле `Transform` можно вручную переопределить.")

            st.markdown("### Ручная правка матчинга")
            manual_rows = []
            for idx, match in enumerate(matches):
                c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
                with c1:
                    st.text_input("Колонка", value=match["template_column"], disabled=True, key=f"tmpl_col_{idx}")
                with c2:
                    source_type = st.selectbox(
                        "Тип источника",
                        options=["attribute", "column", "skip"],
                        index=( ["attribute", "column", "skip"].index(match["source_type"]) if match["source_type"] in ["attribute", "column"] else 2 ),
                        key=f"tmpl_type_{idx}",
                    )
                with c3:
                    allowed_names = [name for stype, name in source_options if stype == source_type] if source_type != "skip" else [""]
                    current_name = match["source_name"] if match["source_name"] in allowed_names else (allowed_names[0] if allowed_names else "")
                    source_name = st.selectbox("Источник", options=allowed_names, index=(allowed_names.index(current_name) if current_name in allowed_names else 0), key=f"tmpl_name_{idx}") if allowed_names else st.text_input("Источник", value="", key=f"tmpl_name_{idx}")
                with c4:
                    current_transform = match.get("transform_rule") if match.get("transform_rule") in TEMPLATE_TRANSFORM_OPTIONS else ""
                    transform_rule = st.selectbox(
                        "Transform",
                        options=TEMPLATE_TRANSFORM_OPTIONS,
                        index=TEMPLATE_TRANSFORM_OPTIONS.index(current_transform),
                        key=f"tmpl_transform_{idx}",
                    )
                manual_rows.append({
                    "template_column": match["template_column"],
                    "status": "matched" if source_type != "skip" else "unmatched",
                    "source_type": None if source_type == "skip" else source_type,
                    "source_name": None if source_type == "skip" else source_name,
                    "matched_by": "manual" if source_type != "skip" else None,
                    "transform_rule": transform_rule or None,
                })

            manual_df = pd.DataFrame(manual_rows)
            unmatched = manual_df[manual_df["status"] == "unmatched"] if not manual_df.empty else pd.DataFrame()

            s1, s2, s3 = st.columns(3)
            with s1:
                if st.button("Сохранить mapping rules", type="primary"):
                    saved = 0
                    for row in manual_rows:
                        if row["status"] != "matched":
                            continue
                        upsert_channel_mapping_rule(
                            conn=conn,
                            channel_code=channel_code,
                            category_code=category_code or None,
                            target_field=row["template_column"],
                            source_type=row["source_type"],
                            source_name=row["source_name"],
                            transform_rule=row.get("transform_rule"),
                            is_required=0,
                        )
                        saved += 1
                    st.success(f"Сохранено mapping rules: {saved}")
            with s2:
                if st.button("Сохранить профиль шаблона"):
                    profile_id = save_template_profile(
                        conn=conn,
                        profile_name=profile_name,
                        channel_code=channel_code,
                        category_code=category_code or None,
                        file_name=getattr(uploaded, 'name', None),
                        columns=manual_rows,
                    )
                    st.success(f"Профиль шаблона сохранён: #{profile_id}")
            with s3:
                if st.button("Добавить несматченные в master-атрибуты"):
                    created = 0
                    overrides: dict[str, dict[str, str]] = {}
                    for idx, row in manual_df.iterrows():
                        if row["status"] == "matched":
                            continue
                        col_name = str(row["template_column"])
                        code = to_attribute_code(col_name)
                        if not code:
                            continue
                        upsert_attribute_definition(
                            conn=conn,
                            code=code,
                            name=col_name.strip(),
                            data_type="text",
                            scope="master",
                            unit=None,
                            description=f"Автосоздано из клиентского шаблона: {col_name}",
                        )
                        overrides[str(int(idx))] = {
                            "source_type": "attribute",
                            "source_name": code,
                        }
                        created += 1
                    if overrides:
                        st.session_state["template_manual_overrides"] = overrides
                    st.success(f"Создано/обновлено master-атрибутов: {created}. Маппинг предзаполнен автоматически.")
                    st.rerun()

            if not unmatched.empty:
                st.warning(f"Не сматчено колонок: {len(unmatched)}")
                st.dataframe(unmatched[["template_column", "status"]], use_container_width=True, hide_index=True)
            else:
                st.success("Все колонки шаблона сматчены.")

        with tab_fill:
            manual_rows = []
            for idx, match in enumerate(matches):
                source_type = st.session_state.get(f"tmpl_type_{idx}", match.get("source_type") if match.get("source_type") in ["attribute", "column"] else "skip")
                source_name = st.session_state.get(f"tmpl_name_{idx}", match.get("source_name"))
                transform_rule = st.session_state.get(f"tmpl_transform_{idx}", match.get("transform_rule") or "")
                manual_rows.append({
                    "template_column": match["template_column"],
                    "status": "matched" if source_type != "skip" else "unmatched",
                    "source_type": None if source_type == "skip" else source_type,
                    "source_name": None if source_type == "skip" else source_name,
                    "matched_by": "manual" if source_type != "skip" else None,
                    "transform_rule": transform_rule or None,
                })

            if product_df.empty:
                st.info("В каталоге пока нет товаров для заполнения шаблона.")
            else:
                selected_ids = st.multiselect(
                    "Выбери товары для заполнения шаблона",
                    options=product_df["id"].tolist(),
                    format_func=lambda x: f"ID {x} | {product_df.loc[product_df['id'] == x, 'name'].iloc[0]}",
                )

                if not selected_ids:
                    st.info("Выбери товары, и я покажу preview и готовность шаблона.")
                else:
                    filled_df = fill_template_dataframe(conn, template_df, selected_ids, manual_rows)
                    st.markdown("### Предпросмотр заполнения")
                    st.dataframe(filled_df, use_container_width=True, hide_index=True)
                    render_template_readiness(filled_df, manual_rows)

                    a1, a2 = st.columns(2)
                    with a1:
                        if st.button("Подтвердить значения как client_validated"):
                            result = apply_client_validated_values(conn, selected_ids, manual_rows, channel_code=channel_code or None)
                            st.success(f"Применено: {result['applied']}, пропущено по приоритету: {result['skipped']}")
                    with a2:
                        export_bytes = fill_template_workbook_bytes(
                            conn,
                            safe_uploaded_bytes,
                            selected_ids,
                            manual_rows,
                            sheet_name=template_sheet_name,
                            data_start_row=int(template_data_start_row),
                        ) if preserve_template_workbook else dataframe_to_excel_bytes(filled_df, sheet_name=template_sheet_name)
                        st.download_button(
                            "Скачать заполненный шаблон",
                            data=export_bytes,
                            file_name=f"filled_{Path(getattr(uploaded, 'name', 'client_template.xlsx')).name}",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )

        with tab_gap:
            manual_rows = []
            for idx, match in enumerate(matches):
                source_type = st.session_state.get(f"tmpl_type_{idx}", match.get("source_type") if match.get("source_type") in ["attribute", "column"] else "skip")
                source_name = st.session_state.get(f"tmpl_name_{idx}", match.get("source_name"))
                transform_rule = st.session_state.get(f"tmpl_transform_{idx}", match.get("transform_rule") or "")
                manual_rows.append({
                    "template_column": match["template_column"],
                    "status": "matched" if source_type != "skip" else "unmatched",
                    "source_type": None if source_type == "skip" else source_type,
                    "source_name": None if source_type == "skip" else source_name,
                    "matched_by": "manual" if source_type != "skip" else None,
                    "transform_rule": transform_rule or None,
                })

            if product_df.empty:
                st.info("Сначала нужны товары в каталоге.")
            else:
                selected_ids = st.session_state.get("template_selected_ids")
                selected_ids = st.multiselect(
                    "Товары для gap-анализа",
                    options=product_df["id"].tolist(),
                    default=selected_ids if selected_ids else [],
                    format_func=lambda x: f"ID {x} | {product_df.loc[product_df['id'] == x, 'name'].iloc[0]}",
                    key="template_gap_selected_ids",
                )
                st.session_state["template_selected_ids"] = selected_ids

                if not selected_ids:
                    st.info("Выбери товары, чтобы увидеть gap-анализ и быстрые действия.")
                else:
                    filled_df = fill_template_dataframe(conn, template_df, selected_ids, manual_rows)
                    manual_df = pd.DataFrame(manual_rows)
                    gap_rows = []
                    gap_actions = []
                    for _, row in manual_df.iterrows():
                        if row["status"] != "matched":
                            gap_rows.append({"template_column": row["template_column"], "reason": "Нет матчинга"})
                            continue
                        if filled_df[row["template_column"]].isna().all():
                            gap_rows.append({"template_column": row["template_column"], "reason": "У выбранных товаров нет данных"})
                            for product_id in selected_ids:
                                product_row = get_product(conn, int(product_id))
                                can_supplier = bool(product_row and product_row["supplier_url"])
                                gap_actions.append({
                                    "product_id": product_id,
                                    "product_name": product_row["name"] if product_row else None,
                                    "template_column": row["template_column"],
                                    "can_supplier_enrich": can_supplier,
                                })

                    if gap_rows:
                        st.markdown("### Gap-анализ")
                        st.dataframe(pd.DataFrame(gap_rows), use_container_width=True, hide_index=True)
                    else:
                        st.success("Критичных gap по текущему выбору не найдено.")

                    if gap_actions:
                        st.markdown("### Быстрые действия")
                        action_df = pd.DataFrame(gap_actions)
                        st.dataframe(action_df, use_container_width=True, hide_index=True)

                        action_product_id = st.selectbox(
                            "Выбери товар для быстрого действия",
                            options=sorted(set([x["product_id"] for x in gap_actions])),
                            format_func=lambda x: f"ID {x} | {next((g['product_name'] for g in gap_actions if g['product_id'] == x), x)}",
                        )
                        a1, a2 = st.columns(2)
                        with a1:
                            if st.button("Обогатить товар из supplier", key="gap_supplier_enrich"):
                                result = enrich_product_from_supplier(conn, int(action_product_id), force=False)
                                if result["ok"]:
                                    st.success(result["message"])
                                    st.rerun()
                                else:
                                    st.error(result["message"])
                        with a2:
                            if st.button("Открыть товар в карточке", key="gap_open_product"):
                                st.session_state["selected_product_id"] = int(action_product_id)
                                st.success(f"Товар #{action_product_id} выбран, открой вкладку Карточка")

    conn.close()


def show_ozon_tab():
    conn = get_db()
    st.subheader("Ozon")
    st.caption("Ozon для нас, это эталон структуры и атрибутов. Здесь синхронизируем дерево категорий и характеристики категорий в локальный кэш PIM.")
    with st.expander("Инструкция по разделу Ozon и расшифровка кнопок", expanded=False):
        st.markdown(
            """
**Порядок работы (рекомендуемый)**
1. `Синхронизировать дерево категорий Ozon`
2. `Запустить полную синхронизацию Ozon в фоне`
3. Проверить блок покрытия синхронизации (`Пары для проверки`, `Пары с атрибутами`, `Пропущено пар`, `%`)
4. Если есть пропуски: `Досинхронизировать пропущенные категории`
5. При необходимости импортировать в мастер: `Импортировать все атрибуты Ozon из кэша в PIM`

**Кнопки верхнего блока**
- `Синхронизировать дерево категорий Ozon`: обновляет локальный кэш дерева категорий Ozon.
- `Запустить полную синхронизацию Ozon в фоне`: фоном проходит по категориям и подтягивает атрибуты, не блокируя UI.
- `Импортировать все атрибуты Ozon из кэша в PIM`: переносит уже загруженные атрибуты из кэша в master-слой PIM.
- `Досинхронизировать пропущенные категории`: подтягивает только те пары `cat/type`, где в кэше ещё нет атрибутов.

**Кнопки по выбранной Ozon-категории**
- `Синхронизировать атрибуты выбранной категории`: точечная синхронизация атрибутов одной пары `cat/type`.
- `Импортировать атрибуты Ozon в PIM`: перенос атрибутов выбранной пары в `attribute_definitions` и requirements.
- `Создать стартовые mapping rules для Ozon`: создаёт стартовые правила маппинга для выбранной категории.
- `Синхронизировать все справочники категории`: подтягивает dictionary-значения по всем dictionary-атрибутам категории.
- `Синхронизировать значения справочника`: подтягивает dictionary-значения только для выбранного атрибута.

**Кнопки массовой работы по товарам**
- `Загрузить список из Excel`: выбирает товары из Excel для массовых действий.
- `Проверить покрытие товара под Ozon`: отчёт по одному товару (готовность required-атрибутов).
- `Заполнить Ozon-атрибуты из мастер-карточки`: автозаполнение Ozon-атрибутов по одному товару.
- `Массовая проверка готовности по выбранным товарам`: отчёт готовности по группе товаров.
- `Массово заполнить Ozon-атрибуты для выбранных`: автозаполнение по группе товаров.
- `Сформировать dictionary gaps по выбранным (Excel)`: выгрузка проблем словарного сопоставления.
- `Отправить batch в Ozon (/v1/product/attributes/update)`: отправка подготовленного batch в Ozon API.

**Кнопки dictionary overrides**
- `Сохранить dictionary override`: сохранить ручное правило raw -> dictionary value.
- `Импортировать overrides из Excel`: массовая загрузка overrides из файла.
- `Удалить выбранный override`: удалить сохранённый override.

**Кнопки по jobs (журнал отправок)**
- `Массово повторить jobs из Excel`: повторная отправка job_id из Excel.
- `Повторить все jobs из фильтра`: повтор всех jobs текущего фильтра статуса.
- `Повторить отправку job`: повтор одного выбранного job.
            """
        )

    c1, c2 = st.columns(2)
    with c1:
        client_id = st.text_input("Client ID Ozon", value="")
    with c2:
        api_key = st.text_input("API Key Ozon", value="", type="password")

    configured = is_configured(client_id or None, api_key or None)
    if configured:
        st.success("Ozon-креды заданы, можно синхронизировать дерево и атрибуты.")
    else:
        st.warning("Ozon-креды не заданы в этой сессии. Можно вставить их сюда вручную и сразу выполнить синхронизацию.")

    top1, top2, top3, top4 = st.columns(4)
    with top1:
        if st.button("Синхронизировать дерево категорий Ozon", type="primary", disabled=not configured, help="Обновить локальный кэш дерева категорий Ozon"):
            result = sync_category_tree(conn, client_id=client_id or None, api_key=api_key or None)
            st.success(f"Дерево категорий обновлено, записей: {result['total']}")
            st.rerun()
    with top2:
        if st.button("Запустить полную синхронизацию Ozon в фоне", disabled=not configured, help="Фоновая загрузка атрибутов по категориям Ozon"):
            ok, message = _start_ozon_bg_sync(client_id=client_id or "", api_key=api_key or "")
            if ok:
                st.success(message)
            else:
                st.info(message)
    with top3:
        category_limit = st.number_input("Сколько категорий показать", min_value=100, max_value=10000, value=2000, step=100)
    with top4:
        if st.button("Импортировать все атрибуты Ozon из кэша в PIM", help="Перенести атрибуты из ozon_attribute_cache в master-атрибуты PIM"):
            result = import_all_cached_attributes_to_pim(conn)
            st.success(
                "Массовый импорт завершён: "
                f"пар обработано {int(result.get('pairs_processed') or 0)} из {int(result.get('pairs_total') or 0)}, "
                f"атрибутов импортировано {int(result.get('imported_total') or 0)}."
            )
            if result.get("errors"):
                st.warning(f"Ошибок при массовом импорте: {len(result['errors'])}.")
            st.rerun()

    bg_state = _get_ozon_bg_state()
    if bg_state.get("running"):
        st.info(
            "Фоновая синхронизация Ozon выполняется. "
            f"Старт: {bg_state.get('started_at') or '-'}."
        )
    elif bg_state.get("last_error"):
        st.error(f"Фоновая синхронизация Ozon завершилась с ошибкой: {bg_state.get('last_error')}")
    elif bg_state.get("result"):
        r = bg_state.get("result") or {}
        st.success(
            "Фоновая синхронизация Ozon завершена: "
            f"пар обработано {int(r.get('pairs_processed') or 0)} из {int(r.get('pairs_total') or 0)}, "
            f"атрибутов загружено {int(r.get('attributes_total') or 0)}, "
            f"импортировано в PIM {int(r.get('imported_to_pim') or 0)}."
        )
        if r.get("errors"):
            st.warning(f"Ошибок в фоновой синхронизации: {len(r['errors'])}.")
    st.caption("Полная синхронизация Ozon теперь запускается в фоне и не блокирует работу с остальными разделами.")

    stats = get_ozon_cache_stats(conn)
    s1, s2, s3, s4, s5, s6 = st.columns(6)
    s1.metric("Узлов категорий", int(stats.get("category_nodes") or 0))
    s2.metric("Уникальных пар категорий", int(stats.get("category_pairs") or 0))
    s3.metric("Атрибутов в кэше", int(stats.get("attributes_total") or 0))
    s4.metric("Обязательных", int(stats.get("attributes_required") or 0))
    s5.metric("Атрибутов в мастере", int(stats.get("attribute_defs_ozon") or 0))
    s6.metric("Требований Ozon (категорийных)", int(stats.get("ozon_requirements") or 0))
    if int(stats.get("category_pairs") or 0) == 0 and int(stats.get("attribute_pairs") or 0) > 0:
        st.warning(
            f"В кэше категорий пока 0 пар cat/type, но в кэше атрибутов уже есть {int(stats.get('attribute_pairs') or 0)} пар. "
            "Сначала нажми `Синхронизировать дерево категорий Ozon`, затем запусти полную синхронизацию."
        )

    qc1, qc2, qc3, qc4 = st.columns([1, 1, 1, 2])
    with qc1:
        coverage_only_leaf = st.checkbox(
            "Проверять только листовые категории",
            value=True,
            key="ozon_coverage_only_leaf",
        )
    with qc2:
        coverage_include_disabled = st.checkbox(
            "Включая отключённые",
            value=False,
            key="ozon_coverage_include_disabled",
        )
    with qc3:
        missing_sync_limit = st.number_input(
            "Лимит досинхронизации пропусков",
            min_value=10,
            max_value=5000,
            value=500,
            step=10,
            key="ozon_missing_sync_limit",
        )
    with qc4:
        if st.button("Досинхронизировать пропущенные категории", disabled=not configured, help="Обработать только пары cat/type без атрибутов в кэше"):
            miss_result = sync_missing_category_attributes(
                conn,
                client_id=client_id or None,
                api_key=api_key or None,
                only_leaf=bool(coverage_only_leaf),
                include_disabled=bool(coverage_include_disabled),
                limit=int(missing_sync_limit),
                import_to_pim=True,
            )
            st.success(
                "Досинхронизация завершена: "
                f"обработано пар {int(miss_result.get('pairs_processed') or 0)} из {int(miss_result.get('missing_pairs_requested') or 0)}, "
                f"атрибутов загружено {int(miss_result.get('attributes_total') or 0)}."
            )
            if miss_result.get("errors"):
                st.warning(f"Ошибок при досинхронизации: {len(miss_result['errors'])}.")
            st.rerun()

    coverage = get_ozon_sync_coverage(
        conn,
        only_leaf=bool(coverage_only_leaf),
        include_disabled=bool(coverage_include_disabled),
        missing_preview_limit=200,
    )
    cv1, cv2, cv3, cv4 = st.columns(4)
    cv1.metric("Пары для проверки", int(coverage.get("total_pairs") or 0))
    cv2.metric("Пары с атрибутами", int(coverage.get("pairs_with_attrs") or 0))
    cv3.metric("Пропущено пар", int(coverage.get("missing_pairs") or 0))
    cv4.metric("Покрытие синхронизации, %", float(coverage.get("coverage_percent") or 0.0))
    if int(coverage.get("missing_pairs") or 0) > 0:
        st.warning(
            f"Синхронизация Ozon покрыта не полностью: {int(coverage.get('pairs_with_attrs') or 0)} из {int(coverage.get('total_pairs') or 0)} пар. "
            "Ниже показан список первых пропусков."
        )
        missing_preview = coverage.get("missing_preview") or []
        if missing_preview:
            st.dataframe(with_ru_columns(pd.DataFrame(missing_preview)), use_container_width=True, hide_index=True)
    else:
        st.success("Покрытие синхронизации Ozon полное для выбранных условий проверки.")

    category_search = st.text_input(
        "Фильтр категорий Ozon",
        value="",
        placeholder="Например: велосипед, аксессуары, запчасти",
        key="ozon_category_search",
    )
    category_pairs = list_cached_category_pairs(conn, search=category_search or None, limit=int(category_limit))
    categories = list_cached_categories(conn, limit=min(1000, int(category_limit)))
    if not categories:
        st.warning("Кэш категорий Ozon пуст. Сначала запусти синхронизацию дерева категорий.")
    if categories:
        cat_df = pd.DataFrame(categories)
        st.markdown("### Кэш категорий Ozon")
        st.dataframe(with_ru_columns(cat_df[[c for c in ["description_category_id", "category_name", "full_path", "type_id", "type_name", "disabled", "fetched_at"] if c in cat_df.columns]]), use_container_width=True, hide_index=True)

        if category_pairs:
            pairs_df = pd.DataFrame(category_pairs)
            st.markdown("### Уникальные пары категорий Ozon (cat/type)")
            st.dataframe(
                with_ru_columns(pairs_df[[c for c in ["description_category_id", "type_id", "full_path", "type_name", "disabled", "nodes", "fetched_at"] if c in pairs_df.columns]]),
                use_container_width=True,
                hide_index=True,
            )

        valid_rows = [row for row in category_pairs if row.get("description_category_id") and row.get("type_id")]
        if valid_rows:
            category_options = [f"{row['full_path']} | cat={row['description_category_id']} | type={row['type_id']}" for row in valid_rows]
            selected_category_label = st.selectbox("Категория Ozon для загрузки атрибутов", options=category_options)
            selected_row = valid_rows[category_options.index(selected_category_label)]

            a1, a2 = st.columns(2)
            with a1:
                if st.button("Синхронизировать атрибуты выбранной категории", disabled=not configured):
                    result = sync_category_attributes(
                        conn,
                        description_category_id=int(selected_row["description_category_id"]),
                        type_id=int(selected_row["type_id"]),
                        client_id=client_id or None,
                        api_key=api_key or None,
                    )
                    st.success(f"Атрибуты обновлены: всего {result['total']}, обязательных {result['required']}")
                    st.rerun()
            with a2:
                attr_limit = st.number_input("Сколько атрибутов показать", min_value=50, max_value=2000, value=300, step=50)

            attributes = list_cached_attributes(
                conn,
                description_category_id=int(selected_row["description_category_id"]),
                type_id=int(selected_row["type_id"]),
                limit=int(attr_limit),
            )
            if attributes:
                attr_df = pd.DataFrame(attributes)
                required_count = int(attr_df["is_required"].fillna(0).astype(int).sum()) if "is_required" in attr_df.columns else 0
                master_seed = ensure_ozon_master_attributes(conn)
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Атрибутов в кэше", int(len(attr_df)))
                m2.metric("Обязательных", required_count)
                m3.metric("Справочники", int((attr_df["dictionary_id"].fillna(0).astype(float) > 0).sum()) if "dictionary_id" in attr_df.columns else 0)
                m4.metric("Базовых мастер-атрибутов", int(master_seed["total"]))

                a1, a2 = st.columns(2)
                with a1:
                    if st.button("Импортировать атрибуты Ozon в PIM"):
                        result = import_cached_attributes_to_pim(
                            conn,
                            description_category_id=int(selected_row["description_category_id"]),
                            type_id=int(selected_row["type_id"]),
                        )
                        st.success(f"В PIM импортировано {result['imported']} атрибутов, обязательных {result['required']}. category_code={result['category_code']}")
                with a2:
                    if st.button("Создать стартовые mapping rules для Ozon"):
                        result = save_suggested_mappings(
                            conn,
                            description_category_id=int(selected_row["description_category_id"]),
                            type_id=int(selected_row["type_id"]),
                        )
                        st.success(f"Сохранено стартовых mapping rules: {result['saved']}. category_code={result['category_code']}")

                mapping_df = pd.DataFrame(
                    suggest_mappings_for_cached_attributes(
                        conn,
                        description_category_id=int(selected_row["description_category_id"]),
                        type_id=int(selected_row["type_id"]),
                    )
                )
                if not mapping_df.empty:
                    mm1, mm2, mm3 = st.columns(3)
                    mm1.metric("Matched по эвристике/правилам", int((mapping_df["status"] == "matched").sum()))
                    mm2.metric("Без маппинга", int((mapping_df["status"] != "matched").sum()))
                    mm3.metric("Обязательных без маппинга", int(((mapping_df["is_required"] == 1) & (mapping_df["status"] != "matched")).sum()))

                    st.markdown("### Предлагаемые Ozon mapping rules")
                    st.dataframe(
                        with_ru_columns(mapping_df[[c for c in ["attribute_id", "name", "group_name", "is_required", "source_type", "source_name", "transform_rule", "matched_by", "status"] if c in mapping_df.columns]]),
                        use_container_width=True,
                        hide_index=True,
                    )

                st.markdown("### Атрибуты выбранной категории")
                attr_show = attr_df[[c for c in ["attribute_id", "name", "group_name", "type", "dictionary_id", "is_required", "is_collection", "max_value_count", "fetched_at"] if c in attr_df.columns]].copy()
                if "attribute_id" in attr_show.columns:
                    attr_show["attribute_code_ru"] = attr_show["attribute_id"].map(lambda x: f"Ozon атрибут ID {int(x)}" if pd.notna(x) else "")
                st.dataframe(
                    with_ru_columns(attr_show, extra_map={"attribute_code_ru": "Код атрибута (рус.)", "type": "Тип"}),
                    use_container_width=True,
                    hide_index=True,
                )

                dictionary_attrs = [row for row in attributes if int(row.get("dictionary_id") or 0) > 0]
                if dictionary_attrs:
                    st.markdown("### Справочники значений Ozon")
                    dd1, dd2, dd3 = st.columns(3)
                    dd1.metric("Атрибутов-справочников", int(len(dictionary_attrs)))
                    cached_dict_attr_count = conn.execute(
                        "SELECT COUNT(DISTINCT attribute_id) FROM ozon_attribute_value_cache WHERE description_category_id = ? AND type_id = ?",
                        (int(selected_row["description_category_id"]), int(selected_row["type_id"])),
                    ).fetchone()[0]
                    cached_dict_value_count = conn.execute(
                        "SELECT COUNT(*) FROM ozon_attribute_value_cache WHERE description_category_id = ? AND type_id = ?",
                        (int(selected_row["description_category_id"]), int(selected_row["type_id"])),
                    ).fetchone()[0]
                    dd2.metric("Справочников в кэше", int(cached_dict_attr_count or 0))
                    dd3.metric("Значений в кэше", int(cached_dict_value_count or 0))

                    if st.button("Синхронизировать все справочники категории", disabled=not configured):
                        result = sync_all_category_dictionary_values(
                            conn,
                            description_category_id=int(selected_row["description_category_id"]),
                            type_id=int(selected_row["type_id"]),
                            client_id=client_id or None,
                            api_key=api_key or None,
                        )
                        st.success(f"Синхронизировано справочников: {result['synced_attributes']}, значений: {result['synced_values']}")

                    dict_options = [f"{row['name']} | attr={row['attribute_id']} | dict={row['dictionary_id']}" for row in dictionary_attrs]
                    selected_dict_label = st.selectbox("Атрибут-справочник", options=dict_options, key="ozon_dict_attr")
                    selected_dict_row = dictionary_attrs[dict_options.index(selected_dict_label)]
                    d1, d2 = st.columns(2)
                    with d1:
                        if st.button("Синхронизировать значения справочника", disabled=not configured):
                            result = sync_attribute_dictionary_values(
                                conn,
                                description_category_id=int(selected_row["description_category_id"]),
                                type_id=int(selected_row["type_id"]),
                                attribute_id=int(selected_dict_row["attribute_id"]),
                                client_id=client_id or None,
                                api_key=api_key or None,
                            )
                            st.success(f"Значения справочника обновлены: {result['inserted']} | attr={result['attribute_id']} | dict={result['dictionary_id']}")
                    with d2:
                        dict_limit = st.number_input("Сколько значений справочника показать", min_value=50, max_value=5000, value=200, step=50, key="ozon_dict_limit")
                    dict_search = st.text_input("Фильтр по значению справочника", value="", key="ozon_dict_search")
                    dict_values = list_cached_attribute_values(
                        conn,
                        description_category_id=int(selected_row["description_category_id"]),
                        type_id=int(selected_row["type_id"]),
                        attribute_id=int(selected_dict_row["attribute_id"]),
                        search=dict_search or None,
                        limit=int(dict_limit),
                    )
                    if dict_values:
                        dict_df = pd.DataFrame(dict_values)
                        st.dataframe(with_ru_columns(dict_df[[c for c in ["value_id", "value", "info", "picture", "fetched_at"] if c in dict_df.columns]]), use_container_width=True, hide_index=True)
                    else:
                        st.caption("Значения этого справочника ещё не загружены в кэш.")

                product_rows = conn.execute(
                    "SELECT id, name, article, internal_article, supplier_article FROM products ORDER BY id DESC LIMIT 5000"
                ).fetchall()
                if product_rows:
                    product_options = [int(r["id"]) for r in product_rows]
                    selected_product_id = st.selectbox(
                        "Проверить покрытие конкретного товара под выбранную Ozon-категорию",
                        options=product_options,
                        format_func=lambda x: next((f"ID {r['id']} | {r['article'] or '-'} | {r['name'] or '-'}" for r in product_rows if int(r['id']) == int(x)), str(x)),
                        key="ozon_coverage_product_id",
                    )
                    dictionary_min_score = st.slider(
                        "Порог dictionary matching (чем выше, тем строже)",
                        min_value=0.50,
                        max_value=0.99,
                        value=0.78,
                        step=0.01,
                        key=f"ozon_dict_min_score_{selected_product_id}",
                    )
                    st.markdown("### Excel: список товаров для массовых действий")
                    excel_col1, excel_col2, excel_col3 = st.columns([1, 2, 1])
                    with excel_col1:
                        excel_lookup_field = st.selectbox(
                            "Поле поиска в Excel",
                            options=["id", "article", "internal_article", "supplier_article"],
                            index=1,
                            key=f"ozon_excel_lookup_{selected_product_id}",
                        )
                        excel_sheet_name = st.text_input(
                            "Лист Excel (опционально)",
                            value="",
                            key=f"ozon_excel_sheet_{selected_product_id}",
                            placeholder="Например: products",
                        )
                        excel_column_name = st.text_input(
                            "Колонка Excel (опционально)",
                            value="",
                            key=f"ozon_excel_column_{selected_product_id}",
                            placeholder="Например: article",
                        )
                    with excel_col2:
                        excel_file = st.file_uploader(
                            "Загрузи Excel со списком товаров",
                            type=["xlsx", "xls"],
                            key=f"ozon_excel_file_{selected_product_id}",
                        )
                    bulk_select_key = f"ozon_bulk_product_ids_{selected_product_id}"
                    if bulk_select_key not in st.session_state:
                        st.session_state[bulk_select_key] = [int(selected_product_id)]
                    with excel_col3:
                        st.download_button(
                            "Скачать шаблон Excel",
                            data=build_ozon_product_list_template_excel(),
                            file_name="ozon_products_list_template.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"ozon_excel_template_{selected_product_id}",
                        )
                        if st.button("Загрузить список из Excel", key=f"ozon_excel_apply_{selected_product_id}"):
                            if excel_file is None:
                                st.warning("Сначала загрузи Excel файл.")
                            else:
                                parsed = resolve_product_ids_from_excel(
                                    conn,
                                    excel_file.read(),
                                    excel_lookup_field,
                                    sheet_name=excel_sheet_name or None,
                                    column_name=excel_column_name or None,
                                )
                                st.session_state[f"ozon_excel_parse_{selected_product_id}"] = parsed
                                if parsed.get("ok"):
                                    st.session_state[bulk_select_key] = parsed.get("resolved_ids") or [int(selected_product_id)]
                                    st.success(
                                        f"Excel обработан: найдено {parsed.get('resolved_count', 0)} из {parsed.get('input_values', 0)} значений."
                                    )
                                    st.rerun()
                                else:
                                    st.error(parsed.get("message") or "Не удалось обработать Excel.")

                    parse_summary = st.session_state.get(f"ozon_excel_parse_{selected_product_id}")
                    if parse_summary and parse_summary.get("ok"):
                        s1, s2, s3 = st.columns(3)
                        s1.metric("Входных значений", int(parse_summary.get("input_values") or 0))
                        s2.metric("Найдено товаров", int(parse_summary.get("resolved_count") or 0))
                        s3.metric("Не найдено", int(parse_summary.get("not_found_count") or 0))
                        st.caption(
                            f"Использована колонка: {parse_summary.get('used_column')} | Поле lookup: {parse_summary.get('lookup_field')}"
                        )
                        not_found = parse_summary.get("not_found") or []
                        if not_found:
                            not_found_df = pd.DataFrame({"not_found_value": not_found})
                            st.dataframe(not_found_df, use_container_width=True, hide_index=True)
                            st.download_button(
                                "Скачать не найденные значения (Excel)",
                                data=dataframe_to_excel_bytes(not_found_df, sheet_name="not_found"),
                                file_name="ozon_excel_not_found.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"ozon_not_found_export_{selected_product_id}",
                            )

                    selected_product_ids = st.multiselect(
                        "Товары для массовых действий",
                        options=product_options,
                        format_func=lambda x: next((f"ID {r['id']} | {r['article'] or '-'} | {r['name'] or '-'}" for r in product_rows if int(r['id']) == int(x)), str(x)),
                        key=bulk_select_key,
                    )
                    required_only_mode = st.checkbox(
                        "Работать только с обязательными Ozon-атрибутами",
                        value=False,
                        key=f"ozon_required_only_{selected_product_id}",
                    )
                    offer_id_field = st.selectbox(
                        "Поле товара для Ozon offer_id",
                        options=OZON_OFFER_ID_OPTIONS,
                        index=0,
                        key=f"ozon_offer_id_field_{selected_product_id}",
                    )
                    preview_rows = build_product_ozon_payload(
                        conn,
                        product_id=int(selected_product_id),
                        description_category_id=int(selected_row["description_category_id"]),
                        type_id=int(selected_row["type_id"]),
                        required_only=required_only_mode,
                        dictionary_min_score=float(dictionary_min_score),
                    )
                    if preview_rows:
                        preview_df = pd.DataFrame(preview_rows)
                        p1, p2, p3, p4 = st.columns(4)
                        p1.metric("Готово к автозаполнению", int((preview_df["status"] == "ready").sum()))
                        p2.metric("Пусто после маппинга", int((preview_df["status"] == "empty").sum()))
                        p3.metric("Обязательных готово", int(((preview_df["status"] == "ready") & (preview_df["is_required"] == 1)).sum()))
                        p4.metric("Dictionary не сматчено", int((preview_df["status"] == "dictionary_unmatched").sum()))

                        action1, action2 = st.columns(2)
                        with action1:
                            if st.button("Проверить покрытие товара под Ozon"):
                                coverage = analyze_product_ozon_coverage(
                                    conn,
                                    product_id=int(selected_product_id),
                                    description_category_id=int(selected_row["description_category_id"]),
                                    type_id=int(selected_row["type_id"]),
                                    dictionary_min_score=float(dictionary_min_score),
                                )
                                summary = coverage["summary"]
                                cc1, cc2, cc3, cc4 = st.columns(4)
                                cc1.metric("Готовность, %", int(summary["readiness_pct"]))
                                cc2.metric("Обязательных всего", int(summary["required_total"]))
                                cc3.metric("Обязательных закрыто", int(summary["required_covered"]))
                                cc4.metric("Обязательных пусто", int(summary["required_missing"]))
                                st.caption(f"Обязательных с несопоставленным справочником: {int(summary.get('required_dictionary_unmatched') or 0)}")
                                if summary["readiness_pct"] == 100:
                                    st.success("Обязательные Ozon-атрибуты по этой категории закрыты.")
                                else:
                                    st.warning("Не все обязательные Ozon-атрибуты закрыты. Ниже видно, что именно отсутствует.")
                                coverage_df = pd.DataFrame(coverage["rows"])
                                if not coverage_df.empty:
                                    st.dataframe(coverage_df, use_container_width=True, hide_index=True)
                        with action2:
                            if st.button("Заполнить Ozon-атрибуты из мастер-карточки"):
                                result = materialize_product_ozon_attributes(
                                    conn,
                                    product_id=int(selected_product_id),
                                    description_category_id=int(selected_row["description_category_id"]),
                                    type_id=int(selected_row["type_id"]),
                                    required_only=required_only_mode,
                                    dictionary_min_score=float(dictionary_min_score),
                                )
                                st.success(
                                    f"Записано Ozon-значений: {result['applied']}, пустых пропущено: {result['skipped_empty']}, "
                                    f"dictionary без матчинга: {result.get('skipped_dictionary', 0)}. category_code={result['category_code']}"
                                )

                        b1, b2 = st.columns(2)
                        with b1:
                            if st.button("Массовая проверка готовности по выбранным товарам"):
                                if not selected_product_ids:
                                    st.warning("Выбери хотя бы один товар для массовой проверки.")
                                else:
                                    report_rows = []
                                    progress = st.progress(0)
                                    for i, pid in enumerate(selected_product_ids, start=1):
                                        coverage = analyze_product_ozon_coverage(
                                            conn,
                                            product_id=int(pid),
                                            description_category_id=int(selected_row["description_category_id"]),
                                            type_id=int(selected_row["type_id"]),
                                            dictionary_min_score=float(dictionary_min_score),
                                        )
                                        summary = coverage.get("summary", {})
                                        product_row = next((r for r in product_rows if int(r["id"]) == int(pid)), None)
                                        report_rows.append(
                                            {
                                                "product_id": int(pid),
                                                "offer_id": (product_row[offer_id_field] if product_row else None),
                                                "article": product_row["article"] if product_row else None,
                                                "name": product_row["name"] if product_row else None,
                                                "readiness_pct": int(summary.get("readiness_pct") or 0),
                                                "required_total": int(summary.get("required_total") or 0),
                                                "required_covered": int(summary.get("required_covered") or 0),
                                                "required_missing": int(summary.get("required_missing") or 0),
                                                "required_dictionary_unmatched": int(summary.get("required_dictionary_unmatched") or 0),
                                            }
                                        )
                                        progress.progress(i / len(selected_product_ids))
                                    report_df = pd.DataFrame(report_rows).sort_values(
                                        by=["readiness_pct", "required_dictionary_unmatched", "required_missing"],
                                        ascending=[False, True, True],
                                    )
                                    st.dataframe(report_df, use_container_width=True, hide_index=True)
                                    st.download_button(
                                        "Скачать отчёт готовности Ozon (Excel)",
                                        data=dataframe_to_excel_bytes(report_df, sheet_name="ozon_readiness"),
                                        file_name="ozon_readiness_report.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key=f"ozon_readiness_export_{selected_product_id}",
                                    )
                        with b2:
                            if st.button("Массово заполнить Ozon-атрибуты для выбранных"):
                                if not selected_product_ids:
                                    st.warning("Выбери хотя бы один товар для массового заполнения.")
                                else:
                                    progress = st.progress(0)
                                    total_applied = 0
                                    total_skipped_empty = 0
                                    total_skipped_dict = 0
                                    for i, pid in enumerate(selected_product_ids, start=1):
                                        result = materialize_product_ozon_attributes(
                                            conn,
                                            product_id=int(pid),
                                            description_category_id=int(selected_row["description_category_id"]),
                                            type_id=int(selected_row["type_id"]),
                                            required_only=required_only_mode,
                                            dictionary_min_score=float(dictionary_min_score),
                                        )
                                        total_applied += int(result.get("applied") or 0)
                                        total_skipped_empty += int(result.get("skipped_empty") or 0)
                                        total_skipped_dict += int(result.get("skipped_dictionary") or 0)
                                        progress.progress(i / len(selected_product_ids))
                                    st.success(
                                        f"Массовое заполнение завершено. Записано: {total_applied}, "
                                        f"пустых пропущено: {total_skipped_empty}, dictionary без матчинга: {total_skipped_dict}."
                                    )
                        if st.button("Сформировать dictionary gaps по выбранным (Excel)", key=f"ozon_bulk_gap_export_btn_{selected_product_id}"):
                            if not selected_product_ids:
                                st.warning("Выбери хотя бы один товар.")
                            else:
                                gap_export_rows = []
                                progress = st.progress(0)
                                for i, pid in enumerate(selected_product_ids, start=1):
                                    product_row = next((r for r in product_rows if int(r["id"]) == int(pid)), None)
                                    gap_rows = preview_product_ozon_dictionary_gaps(
                                        conn=conn,
                                        product_id=int(pid),
                                        description_category_id=int(selected_row["description_category_id"]),
                                        type_id=int(selected_row["type_id"]),
                                        top_n=3,
                                        dictionary_min_score=float(dictionary_min_score),
                                    )
                                    for gap in gap_rows:
                                        gap_export_rows.append(
                                            {
                                                "product_id": int(pid),
                                                "article": product_row["article"] if product_row else None,
                                                "name": product_row["name"] if product_row else None,
                                                "attribute_id": gap.get("attribute_id"),
                                                "attribute_name": gap.get("name"),
                                                "source_name": gap.get("source_name"),
                                                "raw_value": gap.get("raw_value"),
                                                "suggestion_values": gap.get("suggestion_values"),
                                            }
                                        )
                                    progress.progress(i / len(selected_product_ids))
                                if gap_export_rows:
                                    gap_export_df = pd.DataFrame(gap_export_rows)
                                    st.dataframe(gap_export_df, use_container_width=True, hide_index=True)
                                    st.download_button(
                                        "Скачать dictionary gaps (Excel)",
                                        data=dataframe_to_excel_bytes(gap_export_df, sheet_name="dictionary_gaps"),
                                        file_name=f"ozon_dictionary_gaps_{int(selected_row['description_category_id'])}_{int(selected_row['type_id'])}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key=f"ozon_bulk_gap_export_{selected_product_id}",
                                    )
                                else:
                                    st.success("Dictionary gaps по выбранным товарам не найдено.")

                        payload_preview = build_product_ozon_api_attributes(
                            conn,
                            product_id=int(selected_product_id),
                            description_category_id=int(selected_row["description_category_id"]),
                            type_id=int(selected_row["type_id"]),
                            required_only=required_only_mode,
                            dictionary_min_score=float(dictionary_min_score),
                            offer_id_field=str(offer_id_field),
                        )
                        st.download_button(
                            "Скачать preview Ozon JSON",
                            data=json.dumps(payload_preview, ensure_ascii=False, indent=2).encode("utf-8"),
                            file_name=f"ozon_payload_preview_product_{int(selected_product_id)}.json",
                            mime="application/json",
                        )
                        if selected_product_ids:
                            bulk_payload = build_bulk_ozon_api_payloads(
                                conn,
                                product_ids=[int(x) for x in selected_product_ids],
                                description_category_id=int(selected_row["description_category_id"]),
                                type_id=int(selected_row["type_id"]),
                                required_only=required_only_mode,
                                dictionary_min_score=float(dictionary_min_score),
                                offer_id_field=str(offer_id_field),
                            )
                            bulk_result_df = pd.DataFrame()
                            st.download_button(
                                "Скачать bulk Ozon JSON по выбранным товарам",
                                data=json.dumps(bulk_payload, ensure_ascii=False, indent=2).encode("utf-8"),
                                file_name=f"ozon_bulk_payload_{int(selected_row['description_category_id'])}_{int(selected_row['type_id'])}.json",
                                mime="application/json",
                                key=f"ozon_bulk_payload_export_{selected_product_id}",
                            )
                            bulk_products = bulk_payload.get("products") or []
                            if bulk_products:
                                bulk_result_df = pd.DataFrame(
                                    [
                                        {
                                            "product_id": int(item.get("product_id") or 0),
                                            "offer_id": item.get("offer_id"),
                                            "offer_id_field": item.get("offer_id_field"),
                                            "included_attributes": int(item.get("included") or 0),
                                            "skipped_attributes": int(item.get("skipped") or 0),
                                            "description_category_id": int(item.get("description_category_id") or 0),
                                            "type_id": int(item.get("type_id") or 0),
                                        }
                                        for item in bulk_products
                                    ]
                                )
                                st.download_button(
                                    "Скачать результат bulk обработки (Excel)",
                                    data=dataframe_to_excel_bytes(bulk_result_df, sheet_name="ozon_bulk_result"),
                                    file_name=f"ozon_bulk_result_{int(selected_row['description_category_id'])}_{int(selected_row['type_id'])}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"ozon_bulk_result_export_{selected_product_id}",
                                )
                            update_request = build_ozon_attributes_update_request(bulk_payload)
                            st.download_button(
                                "Скачать request JSON для /v1/product/attributes/update",
                                data=json.dumps(update_request, ensure_ascii=False, indent=2).encode("utf-8"),
                                file_name=f"ozon_attributes_update_request_{int(selected_row['description_category_id'])}_{int(selected_row['type_id'])}.json",
                                mime="application/json",
                                key=f"ozon_update_request_export_{selected_product_id}",
                            )
                            update_items = update_request.get("items") or []
                            update_items_df = pd.DataFrame(
                                [
                                    {
                                        "offer_id": item.get("offer_id"),
                                        "description_category_id": item.get("description_category_id"),
                                        "type_id": item.get("type_id"),
                                        "attributes_count": len(item.get("attributes") or []),
                                    }
                                    for item in update_items
                                ]
                            )
                            update_summary_df = pd.DataFrame(
                                [
                                    {
                                        "products_total": int((bulk_payload.get("summary") or {}).get("products_total") or 0),
                                        "attributes_included": int((bulk_payload.get("summary") or {}).get("attributes_included") or 0),
                                        "attributes_skipped": int((bulk_payload.get("summary") or {}).get("attributes_skipped") or 0),
                                        "missing_offer_id": int((bulk_payload.get("summary") or {}).get("missing_offer_id") or 0),
                                        "request_items": int((update_request.get("summary") or {}).get("items_total") or 0),
                                        "request_skipped_missing_offer": int((update_request.get("summary") or {}).get("skipped_missing_offer") or 0),
                                        "request_skipped_empty_attrs": int((update_request.get("summary") or {}).get("skipped_empty_attrs") or 0),
                                    }
                                ]
                            )
                            st.download_button(
                                "Скачать Ozon bulk пакет (Excel)",
                                data=dataframes_to_excel_bytes(
                                    {
                                        "bulk_result": bulk_result_df,
                                        "update_items": update_items_df,
                                        "update_summary": update_summary_df,
                                    }
                                ),
                                file_name=f"ozon_bulk_package_{int(selected_row['description_category_id'])}_{int(selected_row['type_id'])}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"ozon_bulk_package_export_{selected_product_id}",
                            )
                            if st.button(
                                "Отправить batch в Ozon (/v1/product/attributes/update)",
                                disabled=(not configured),
                                key=f"ozon_send_update_{selected_product_id}",
                            ):
                                send_result = submit_ozon_attributes_update(
                                    conn=conn,
                                    product_ids=[int(x) for x in selected_product_ids],
                                    description_category_id=int(selected_row["description_category_id"]),
                                    type_id=int(selected_row["type_id"]),
                                    required_only=required_only_mode,
                                    dictionary_min_score=float(dictionary_min_score),
                                    offer_id_field=str(offer_id_field),
                                    client_id=client_id or None,
                                    api_key=api_key or None,
                                )
                                if send_result.get("ok"):
                                    response = send_result.get("response") or {}
                                    result_part = response.get("result") if isinstance(response, dict) else None
                                    task_id = result_part.get("task_id") if isinstance(result_part, dict) else None
                                    st.success(
                                        f"Batch отправлен в Ozon. items={send_result.get('request', {}).get('summary', {}).get('items_total', 0)}"
                                        + (f", task_id={task_id}" if task_id else "")
                                    )
                                else:
                                    st.error(send_result.get("message") or "Не удалось отправить batch в Ozon")
                            st.caption(
                                f"Bulk summary: products={bulk_payload.get('summary', {}).get('products_total', 0)}, "
                                f"included={bulk_payload.get('summary', {}).get('attributes_included', 0)}, "
                                f"skipped={bulk_payload.get('summary', {}).get('attributes_skipped', 0)}, "
                                f"missing_offer_id={bulk_payload.get('summary', {}).get('missing_offer_id', 0)} | "
                                f"request_items={update_request.get('summary', {}).get('items_total', 0)}"
                            )

                        st.markdown("### Preview полуавтозаполнения Ozon")
                        st.dataframe(
                            preview_df[
                                [
                                    c
                                    for c in [
                                        "attribute_id",
                                        "dictionary_id",
                                        "name",
                                        "is_required",
                                        "source_type",
                                        "source_name",
                                        "transform_rule",
                                        "status",
                                        "value",
                                        "dictionary_value_id",
                                        "dictionary_match_score",
                                        "dictionary_match_by",
                                    ]
                                    if c in preview_df.columns
                                ]
                            ],
                            use_container_width=True,
                            hide_index=True,
                        )

                        dict_unmatched_count = int((preview_df["status"] == "dictionary_unmatched").sum())
                        if dict_unmatched_count > 0:
                            st.markdown("### Подсказки по dictionary mismatch")
                            st.caption("Для несопоставленных значений система предлагает ближайшие варианты из кэша справочника Ozon.")
                            top_n = st.number_input(
                                "Сколько вариантов показывать на один атрибут",
                                min_value=1,
                                max_value=10,
                                value=3,
                                step=1,
                                key=f"ozon_dict_gap_topn_{selected_product_id}",
                            )
                            gap_rows = preview_product_ozon_dictionary_gaps(
                                conn,
                                product_id=int(selected_product_id),
                                description_category_id=int(selected_row["description_category_id"]),
                                type_id=int(selected_row["type_id"]),
                                top_n=int(top_n),
                                dictionary_min_score=float(dictionary_min_score),
                            )
                            if gap_rows:
                                gap_df = pd.DataFrame(gap_rows)
                                st.dataframe(
                                    gap_df[
                                        [
                                            c
                                            for c in [
                                                "attribute_id",
                                                "name",
                                                "source_name",
                                                "raw_value",
                                                "suggestion_values",
                                            ]
                                            if c in gap_df.columns
                                        ]
                                    ],
                                    use_container_width=True,
                                    hide_index=True,
                                )

                                gap_options = list(range(len(gap_rows)))
                                selected_gap_idx = st.selectbox(
                                    "Выбери проблемное значение для dictionary override",
                                    options=gap_options,
                                    format_func=lambda idx: (
                                        f"attr={gap_rows[idx].get('attribute_id')} | "
                                        f"{gap_rows[idx].get('name')} | raw={gap_rows[idx].get('raw_value')}"
                                    ),
                                    key=f"ozon_override_gap_idx_{selected_product_id}",
                                )
                                selected_gap = gap_rows[int(selected_gap_idx)]
                                selected_gap_suggestions = selected_gap.get("suggestions") or []
                                if selected_gap_suggestions:
                                    suggestion_options = list(range(len(selected_gap_suggestions)))
                                    selected_suggestion_idx = st.selectbox(
                                        "Подходящее значение из справочника",
                                        options=suggestion_options,
                                        format_func=lambda idx: (
                                            f"{selected_gap_suggestions[idx].get('value')} "
                                            f"(id={selected_gap_suggestions[idx].get('value_id')}, s={selected_gap_suggestions[idx].get('score')})"
                                        ),
                                        key=f"ozon_override_suggestion_idx_{selected_product_id}",
                                    )
                                    override_comment = st.text_input(
                                        "Комментарий к override (необязательно)",
                                        value="Сохранено из блока dictionary mismatch",
                                        key=f"ozon_override_comment_{selected_product_id}",
                                    )
                                    if st.button("Сохранить dictionary override", key=f"ozon_save_override_{selected_product_id}"):
                                        picked = selected_gap_suggestions[int(selected_suggestion_idx)]
                                        save_dictionary_override(
                                            conn=conn,
                                            description_category_id=int(selected_row["description_category_id"]),
                                            type_id=int(selected_row["type_id"]),
                                            attribute_id=int(selected_gap.get("attribute_id")),
                                            raw_value=selected_gap.get("raw_value"),
                                            value_id=int(picked.get("value_id")),
                                            value=picked.get("value"),
                                            comment=override_comment or None,
                                        )
                                        st.success(
                                            f"Override сохранён: raw='{selected_gap.get('raw_value')}' -> "
                                            f"id={picked.get('value_id')} ({picked.get('value')})"
                                        )
                                        st.rerun()
                                else:
                                    st.info("Для выбранного raw-значения пока нет кандидатов из справочника.")
                            else:
                                st.info("Несматченные dictionary-значения не найдены.")

                        overrides = list_dictionary_overrides(
                            conn=conn,
                            description_category_id=int(selected_row["description_category_id"]),
                            type_id=int(selected_row["type_id"]),
                            limit=200,
                        )
                        st.markdown("### Excel: массовый импорт dictionary overrides")
                        ov1, ov2 = st.columns([2, 1])
                        with ov1:
                            overrides_excel = st.file_uploader(
                                "Загрузи Excel с overrides",
                                type=["xlsx", "xls"],
                                key=f"ozon_overrides_excel_{selected_product_id}",
                            )
                        with ov2:
                            st.download_button(
                                "Скачать шаблон overrides",
                                data=build_ozon_dictionary_overrides_template_excel(),
                                file_name="ozon_dictionary_overrides_template.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"ozon_overrides_template_{selected_product_id}",
                            )
                            if st.button("Импортировать overrides из Excel", key=f"ozon_import_overrides_{selected_product_id}"):
                                if overrides_excel is None:
                                    st.warning("Сначала загрузи Excel файл с overrides.")
                                else:
                                    import_result = import_dictionary_overrides_from_excel(
                                        conn=conn,
                                        file_bytes=overrides_excel.read(),
                                        description_category_id=int(selected_row["description_category_id"]),
                                        type_id=int(selected_row["type_id"]),
                                    )
                                    if import_result.get("ok"):
                                        st.success(
                                            f"Импорт завершён: применено {import_result.get('applied', 0)}, "
                                            f"пропущено {import_result.get('skipped', 0)}."
                                        )
                                        errors = import_result.get("errors") or []
                                        if errors:
                                            st.dataframe(pd.DataFrame(errors), use_container_width=True, hide_index=True)
                                        st.rerun()
                                    else:
                                        st.error(import_result.get("message") or "Не удалось импортировать overrides.")

                        if overrides:
                            st.markdown("### Сохранённые dictionary overrides")
                            overrides_df = pd.DataFrame(overrides)
                            st.download_button(
                                "Скачать overrides (Excel)",
                                data=dataframe_to_excel_bytes(overrides_df, sheet_name="overrides"),
                                file_name=f"ozon_dictionary_overrides_{int(selected_row['description_category_id'])}_{int(selected_row['type_id'])}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"ozon_overrides_export_{selected_product_id}",
                            )
                            st.dataframe(
                                overrides_df[
                                    [
                                        c
                                        for c in [
                                            "attribute_id",
                                            "raw_value",
                                            "value_id",
                                            "value",
                                            "comment",
                                            "updated_at",
                                        ]
                                        if c in overrides_df.columns
                                    ]
                                ],
                                use_container_width=True,
                                hide_index=True,
                            )
                            selected_override_idx = st.selectbox(
                                "Выбери override для удаления",
                                options=list(range(len(overrides))),
                                format_func=lambda idx: (
                                    f"attr={overrides[idx].get('attribute_id')} | "
                                    f"raw={overrides[idx].get('raw_value')} -> "
                                    f"id={overrides[idx].get('value_id')} ({overrides[idx].get('value')})"
                                ),
                                key=f"ozon_override_delete_idx_{selected_product_id}",
                            )
                            if st.button("Удалить выбранный override", key=f"ozon_delete_override_{selected_product_id}"):
                                item = overrides[int(selected_override_idx)]
                                result = delete_dictionary_override(
                                    conn=conn,
                                    description_category_id=int(selected_row["description_category_id"]),
                                    type_id=int(selected_row["type_id"]),
                                    attribute_id=int(item.get("attribute_id")),
                                    raw_value=item.get("raw_value"),
                                )
                                st.success(f"Удалено overrides: {int(result.get('deleted') or 0)}")
                                st.rerun()

                        st.markdown("### Журнал отправок в Ozon")
                        retry_col1, retry_col2 = st.columns([2, 1])
                        with retry_col1:
                            retry_excel_file = st.file_uploader(
                                "Excel со списком job_id для повторной отправки",
                                type=["xlsx", "xls"],
                                key=f"ozon_retry_jobs_file_{selected_product_id}",
                            )
                            retry_excel_column = st.text_input(
                                "Колонка job_id (опционально)",
                                value="",
                                key=f"ozon_retry_jobs_column_{selected_product_id}",
                                placeholder="job_id",
                            )
                        with retry_col2:
                            st.download_button(
                                "Скачать шаблон job_id",
                                data=build_ozon_retry_jobs_template_excel(),
                                file_name="ozon_retry_jobs_template.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"ozon_retry_jobs_template_{selected_product_id}",
                            )
                            if st.button(
                                "Массово повторить jobs из Excel",
                                disabled=(not configured),
                                key=f"ozon_retry_jobs_apply_{selected_product_id}",
                            ):
                                if retry_excel_file is None:
                                    st.warning("Сначала загрузи Excel со списком job_id.")
                                else:
                                    parsed_jobs = resolve_job_ids_from_excel(
                                        retry_excel_file.read(),
                                        column_name=retry_excel_column or None,
                                    )
                                    if not parsed_jobs.get("ok"):
                                        st.error(parsed_jobs.get("message") or "Не удалось прочитать job_id из Excel.")
                                    else:
                                        job_ids = parsed_jobs.get("job_ids") or []
                                        if not job_ids:
                                            st.warning("В Excel не найдено корректных job_id.")
                                        else:
                                            progress = st.progress(0)
                                            ok_count = 0
                                            err_count = 0
                                            errors = []
                                            retry_rows = []
                                            for i, job_id in enumerate(job_ids, start=1):
                                                result = retry_ozon_update_job(
                                                    conn=conn,
                                                    job_id=int(job_id),
                                                    client_id=client_id or None,
                                                    api_key=api_key or None,
                                                )
                                                if result.get("ok"):
                                                    ok_count += 1
                                                    retry_rows.append(
                                                        {
                                                            "job_id": int(job_id),
                                                            "status": "success",
                                                            "task_id": result.get("task_id"),
                                                            "error": None,
                                                        }
                                                    )
                                                else:
                                                    err_count += 1
                                                    err_msg = result.get("message") or "Ошибка"
                                                    errors.append({"job_id": job_id, "error": err_msg})
                                                    retry_rows.append(
                                                        {
                                                            "job_id": int(job_id),
                                                            "status": "error",
                                                            "task_id": None,
                                                            "error": err_msg,
                                                        }
                                                    )
                                                progress.progress(i / len(job_ids))
                                            st.success(f"Массовый retry завершён. Успешно: {ok_count}, с ошибкой: {err_count}.")
                                            if retry_rows:
                                                retry_df = pd.DataFrame(retry_rows)
                                                st.dataframe(retry_df, use_container_width=True, hide_index=True)
                                                st.download_button(
                                                    "Скачать результат retry (Excel)",
                                                    data=dataframe_to_excel_bytes(retry_df, sheet_name="retry_result"),
                                                    file_name="ozon_retry_result.xlsx",
                                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                                    key=f"ozon_retry_result_export_{selected_product_id}",
                                                )
                                            if parsed_jobs.get("errors"):
                                                st.dataframe(pd.DataFrame(parsed_jobs.get("errors")), use_container_width=True, hide_index=True)
                                            if errors:
                                                err_df = pd.DataFrame(errors)
                                                st.dataframe(err_df, use_container_width=True, hide_index=True)
                                                st.download_button(
                                                    "Скачать ошибки retry (Excel)",
                                                    data=dataframe_to_excel_bytes(err_df, sheet_name="retry_errors"),
                                                    file_name="ozon_retry_errors.xlsx",
                                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                                    key=f"ozon_retry_errors_export_{selected_product_id}",
                                                )
                                            st.rerun()
                        jobs_limit = st.number_input(
                            "Сколько последних отправок показывать",
                            min_value=10,
                            max_value=500,
                            value=50,
                            step=10,
                            key=f"ozon_jobs_limit_{selected_product_id}",
                        )
                        jobs = list_ozon_update_jobs(conn, limit=int(jobs_limit))
                        if jobs:
                            jobs_df = pd.DataFrame(jobs)
                            st.download_button(
                                "Скачать журнал jobs (Excel)",
                                data=dataframe_to_excel_bytes(jobs_df, sheet_name="ozon_jobs"),
                                file_name="ozon_update_jobs.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"ozon_jobs_export_{selected_product_id}",
                            )
                            jm1, jm2, jm3, jm4 = st.columns(4)
                            jm1.metric("Всего jobs", int(len(jobs_df)))
                            jm2.metric("Успех", int((jobs_df["status"] == "success").sum()) if "status" in jobs_df.columns else 0)
                            jm3.metric("Ошибка", int((jobs_df["status"] == "error").sum()) if "status" in jobs_df.columns else 0)
                            jm4.metric("Skipped", int((jobs_df["status"] == "skipped").sum()) if "status" in jobs_df.columns else 0)

                            status_filter = st.selectbox(
                                "Фильтр jobs по статусу",
                                options=["Все", "success", "error", "skipped"],
                                index=0,
                                key=f"ozon_jobs_status_filter_{selected_product_id}",
                            )
                            if status_filter != "Все":
                                jobs_df = jobs_df[jobs_df["status"] == status_filter]

                            st.dataframe(
                                jobs_df[
                                    [
                                        c
                                        for c in [
                                            "id",
                                            "status",
                                            "items_count",
                                            "description_category_id",
                                            "type_id",
                                            "offer_id_field",
                                            "task_id",
                                            "retry_of_job_id",
                                            "error_message",
                                            "created_at",
                                        ]
                                        if c in jobs_df.columns
                                    ]
                                ],
                                use_container_width=True,
                                hide_index=True,
                            )
                            if jobs_df.empty:
                                st.info("По текущему фильтру jobs не найдено.")
                            else:
                                retry_all_col1, retry_all_col2 = st.columns([1, 2])
                                with retry_all_col1:
                                    if st.button(
                                        "Повторить все jobs из фильтра",
                                        disabled=(not configured),
                                        key=f"ozon_retry_filtered_jobs_{selected_product_id}",
                                    ):
                                        filtered_ids = [int(jid) for jid in jobs_df["id"].tolist()]
                                        progress = st.progress(0)
                                        ok_count = 0
                                        err_rows = []
                                        for i, jid in enumerate(filtered_ids, start=1):
                                            res = retry_ozon_update_job(
                                                conn=conn,
                                                job_id=int(jid),
                                                client_id=client_id or None,
                                                api_key=api_key or None,
                                            )
                                            if res.get("ok"):
                                                ok_count += 1
                                            else:
                                                err_rows.append({"job_id": int(jid), "error": res.get("message") or "Ошибка"})
                                            progress.progress(i / len(filtered_ids))
                                        st.success(
                                            f"Retry по фильтру завершён. Успешно: {ok_count}, ошибок: {len(err_rows)}."
                                        )
                                        if err_rows:
                                            err_df = pd.DataFrame(err_rows)
                                            st.dataframe(err_df, use_container_width=True, hide_index=True)
                                            st.download_button(
                                                "Скачать ошибки retry по фильтру (Excel)",
                                                data=dataframe_to_excel_bytes(err_df, sheet_name="retry_errors"),
                                                file_name="ozon_retry_filtered_errors.xlsx",
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                                key=f"ozon_retry_filtered_errors_{selected_product_id}",
                                            )
                                        st.rerun()
                                with retry_all_col2:
                                    st.caption("Кнопка повторяет все jobs, которые видны после текущего фильтра статуса.")

                                selected_job_id = st.selectbox(
                                    "Job для действий",
                                    options=[int(jid) for jid in jobs_df["id"].tolist()],
                                    format_func=lambda jid: next(
                                        (
                                            f"#{j['id']} | {j.get('status')} | items={j.get('items_count')} | created={j.get('created_at')}"
                                            for j in jobs
                                            if int(j["id"]) == int(jid)
                                        ),
                                        str(jid),
                                    ),
                                    key=f"ozon_job_action_{selected_product_id}",
                                )
                                job_item = get_ozon_update_job(conn, int(selected_job_id))
                                if job_item:
                                    a1, a2, a3 = st.columns(3)
                                    with a1:
                                        request_bytes = (job_item.get("request_json") or "{}").encode("utf-8")
                                        st.download_button(
                                            "Скачать request job",
                                            data=request_bytes,
                                            file_name=f"ozon_job_{int(selected_job_id)}_request.json",
                                            mime="application/json",
                                            key=f"ozon_job_req_dl_{selected_product_id}",
                                        )
                                    with a2:
                                        response_bytes = (job_item.get("response_json") or "{}").encode("utf-8")
                                        st.download_button(
                                            "Скачать response job",
                                            data=response_bytes,
                                            file_name=f"ozon_job_{int(selected_job_id)}_response.json",
                                            mime="application/json",
                                            key=f"ozon_job_resp_dl_{selected_product_id}",
                                        )
                                    with a3:
                                        if st.button(
                                            "Повторить отправку job",
                                            disabled=(not configured),
                                            key=f"ozon_job_retry_{selected_product_id}",
                                        ):
                                            retry_result = retry_ozon_update_job(
                                                conn=conn,
                                                job_id=int(selected_job_id),
                                                client_id=client_id or None,
                                                api_key=api_key or None,
                                            )
                                            if retry_result.get("ok"):
                                                st.success(
                                                    "Повторная отправка выполнена"
                                                    + (f", task_id={retry_result.get('task_id')}" if retry_result.get("task_id") else "")
                                                )
                                            else:
                                                st.error(retry_result.get("message") or "Не удалось повторить отправку job")
                                            st.rerun()
                                    job_items = list_ozon_update_job_items(conn, int(selected_job_id), limit=10000)
                                    try:
                                        job_response = json.loads(job_item.get("response_json") or "{}")
                                    except Exception:
                                        job_response = {}
                                    if job_items:
                                        job_items_df = pd.DataFrame(job_items)
                                        st.download_button(
                                            "Скачать selected job items (Excel)",
                                            data=dataframe_to_excel_bytes(job_items_df, sheet_name="job_items"),
                                            file_name=f"ozon_job_{int(selected_job_id)}_items.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key=f"ozon_job_items_excel_{selected_product_id}",
                                        )
                                        st.dataframe(job_items_df, use_container_width=True, hide_index=True)
                                    if job_response:
                                        response_df = pd.DataFrame([job_response])
                                        st.download_button(
                                            "Скачать selected job response (Excel)",
                                            data=dataframe_to_excel_bytes(response_df, sheet_name="job_response"),
                                            file_name=f"ozon_job_{int(selected_job_id)}_response.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key=f"ozon_job_response_excel_{selected_product_id}",
                                        )
                        else:
                            st.info("Отправок в Ozon пока не было.")
            else:
                st.info("По этой категории атрибуты ещё не загружались.")
    else:
        st.info("Кэш категорий пока пуст. Сначала синхронизируй дерево Ozon.")

    conn.close()


def show_channels_tab():
    conn = get_db()
    st.subheader("Каналы")
    st.caption("Здесь настраиваются требования и mapping rules для клиентов и каналов. Это служебный слой, который управляет экспортом.")

    channels = conn.execute(
        "SELECT channel_code, channel_name, is_active FROM channel_profiles ORDER BY channel_name"
    ).fetchall()
    channel_df = pd.DataFrame([dict(r) for r in channels]) if channels else pd.DataFrame()
    st.subheader("Каналы")
    if not channel_df.empty:
        st.dataframe(channel_df, use_container_width=True, hide_index=True)

    channel_code = st.text_input("Channel code", value="detmir")
    category_code = st.text_input("Category code", value="bicycle")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Требования канала")
        reqs = list_channel_requirements(conn, channel_code=channel_code, category_code=category_code or None)
        if reqs:
            st.dataframe(pd.DataFrame(reqs), use_container_width=True, hide_index=True)

        defs = list_attribute_definitions(conn)
        def_codes = [d["code"] for d in defs] if defs else []

        with st.form("channel_req_form"):
            attribute_code = st.selectbox("Обязательный атрибут", def_codes) if def_codes else st.text_input("Код атрибута")
            is_required = st.checkbox("Обязательный", value=True)
            sort_order = st.number_input("Порядок", min_value=1, value=100, step=1)
            notes = st.text_input("Комментарий")
            save_req = st.form_submit_button("Сохранить требование")

            if save_req and attribute_code:
                upsert_channel_attribute_requirement(
                    conn=conn,
                    channel_code=channel_code,
                    category_code=category_code or None,
                    attribute_code=attribute_code,
                    is_required=1 if is_required else 0,
                    sort_order=int(sort_order),
                    notes=notes or None,
                )
                st.success("Требование сохранено")
                st.rerun()

    with col2:
        st.markdown("### Mapping rules")
        rules = list_channel_mapping_rules(conn, channel_code=channel_code, category_code=category_code or None)
        if rules:
            st.dataframe(pd.DataFrame(rules), use_container_width=True, hide_index=True)

        defs = list_attribute_definitions(conn)
        def_codes = [d["code"] for d in defs] if defs else []

        with st.form("channel_rule_form"):
            target_field = st.text_input("Поле канала")
            source_type = st.selectbox("Источник", ["attribute", "column", "constant"])
            source_name = st.selectbox("Source name", def_codes) if source_type == "attribute" and def_codes else st.text_input("Source name")
            transform_rule = st.text_input("Transform rule")
            is_required = st.checkbox("Обязательное поле", value=False)
            save_rule = st.form_submit_button("Сохранить mapping")

            if save_rule and target_field and source_name:
                upsert_channel_mapping_rule(
                    conn=conn,
                    channel_code=channel_code,
                    category_code=category_code or None,
                    target_field=target_field.strip(),
                    source_type=source_type,
                    source_name=str(source_name).strip(),
                    transform_rule=transform_rule or None,
                    is_required=1 if is_required else 0,
                )
                st.success("Mapping сохранён")
                st.rerun()

    conn.close()


def main():
    st.title("📦 PIM")
    st.caption("PIM для контент-отдела: мастер-карточка, обогащение от поставщика, клиентские шаблоны и экспорт без лишнего ручного труда.")
    active_db = _get_active_db_path() or str(Path("data/catalog.db"))
    st.caption(f"Текущая база данных: `{active_db}`")
    low_db = str(active_db).lower()
    if "\\temp\\pim\\catalog.db" in low_db or "/tmp/pim/catalog.db" in low_db:
        st.warning("Сейчас используется временная БД. Чтобы каталог не пропадал, задай постоянный путь через переменную окружения `PIM_DB_PATH`.")

    with st.expander("Как здесь работать", expanded=False):
        st.markdown(
            """
1. **Импорт**: загрузи Excel, выбери авто/ручной режим, при необходимости задай поставщика и URL-шаблон (`{article}`, `{supplier_article}`), затем выполни импорт.
2. **Импорт → Ozon**: после импорта можно сразу автопривязать товары к Ozon категориям (если синхронизирован кэш категорий во вкладке Ozon).
3. **Каталог**: работай постранично (страницы + размер), фильтруй по статусу парсинга, запускай массовый supplier enrichment и Ozon-автопривязку по текущей странице.
4. **Карточка**: дозаполни вручную, смотри источники данных, запусти парсинг поставщика и проверь/скорректируй Ozon category (id/type/path/confidence).
5. **Атрибуты**: поддерживай master/channel атрибуты, которые участвуют в автоматчинге шаблонов.
6. **Клиентский шаблон**: загрузи шаблон клиента, выбери категорию, проверь авто-матчинг, при необходимости поправь руками и сохрани профиль.
7. **Клиентский шаблон → новые поля**: если в шаблоне есть новые характеристики, кнопка добавит их в master-атрибуты и сразу предложит маппинг.
8. **Экспорт Excel**: заполни выбранные товары, проверь gap/readiness, выгрузи готовый Excel в исходной структуре клиента.
9. **Ozon**: синхронизируй дерево/атрибуты, используй Ozon как эталон структуры, импортируй атрибуты в PIM и контролируй покрытие.
10. **Каналы**: поддерживай channel requirements и mapping rules для повторного безручного экспорта.
            """
        )
    render_section_help()

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
        ["📥 Импорт", "📚 Каталог", "🧾 Карточка", "🧩 Атрибуты", "🧠 Клиентский шаблон", "🛒 Ozon", "⚙️ Каналы"]
    )

    with tab1:
        show_import_tab()

    with tab2:
        show_catalog_tab()

    with tab3:
        show_product_tab()

    with tab4:
        show_attributes_tab()

    with tab5:
        show_template_tab()

    with tab6:
        show_ozon_tab()

    with tab7:
        show_channels_tab()


if __name__ == "__main__":
    main()
