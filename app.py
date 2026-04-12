from __future__ import annotations

import json
from io import BytesIO
import math
from pathlib import Path

import pandas as pd
import sqlite3
import streamlit as st
from openpyxl import load_workbook

from db import get_connection, init_db
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
from services.supplier_parser import fetch_supplier_page, extract_supplier_data, normalize_supplier_data
from services.template_matching import auto_match_template_columns, apply_saved_mapping_rules, fill_template_dataframe, apply_client_validated_values, fill_template_workbook_bytes, dataframe_to_excel_bytes, detect_template_data_start_row
from services.template_profiles import save_template_profile, list_template_profiles, get_template_profile_columns
from services.readiness_service import analyze_template_readiness
from services.ozon_api_service import is_configured, sync_category_tree, list_cached_categories, sync_category_attributes, list_cached_attributes, sync_attribute_dictionary_values, sync_all_category_dictionary_values, list_cached_attribute_values, import_cached_attributes_to_pim, suggest_mappings_for_cached_attributes, save_suggested_mappings, analyze_product_ozon_coverage, ensure_ozon_master_attributes, build_product_ozon_payload, materialize_product_ozon_attributes, preview_product_ozon_dictionary_gaps, build_product_ozon_api_attributes, build_bulk_ozon_api_payloads, build_ozon_attributes_update_request, submit_ozon_attributes_update, list_ozon_update_jobs, get_ozon_update_job, retry_ozon_update_job, list_ozon_update_job_items, save_dictionary_override, list_dictionary_overrides, delete_dictionary_override
from services.ozon_category_match import bulk_assign_ozon_categories

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
    "image_1",
    "image_2",
    "image_3",
    "image_4",
    "image_5",
]


def get_db():
    conn = get_connection()
    init_db(conn)
    return conn


def to_attribute_code(name: str) -> str:
    clean = str(name or "").strip().lower()
    clean = "_".join("".join(ch if ch.isalnum() else " " for ch in clean).split())
    return clean[:120]


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
        where.append("(category = ? OR base_category = ?)")
        params.extend([category, category])

    if supplier:
        where.append("supplier_name = ?")
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
    st.download_button(
        "Скачать шаблон импорта поставщика (Excel)",
        data=build_supplier_catalog_template_excel(),
        file_name="supplier_catalog_import_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="supplier_import_template",
    )
    uploaded = st.file_uploader("Excel файл", type=["xlsx", "xls"])
    s1, s2 = st.columns(2)
    with s1:
        default_supplier_name = st.text_input(
            "Поставщик по умолчанию",
            value=st.session_state.get("import_default_supplier_name", ""),
            placeholder="Например: Rockbros / SKS",
            help="Если в файле нет колонки Поставщик, это значение будет проставлено автоматически.",
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

        if st.button("Импортировать", type="primary"):
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
                        ozon_match_result = bulk_assign_ozon_categories(conn, batch_ids, min_score=0.28, force=False)
                        if ozon_match_result.get("message"):
                            st.info(str(ozon_match_result["message"]))
                        else:
                            st.caption(
                                f"Ozon автопривязка: обработано {ozon_match_result['processed']}, "
                                f"привязано {ozon_match_result['assigned']}, пропущено {ozon_match_result['skipped']}"
                            )
                batch_df = load_products(conn, limit=1000, import_batch_id=result.batch_id)
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
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Импортировано", int(result.imported))
            c2.metric("Создано", int(result.created))
            c3.metric("Обновлено", int(result.updated))
            c4.metric("Дублей", int(len(result.duplicates)))

            st.markdown("### Последняя загруженная партия")
            if not batch_df.empty:
                st.dataframe(batch_df, use_container_width=True, hide_index=True)
            else:
                st.info("В текущей партии нет отображаемых записей. Попробуй ручной выбор листа и строки заголовка.")

            if result.duplicates:
                st.dataframe(pd.DataFrame(result.duplicates), use_container_width=True)


def show_catalog_tab():
    conn = get_db()
    st.subheader("Каталог")
    st.caption("Здесь быстрый контроль по каталогу: поиск, последняя загрузка, статус supplier enrichment и переход в карточку товара.")

    c1, c2, c3, c4, c5, c6 = st.columns([2, 1, 1, 1, 1, 1])
    with c1:
        search = st.text_input("Поиск", placeholder="Название / артикул / штрихкод")
    with c2:
        category = st.text_input("Категория")
    with c3:
        supplier = st.text_input("Поставщик")
    with c4:
        page_size = st.selectbox("Размер страницы", options=[50, 100, 200, 500], index=1)
    with c5:
        only_last_batch = st.checkbox("Только последняя загрузка", value=False)
    with c6:
        parse_filter = st.selectbox("Парсинг", ["Все", "Есть supplier_url", "Не парсено", "Ошибка", "Успех"], index=0)

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
    current_page = int(st.session_state.get("catalog_page", 1))
    if current_page > total_pages:
        current_page = 1
    p1, p2, p3 = st.columns([1, 1, 2])
    with p1:
        page = st.selectbox("Страница", options=page_options, index=page_options.index(current_page), key="catalog_page")
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

    b1, b2 = st.columns(2)
    with b1:
        if st.button("Обновить дубли по текущей выборке"):
            total = 0
            progress = st.progress(0)
            for i, pid in enumerate(ids, start=1):
                refresh_duplicates_for_product(conn, int(pid))
                total += 1
                progress.progress(i / len(ids))
            st.success(f"Проверка дублей завершена: {total} товаров")
    with b2:
        if st.button("Обогатить поставщика по текущей странице"):
            total = 0
            progress = st.progress(0)
            for i, pid in enumerate(ids, start=1):
                product_row = get_product(conn, int(pid))
                if product_row and product_row["supplier_url"]:
                    enrich_product_from_supplier(conn, int(pid), force=False)
                    total += 1
                progress.progress(i / len(ids))
            st.success(f"Обогащение поставщика завершено: обработано {total} товаров")
    cextra1, cextra2 = st.columns(2)
    with cextra1:
        if st.button("Автопривязать Ozon категории (текущая страница)"):
            res = bulk_assign_ozon_categories(conn, [int(x) for x in ids], min_score=0.28, force=False)
            if res.get("message"):
                st.info(str(res["message"]))
            else:
                st.success(f"Ozon автопривязка: обработано {res['processed']}, привязано {res['assigned']}, пропущено {res['skipped']}")
            st.rerun()
    with cextra2:
        if st.button("Перепривязать Ozon категории (force, текущая страница)"):
            res = bulk_assign_ozon_categories(conn, [int(x) for x in ids], min_score=0.28, force=True)
            if res.get("message"):
                st.info(str(res["message"]))
            else:
                st.success(f"Ozon force-привязка: обработано {res['processed']}, привязано {res['assigned']}, пропущено {res['skipped']}")
            st.rerun()

    st.dataframe(df, use_container_width=True, hide_index=True)

    if selected_id:
        st.session_state["selected_product_id"] = int(selected_id)

    conn.close()


def enrich_product_from_supplier(conn, product_id: int, force: bool = False) -> dict:
    product = get_product(conn, product_id)
    if not product:
        return {"ok": False, "message": "Товар не найден"}

    supplier_url = (product["supplier_url"] or "").strip() if product["supplier_url"] else ""
    if not supplier_url:
        return {"ok": False, "message": "У товара нет supplier_url"}

    try:
        html = fetch_supplier_page(supplier_url)
        raw_data = extract_supplier_data(html, supplier_url)
        parsed = normalize_supplier_data(raw_data)

        updates = {}
        skipped_manual_fields = []
        fields = [
            "name",
            "brand",
            "category",
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
            source_field = field
            if field == "gross_weight":
                source_field = "gross_weight"
            new_value = parsed.get(source_field)
            old_value = product[field] if field in product.keys() else None
            if new_value is None:
                continue
            if field_is_manual(conn, product_id, field) and not force:
                skipped_manual_fields.append(field)
                continue
            if not can_overwrite_field(conn, product_id, field, "supplier_page", force=force):
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
            if not can_overwrite_field(conn, product_id, attr_field_name, "supplier_page", force=force):
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
                    (clean_code, str(attr_name).strip(), f"Автосоздано из supplier page: {supplier_url}"),
                )
            set_product_attribute_value(conn, product_id, clean_code, str(attr_value))
            save_field_source(
                conn=conn,
                product_id=product_id,
                field_name=attr_field_name,
                source_type="supplier_page",
                source_value_raw=attr_value,
                source_url=supplier_url,
                confidence=0.6,
            )
            attributes_saved += 1

        if updates:
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            params = list(updates.values()) + ["success", None, product_id]
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
                save_field_source(
                    conn=conn,
                    product_id=product_id,
                    field_name=field_name,
                    source_type="supplier_page",
                    source_value_raw=value,
                    source_url=supplier_url,
                    confidence=0.7,
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
                ("success", "Новых данных для записи не найдено", product_id),
            )

        conn.commit()
        ozon_match = bulk_assign_ozon_categories(conn, [int(product_id)], min_score=0.28, force=False)
        skipped_msg = f", пропущено ручных полей: {len(skipped_manual_fields)}" if skipped_manual_fields else ""
        skipped_attr_msg = f", пропущено атрибутов по приоритету: {len(skipped_attribute_fields)}" if skipped_attribute_fields else ""
        ozon_msg = f", Ozon category match: {ozon_match.get('assigned', 0)}" if ozon_match.get("processed") else ""
        return {
            "ok": True,
            "message": f"Обогащение завершено, обновлено полей: {len(updates)}, атрибутов сохранено: {attributes_saved}{skipped_msg}{skipped_attr_msg}{ozon_msg}",
            "updates": updates,
            "attributes": parsed.get("attributes", {}),
            "image_urls": parsed.get("image_urls", []),
            "skipped_manual_fields": skipped_manual_fields,
            "skipped_attribute_fields": skipped_attribute_fields,
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
    product_id = st.session_state.get("selected_product_id")
    if not product_id:
        st.info("Сначала выбери товар во вкладке Каталог")
        return

    conn = get_db()
    product = get_product(conn, int(product_id))

    if not product:
        st.warning("Товар не найден")
        conn.close()
        return

    st.subheader(f"Карточка товара #{product['id']}")
    st.caption("Мастер-карточка должна быть единым источником правды. Здесь можно вручную поправить данные или обогатить их с сайта поставщика.")

    top1, top2, top3, top4 = st.columns(4)
    top1.metric("Артикул", product["article"] or "-")
    top2.metric("Бренд", product["brand"] or "-")
    top3.metric("Категория", product["base_category"] or product["category"] or "-")
    top4.metric("Поставщик", product["supplier_name"] or "-")

    ctop1, ctop2 = st.columns([1, 1])
    with ctop1:
        if st.button("Спарсить поставщика", type="primary"):
            result = enrich_product_from_supplier(conn, int(product_id), force=False)
            if result["ok"]:
                st.success(result["message"])
                if result.get("updates"):
                    st.json(result["updates"])
                st.rerun()
            else:
                st.error(result["message"])
    with ctop2:
        if st.button("Перезаполнить из поставщика"):
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
        if st.button("Подобрать Ozon категорию"):
            res = bulk_assign_ozon_categories(conn, [int(product_id)], min_score=0.28, force=False)
            if res.get("message"):
                st.info(str(res["message"]))
            else:
                st.success(f"Ozon автопривязка: обработано {res['processed']}, привязано {res['assigned']}, пропущено {res['skipped']}")
            st.rerun()
    with ctop4:
        if st.button("Перепривязать Ozon категорию (force)"):
            res = bulk_assign_ozon_categories(conn, [int(product_id)], min_score=0.28, force=True)
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

    with st.form("product_form"):
        c1, c2, c3 = st.columns(3)

        with c1:
            article = st.text_input("Артикул", value=product["article"] or "")
            internal_article = st.text_input("Внутренний артикул", value=product["internal_article"] or "")
            supplier_article = st.text_input("Артикул поставщика", value=product["supplier_article"] or "")
            name = st.text_input("Название", value=product["name"] or "")
            brand = st.text_input("Бренд", value=product["brand"] or "")
            supplier_name = st.text_input("Поставщик", value=product["supplier_name"] or "")
            barcode = st.text_input("Штрихкод", value=product["barcode"] or "")
            barcode_source = st.text_input("Источник штрихкода", value=product["barcode_source"] or "")

        with c2:
            category = st.text_input("Категория", value=product["category"] or "")
            base_category = st.text_input("Базовая категория", value=product["base_category"] or "")
            subcategory = st.text_input("Подкатегория", value=product["subcategory"] or "")
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
    st.dataframe(pd.DataFrame(source_summary), use_container_width=True, hide_index=True)

    st.markdown("### Все источники данных")
    sources = get_field_sources(conn, int(product_id))
    if sources:
        st.dataframe(pd.DataFrame(sources), use_container_width=True, hide_index=True)
    else:
        st.caption("Источники данных пока не записаны")

    conn.close()


def show_attributes_tab():
    product_id = st.session_state.get("selected_product_id")
    if not product_id:
        st.info("Сначала выбери товар во вкладке Каталог")
        return

    conn = get_db()

    left, right = st.columns([1, 1])

    with left:
        st.subheader("Справочник атрибутов")
        defs = list_attribute_definitions(conn)
        if defs:
            st.dataframe(pd.DataFrame(defs), use_container_width=True, hide_index=True)

        with st.form("new_attribute_def"):
            code = st.text_input("Код атрибута")
            name = st.text_input("Название атрибута")
            data_type = st.selectbox("Тип", ["text", "number", "boolean", "json"])
            scope = st.selectbox("Область", ["master", "channel"])
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
        st.subheader(f"Атрибуты товара #{product_id}")
        values = get_product_attribute_values(conn, int(product_id))
        if values:
            st.dataframe(pd.DataFrame(values), use_container_width=True, hide_index=True)

        defs = list_attribute_definitions(conn)
        def_codes = [d["code"] for d in defs] if defs else []

        with st.form("set_product_attr"):
            attribute_code = st.selectbox("Атрибут", def_codes) if def_codes else st.text_input("Атрибут")
            value = st.text_input("Значение")
            locale = st.text_input("Locale", value="")
            channel_code = st.text_input("Channel code", value="")
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
    conn = get_db()
    product_df = load_products(conn, limit=5000)

    t1, t2 = st.columns([1, 1])
    with t1:
        channel_code = st.text_input("Код клиента / канала", value="onlinetrade", key="template_channel_code")
    with t2:
        known_categories = [""] + sorted([str(x) for x in product_df["category"].dropna().unique().tolist()]) if "product_df" in locals() else [""]
        selected_known = st.selectbox("Категория товаров для шаблона (опционально)", options=known_categories, index=0, key="template_category_select")
        category_code = st.text_input("Категория шаблона/профиля", value=selected_known or "", key="template_category_code")

    p1, p2 = st.columns([1, 1])
    with p1:
        profile_name = st.text_input("Имя профиля шаблона", value=f"{channel_code}_default")
    with p2:
        existing_profiles = list_template_profiles(conn, channel_code=channel_code or None)
        profile_options = [None] + [p["id"] for p in existing_profiles]
        selected_profile_id = st.selectbox(
            "Загрузить сохранённый профиль",
            options=profile_options,
            format_func=lambda x: "-- нет --" if x is None else next((f"{p['profile_name']} (#{p['id']})" for p in existing_profiles if p['id'] == x), str(x)),
        )

    uploaded = st.file_uploader("Загрузить Excel-шаблон клиента", type=["xlsx", "xls"], key="client_template")
    defs = list_attribute_definitions(conn)
    source_options = [("column", c) for c in [
        "article", "name", "barcode", "brand", "description", "weight", "length", "width", "height",
        "package_length", "package_width", "package_height", "gross_weight", "image_url", "category", "supplier_name", "supplier_article", "media_gallery"
    ]] + [("attribute", d["code"]) for d in defs]

    if uploaded is not None:
        uploaded_bytes = uploaded.getvalue()
        workbook = load_workbook(BytesIO(uploaded_bytes), read_only=True, data_only=False)
        template_sheet_options = workbook.sheetnames
        workbook.close()

        template_sheet_name = st.selectbox(
            "Лист шаблона",
            options=template_sheet_options,
            index=(template_sheet_options.index("Товары") if "Товары" in template_sheet_options else 0),
            key="template_sheet_name",
        )
        suggested_data_start_row = detect_template_data_start_row(uploaded_bytes, sheet_name=template_sheet_name)

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

        template_df = pd.read_excel(BytesIO(uploaded_bytes), sheet_name=template_sheet_name)
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

        tab_match, tab_fill, tab_gap = st.tabs(["1. Матчинг", "2. Заполнение и preview", "3. Gap и действия"])

        with tab_match:
            st.markdown("### Колонки шаблона")
            st.dataframe(pd.DataFrame({"template_column": list(template_df.columns)}), use_container_width=True, hide_index=True)

            st.markdown("### Автоматический матчинг")
            st.dataframe(match_df, use_container_width=True, hide_index=True)

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
                        st.session_state[f"tmpl_type_{idx}"] = "attribute"
                        st.session_state[f"tmpl_name_{idx}"] = code
                        created += 1
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
                            uploaded_bytes,
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

    c1, c2 = st.columns(2)
    with c1:
        client_id = st.text_input("Ozon Client ID", value="")
    with c2:
        api_key = st.text_input("Ozon API Key", value="", type="password")

    configured = is_configured(client_id or None, api_key or None)
    if configured:
        st.success("Ozon-креды заданы, можно синхронизировать дерево и атрибуты.")
    else:
        st.warning("Ozon-креды не заданы в этой сессии. Можно вставить их сюда вручную и сразу выполнить sync.")

    top1, top2 = st.columns(2)
    with top1:
        if st.button("Синхронизировать дерево категорий Ozon", type="primary", disabled=not configured):
            result = sync_category_tree(conn, client_id=client_id or None, api_key=api_key or None)
            st.success(f"Дерево категорий обновлено, записей: {result['total']}")
            st.rerun()
    with top2:
        category_limit = st.number_input("Сколько категорий показать", min_value=50, max_value=2000, value=200, step=50)

    categories = list_cached_categories(conn, limit=int(category_limit))
    if categories:
        cat_df = pd.DataFrame(categories)
        st.markdown("### Кэш категорий Ozon")
        st.dataframe(cat_df[[c for c in ["description_category_id", "category_name", "full_path", "type_id", "type_name", "disabled", "fetched_at"] if c in cat_df.columns]], use_container_width=True, hide_index=True)

        valid_rows = [row for row in categories if row.get("description_category_id") and row.get("type_id")]
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
                m4.metric("Базовых master-атрибутов", int(master_seed["total"]))

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
                    mm3.metric("Required без маппинга", int(((mapping_df["is_required"] == 1) & (mapping_df["status"] != "matched")).sum()))

                    st.markdown("### Предлагаемые Ozon mapping rules")
                    st.dataframe(mapping_df[[c for c in ["attribute_id", "name", "group_name", "is_required", "source_type", "source_name", "transform_rule", "matched_by", "status"] if c in mapping_df.columns]], use_container_width=True, hide_index=True)

                st.markdown("### Атрибуты выбранной категории")
                st.dataframe(attr_df[[c for c in ["attribute_id", "name", "group_name", "type", "dictionary_id", "is_required", "is_collection", "max_value_count", "fetched_at"] if c in attr_df.columns]], use_container_width=True, hide_index=True)

                dictionary_attrs = [row for row in attributes if int(row.get("dictionary_id") or 0) > 0]
                if dictionary_attrs:
                    st.markdown("### Справочники значений Ozon")
                    dd1, dd2, dd3 = st.columns(3)
                    dd1.metric("Dictionary-атрибутов", int(len(dictionary_attrs)))
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
                    selected_dict_label = st.selectbox("Dictionary-атрибут", options=dict_options, key="ozon_dict_attr")
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
                        st.dataframe(dict_df[[c for c in ["value_id", "value", "info", "picture", "fetched_at"] if c in dict_df.columns]], use_container_width=True, hide_index=True)
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
                        "Работать только с обязательными Ozon-атрибутами (required)",
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
                        p3.metric("Required ready", int(((preview_df["status"] == "ready") & (preview_df["is_required"] == 1)).sum()))
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
                                cc1.metric("Ready %", int(summary["readiness_pct"]))
                                cc2.metric("Required всего", int(summary["required_total"]))
                                cc3.metric("Required закрыто", int(summary["required_covered"]))
                                cc4.metric("Required пусто", int(summary["required_missing"]))
                                st.caption(f"Required dictionary_unmatched: {int(summary.get('required_dictionary_unmatched') or 0)}")
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
