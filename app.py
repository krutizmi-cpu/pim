from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

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
from services.template_matching import auto_match_template_columns, apply_saved_mapping_rules, fill_template_dataframe, apply_client_validated_values, dataframe_to_excel_bytes
from services.template_profiles import save_template_profile, list_template_profiles, get_template_profile_columns
from services.readiness_service import analyze_template_readiness
from services.ozon_api_service import is_configured, sync_category_tree, list_cached_categories, sync_category_attributes, list_cached_attributes

st.set_page_config(page_title="PIM", page_icon="📦", layout="wide")


def get_db():
    conn = get_connection()
    init_db(conn)
    return conn


def load_products(
    conn,
    search: str = "",
    category: str = "",
    supplier: str = "",
    limit: int = 200,
    import_batch_id: str = "",
) -> pd.DataFrame:
    where = []
    params = []

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
            import_batch_id,
            updated_at
        FROM products
    """

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY id DESC LIMIT ?"
    params.append(int(limit))

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
        "category", "base_category", "subcategory", "wheel_diameter_inch", "supplier_url", "uom",
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
    uploaded = st.file_uploader("Excel файл", type=["xlsx", "xls"])

    if uploaded is not None:
        temp_path = Path("data/_import_temp.xlsx")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_bytes(uploaded.read())

        if st.button("Импортировать", type="primary"):
            conn = get_db()
            result = import_catalog_from_excel(conn, temp_path)
            batch_df = load_products(conn, limit=1000, import_batch_id=result.batch_id)
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
                st.info("В текущей партии нет отображаемых записей")

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
        limit = st.number_input("Показать записей", min_value=50, max_value=1000, value=200, step=50)
    with c5:
        only_last_batch = st.checkbox("Только последняя загрузка", value=False)
    with c6:
        parse_filter = st.selectbox("Парсинг", ["Все", "Есть supplier_url", "Не парсено", "Ошибка", "Успех"], index=0)

    batch_id = st.session_state.get("last_import_batch_id") if only_last_batch else ""
    df = load_products(conn, search=search, category=category, supplier=supplier, limit=int(limit), import_batch_id=batch_id or "")
    if not df.empty:
        if parse_filter == "Есть supplier_url":
            df = df[df["supplier_url"].notna() & (df["supplier_url"].astype(str).str.strip() != "")]
        elif parse_filter == "Не парсено":
            df = df[df["supplier_parse_status"].isna() | (df["supplier_parse_status"].astype(str).str.strip() == "")]
        elif parse_filter == "Ошибка":
            df = df[df["supplier_parse_status"] == "error"]
        elif parse_filter == "Успех":
            df = df[df["supplier_parse_status"] == "success"]

    if df.empty:
        st.info("Нет товаров")
        conn.close()
        return

    if batch_id:
        st.caption("Показана только последняя загруженная партия")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Товаров в выборке", int(len(df)))
    m2.metric("С supplier_url", int((df["supplier_url"].fillna("").astype(str).str.strip() != "").sum()) if "supplier_url" in df.columns else 0)
    m3.metric("Парсинг ок", int((df["supplier_parse_status"] == "success").sum()) if "supplier_parse_status" in df.columns else 0)
    m4.metric("Ошибки парсинга", int((df["supplier_parse_status"] == "error").sum()) if "supplier_parse_status" in df.columns else 0)

    st.download_button(
        "Скачать выборку Excel",
        data=export_current_df(df),
        file_name="pim_products.xlsx",
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
        if st.button("Обогатить поставщика по текущей выборке"):
            total = 0
            progress = st.progress(0)
            for i, pid in enumerate(ids, start=1):
                product_row = get_product(conn, int(pid))
                if product_row and product_row["supplier_url"]:
                    enrich_product_from_supplier(conn, int(pid), force=False)
                    total += 1
                progress.progress(i / len(ids))
            st.success(f"Обогащение поставщика завершено: обработано {total} товаров")

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
        fields = ["description", "image_url", "weight", "length", "width", "height", "package_length", "package_width", "package_height", "gross_weight"]
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
        for attr_name, attr_value in (parsed.get("attributes") or {}).items():
            clean_code = str(attr_name).strip().lower()
            clean_code = "_".join("".join(ch if ch.isalnum() else " " for ch in clean_code).split())
            if not clean_code:
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
                field_name=f"attr:{clean_code}",
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
        skipped_msg = f", пропущено ручных полей: {len(skipped_manual_fields)}" if skipped_manual_fields else ""
        return {
            "ok": True,
            "message": f"Обогащение завершено, обновлено полей: {len(updates)}, атрибутов сохранено: {attributes_saved}{skipped_msg}",
            "updates": updates,
            "attributes": parsed.get("attributes", {}),
            "image_urls": parsed.get("image_urls", []),
            "skipped_manual_fields": skipped_manual_fields,
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

    t1, t2 = st.columns([1, 1])
    with t1:
        channel_code = st.text_input("Код клиента / канала", value="onlinetrade", key="template_channel_code")
    with t2:
        category_code = st.text_input("Категория шаблона", value="", key="template_category_code")

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
    product_df = load_products(conn, limit=1000)
    defs = list_attribute_definitions(conn)
    source_options = [("column", c) for c in [
        "article", "name", "barcode", "brand", "description", "weight", "length", "width", "height",
        "package_length", "package_width", "package_height", "gross_weight", "image_url", "category", "supplier_name", "supplier_article", "media_gallery"
    ]] + [("attribute", d["code"]) for d in defs]

    if uploaded is not None:
        template_df = pd.read_excel(uploaded)
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
            transform_options = ["", "cm_to_mm", "mm_to_cm", "m_to_cm", "kg_to_g", "g_to_kg", "inch_to_cm", "lower", "upper", "strip", "first_image", "join_images", "image_1", "image_2", "image_3", "image_4", "image_5"]
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
                    current_transform = match.get("transform_rule") if match.get("transform_rule") in transform_options else ""
                    transform_rule = st.selectbox("Transform", options=transform_options, index=transform_options.index(current_transform), key=f"tmpl_transform_{idx}")
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

            s1, s2 = st.columns(2)
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

            if not unmatched.empty:
                st.warning(f"Не сматчено колонок: {len(unmatched)}")
                st.dataframe(unmatched[["template_column", "status"]], use_container_width=True, hide_index=True)
            else:
                st.success("Все колонки шаблона сматчены.")

        with tab_fill:
            manual_rows = []
            transform_options = ["", "cm_to_mm", "mm_to_cm", "m_to_cm", "kg_to_g", "g_to_kg", "inch_to_cm", "lower", "upper", "strip", "first_image", "join_images", "image_1", "image_2", "image_3", "image_4", "image_5"]
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
                        st.download_button(
                            "Скачать заполненный шаблон",
                            data=dataframe_to_excel_bytes(filled_df),
                            file_name=f"filled_{channel_code or 'client'}_template.xlsx",
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
                st.markdown("### Атрибуты выбранной категории")
                st.dataframe(attr_df[[c for c in ["attribute_id", "name", "group_name", "type", "dictionary_id", "is_required", "is_collection", "max_value_count", "fetched_at"] if c in attr_df.columns]], use_container_width=True, hide_index=True)
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
1. **Импорт**: загружаем новый каталог или ассортимент.
2. **Каталог**: быстро фильтруем товары, смотрим статус supplier enrichment.
3. **Карточка**: правим мастер-товар и обогащаем его с сайта поставщика.
4. **Атрибуты**: управляем атрибутным слоем.
5. **Клиентский шаблон**: матчим поля, смотрим gap, собираем файл клиента.
6. **Каналы**: настраиваем требования и mapping rules.
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
