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

    sql = """
        SELECT
            id,
            article,
            internal_article,
            supplier_article,
            name,
            brand,
            supplier_name,
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
            duplicate_status,
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
    conn.commit()


def export_current_df(df: pd.DataFrame):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="products")
    return output.getvalue()


def show_import_tab():
    st.subheader("Импорт каталога")
    uploaded = st.file_uploader("Excel файл", type=["xlsx", "xls"])

    if uploaded is not None:
        temp_path = Path("data/_import_temp.xlsx")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_bytes(uploaded.read())

        if st.button("Импортировать", type="primary"):
            conn = get_db()
            result = import_catalog_from_excel(conn, temp_path)
            conn.close()
            st.success(
                f"Импорт завершён. Всего: {result.imported}, создано: {result.created}, обновлено: {result.updated}, дублей: {len(result.duplicates)}"
            )

            if result.duplicates:
                st.dataframe(pd.DataFrame(result.duplicates), use_container_width=True)


def show_catalog_tab():
    conn = get_db()

    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    with c1:
        search = st.text_input("Поиск", placeholder="Название / артикул / штрихкод")
    with c2:
        category = st.text_input("Категория")
    with c3:
        supplier = st.text_input("Поставщик")
    with c4:
        limit = st.number_input("Лимит", min_value=50, max_value=1000, value=200, step=50)

    df = load_products(conn, search=search, category=category, supplier=supplier, limit=int(limit))

    if df.empty:
        st.info("Нет товаров")
        conn.close()
        return

    st.download_button(
        "Скачать выборку Excel",
        data=export_current_df(df),
        file_name="pim_products.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    ids = df["id"].tolist()
    selected_id = st.selectbox("Открыть карточку товара", ids, format_func=lambda x: f"ID {x}")

    if st.button("Обновить дубли по текущей выборке"):
        total = 0
        progress = st.progress(0)
        for i, pid in enumerate(ids, start=1):
            refresh_duplicates_for_product(conn, int(pid))
            total += 1
            progress.progress(i / len(ids))
        st.success(f"Проверка дублей завершена: {total} товаров")

    st.dataframe(df, use_container_width=True, hide_index=True)

    if selected_id:
        st.session_state["selected_product_id"] = int(selected_id)

    conn.close()


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


def show_channels_tab():
    conn = get_db()

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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Импорт", "Каталог", "Карточка", "Атрибуты", "Каналы"]
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
        show_channels_tab()


if __name__ == "__main__":
    main()
