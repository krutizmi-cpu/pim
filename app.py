from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

from db import get_connection, init_db
from services.catalog_service import import_catalog_from_excel
from services.enrichment_stub import LOG_PATH, enrich_products_stub


st.set_page_config(page_title="PIM — Каталог товаров", page_icon="📦", layout="wide")


@st.cache_resource
def get_db_connection():
    conn = get_connection()
    init_db(conn)
    return conn


def format_dimensions(row: pd.Series) -> str:
    values = [row.get("length"), row.get("width"), row.get("height")]
    if any(v is None or pd.isna(v) for v in values):
        return "—"
    return f"{values[0]} x {values[1]} x {values[2]}"


def load_products_df(conn) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT article, name, barcode, weight, length, width, height, supplier_url, image_url
        FROM products
        ORDER BY id DESC
        """
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["article", "name", "barcode", "weight", "length", "width", "height", "supplier_url", "image_url"])
    return pd.DataFrame([dict(row) for row in rows])


def show_catalog() -> None:
    st.title("📦 Каталог товаров")
    st.caption("Базовый модуль: загрузка каталога из 1С, просмотр и первичный контроль качества данных.")

    conn = get_db_connection()

    with st.expander("📥 Загрузить каталог из Excel", expanded=True):
        uploaded_file = st.file_uploader("Excel файл каталога", type=["xlsx", "xls"])
        if st.button("Загрузить каталог из Excel", type="primary", disabled=uploaded_file is None):
            if uploaded_file is None:
                st.warning("Выберите файл для загрузки.")
            else:
                uploads_dir = Path("data")
                uploads_dir.mkdir(parents=True, exist_ok=True)
                excel_path = uploads_dir / uploaded_file.name
                excel_path.write_bytes(uploaded_file.getbuffer())

                result = import_catalog_from_excel(conn, excel_path)
                st.success(
                    f"Импорт завершён: импортировано {result.imported}, новых {result.created}, обновлено {result.updated}."
                )

                if result.duplicates:
                    st.warning("Найдены возможные дубли по похожему name (>85%).")
                    st.dataframe(pd.DataFrame(result.duplicates), use_container_width=True)
                st.rerun()

    products_df = load_products_df(conn)

    col_search_1, col_search_2 = st.columns(2)
    with col_search_1:
        article_q = st.text_input("Поиск по article")
    with col_search_2:
        name_q = st.text_input("Поиск по name")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        has_dimensions = st.selectbox("Есть ли габариты", ["Все", "Да", "Нет"])
    with col_f2:
        has_weight = st.selectbox("Есть ли вес", ["Все", "Да", "Нет"])
    with col_f3:
        has_photo = st.selectbox("Есть ли фото", ["Все", "Да", "Нет"])

    filtered = products_df.copy()

    if article_q:
        filtered = filtered[filtered["article"].fillna("").str.contains(article_q, case=False, na=False)]
    if name_q:
        filtered = filtered[filtered["name"].fillna("").str.contains(name_q, case=False, na=False)]

    has_all_dims_mask = filtered[["length", "width", "height"]].notna().all(axis=1)

    if has_dimensions == "Да":
        filtered = filtered[has_all_dims_mask]
    elif has_dimensions == "Нет":
        filtered = filtered[~has_all_dims_mask]

    if has_weight == "Да":
        filtered = filtered[filtered["weight"].notna()]
    elif has_weight == "Нет":
        filtered = filtered[filtered["weight"].isna()]

    if has_photo == "Да":
        filtered = filtered[filtered["image_url"].fillna("").str.strip() != ""]
    elif has_photo == "Нет":
        filtered = filtered[filtered["image_url"].fillna("").str.strip() == ""]

    if filtered.empty:
        st.info("Товары не найдены. Загрузите каталог или измените фильтры.")
    else:
        view_df = filtered.copy()
        view_df["dimensions"] = view_df.apply(format_dimensions, axis=1)
        st.dataframe(
            view_df[["article", "name", "barcode", "weight", "dimensions", "supplier_url"]],
            use_container_width=True,
            hide_index=True,
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            view_df.to_excel(writer, index=False)
        st.download_button(
            "Скачать текущую выборку (Excel)",
            data=output.getvalue(),
            file_name="catalog_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    if st.button("Обогатить данные"):
        count = enrich_products_stub(conn)
        st.info(f"Заглушка выполнена. Товаров, требующих обогащения: {count}.")
        st.caption(f"Лог записан в: {LOG_PATH}")


if __name__ == "__main__":
    show_catalog()
