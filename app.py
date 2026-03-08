from __future__ import annotations

from datetime import datetime
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
        SELECT id, article, name, barcode, weight, length, width, height,
               supplier_url, image_url, enrichment_status, updated_at
        FROM products
        ORDER BY id DESC
        """
    ).fetchall()
    if not rows:
        return pd.DataFrame(
            columns=[
                "id",
                "article",
                "name",
                "barcode",
                "weight",
                "length",
                "width",
                "height",
                "supplier_url",
                "image_url",
                "enrichment_status",
                "updated_at",
            ]
        )
    return pd.DataFrame([dict(row) for row in rows])


def show_product_card(product_id: int) -> None:
    st.title("🗂️ Карточка товара")
    conn = get_db_connection()

    product = conn.execute(
        """
        SELECT id, article, name, barcode, weight, length, width, height,
               supplier_url, image_url, description,
               enrichment_status, enrichment_comment,
               created_at, updated_at
        FROM products
        WHERE id = ?
        """,
        (product_id,),
    ).fetchone()

    if not product:
        st.error("Товар не найден.")
        if st.button("Вернуться в каталог"):
            st.session_state["selected_product_id"] = None
            st.rerun()
        return

    with st.form("product_form"):
        st.subheader(f"Товар ID: {product['id']}")

        article = st.text_input("article", value=product["article"] or "")
        name = st.text_input("name", value=product["name"] or "")
        barcode = st.text_input("barcode", value=product["barcode"] or "")

        c1, c2 = st.columns(2)
        with c1:
            weight = st.number_input("weight", value=float(product["weight"] or 0.0), min_value=0.0)
            length = st.number_input("length", value=float(product["length"] or 0.0), min_value=0.0)
        with c2:
            width = st.number_input("width", value=float(product["width"] or 0.0), min_value=0.0)
            height = st.number_input("height", value=float(product["height"] or 0.0), min_value=0.0)

        supplier_url = st.text_input("supplier_url", value=product["supplier_url"] or "")
        image_url = st.text_input("image_url", value=product["image_url"] or "")
        description = st.text_area("description", value=product["description"] or "", height=120)

        enrichment_status = st.text_input("enrichment_status", value=product["enrichment_status"] or "")
        enrichment_comment = st.text_area("enrichment_comment", value=product["enrichment_comment"] or "", height=100)

        save_clicked = st.form_submit_button("Сохранить", type="primary")

    if save_clicked:
        conn.execute(
            """
            UPDATE products
            SET article = ?,
                name = ?,
                barcode = ?,
                weight = ?,
                length = ?,
                width = ?,
                height = ?,
                supplier_url = ?,
                image_url = ?,
                description = ?,
                enrichment_status = ?,
                enrichment_comment = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                article.strip(),
                name.strip(),
                barcode.strip() or None,
                weight if weight > 0 else None,
                length if length > 0 else None,
                width if width > 0 else None,
                height if height > 0 else None,
                supplier_url.strip() or None,
                image_url.strip() or None,
                description.strip() or None,
                enrichment_status.strip() or None,
                enrichment_comment.strip() or None,
                datetime.utcnow().isoformat(timespec="seconds"),
                product_id,
            ),
        )
        conn.commit()
        st.success("Товар сохранён")
        st.session_state["selected_product_id"] = None
        st.rerun()

    if st.button("Вернуться в каталог"):
        st.session_state["selected_product_id"] = None
        st.rerun()


def show_catalog() -> None:
    st.title("📦 Каталог товаров")
    st.caption("Базовый модуль: загрузка каталога из 1С, просмотр и контроль качества данных.")

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
                    st.dataframe(pd.DataFrame(result.duplicates), width="stretch")
                st.rerun()

    products_df = load_products_df(conn)

    col1, col2 = st.columns(2)
    with col1:
        article_q = st.text_input("Поиск по article")
    with col2:
        name_q = st.text_input("Поиск по name")

    f1, f2, f3 = st.columns(3)
    with f1:
        has_dimensions = st.selectbox("Есть ли габариты", ["Все", "Да", "Нет"])
    with f2:
        has_weight = st.selectbox("Есть ли вес", ["Все", "Да", "Нет"])
    with f3:
        has_photo = st.selectbox("Есть ли фото", ["Все", "Да", "Нет"])

    filtered = products_df.copy()
    if article_q:
        filtered = filtered[filtered["article"].fillna("").str.contains(article_q, case=False, na=False)]
    if name_q:
        filtered = filtered[filtered["name"].fillna("").str.contains(name_q, case=False, na=False)]

    has_all_dims = filtered[["length", "width", "height"]].notna().all(axis=1)
    if has_dimensions == "Да":
        filtered = filtered[has_all_dims]
    elif has_dimensions == "Нет":
        filtered = filtered[~has_all_dims]

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
            view_df[[
                "article",
                "name",
                "barcode",
                "weight",
                "dimensions",
                "supplier_url",
                "enrichment_status",
                "updated_at",
            ]],
            hide_index=True,
            width="stretch",
        )

        st.subheader("Открыть карточку")
        for _, row in view_df.iterrows():
            c1, c2, c3 = st.columns([2, 5, 2])
            c1.write(row["article"])
            c2.write(row["name"])
            if c3.button("Открыть карточку", key=f"open_{int(row['id'])}"):
                st.session_state["selected_product_id"] = int(row["id"])
                st.rerun()

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
    if "selected_product_id" not in st.session_state:
        st.session_state["selected_product_id"] = None

    if st.session_state["selected_product_id"] is None:
        show_catalog()
    else:
        show_product_card(int(st.session_state["selected_product_id"]))
