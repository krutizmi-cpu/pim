from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

from db import get_connection, init_db
from services.catalog_service import import_catalog_from_excel
from services.duplicate_service import refresh_duplicates_for_product
from services.enrichment_stub import LOG_PATH, enrich_products_stub
from services.logistics_service import estimate_logistics
from services.text_utils import normalize_name

st.set_page_config(page_title="PIM — Каталог товаров", page_icon="📦", layout="wide")


def get_db_connection():
    conn = get_connection()
    init_db(conn)
    return conn


def format_dimensions(row: pd.Series, prefix: str = "") -> str:
    cols = [f"{prefix}length", f"{prefix}width", f"{prefix}height"]
    values = [row.get(c) for c in cols]
    if any(v is None or pd.isna(v) for v in values):
        return "—"
    return f"{values[0]} x {values[1]} x {values[2]}"


def load_products_df(conn) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT id, article, name, barcode, category, supplier_url,
               weight, length, width, height,
               package_length, package_width, package_height, gross_weight,
               is_estimated_logistics, image_url,
               enrichment_status, enrichment_comment,
               duplicate_status, updated_at
        FROM products
        ORDER BY id DESC
        """
    ).fetchall()
    return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()


def show_product_card(product_id: int) -> None:
    st.title("🗂️ Карточка товара")
    conn = get_db_connection()

    product = conn.execute(
        """
        SELECT * FROM products WHERE id = ?
        """,
        (product_id,),
    ).fetchone()

    if not product:
        st.error("Товар не найден.")
        if st.button("Вернуться в каталог"):
            st.session_state["selected_product_id"] = None
            st.rerun()
        return

    top1, top2, top3 = st.columns(3)
    top1.metric("Статус дубля", product["duplicate_status"] or "—")
    top2.metric("Статус обогащения", product["enrichment_status"] or "—")
    top3.metric("Обновлён", product["updated_at"] or "—")

    with st.form("product_form"):
        st.subheader(f"Товар ID: {product['id']}")

        c1, c2 = st.columns(2)
        with c1:
            article = st.text_input("Артикул", value=product["article"] or "")
            name = st.text_input("Наименование", value=product["name"] or "")
            barcode = st.text_input("Штрихкод", value=product["barcode"] or "")
            category = st.text_input("Категория", value=product["category"] or "")
            supplier_url = st.text_input("Ссылка поставщика", value=product["supplier_url"] or "")
        with c2:
            image_url = st.text_input("Ссылка на фото", value=product["image_url"] or "")
            enrichment_status = st.selectbox(
                "Статус обогащения",
                ["new", "needs_enrichment", "partial", "enriched", "failed"],
                index=["new", "needs_enrichment", "partial", "enriched", "failed"].index(product["enrichment_status"])
                if product["enrichment_status"] in ["new", "needs_enrichment", "partial", "enriched", "failed"]
                else 0,
            )
            duplicate_status = st.selectbox(
                "Статус дубля",
                ["", "suspected", "checked", "not_duplicate"],
                index=["", "suspected", "checked", "not_duplicate"].index(product["duplicate_status"] or "")
                if (product["duplicate_status"] or "") in ["", "suspected", "checked", "not_duplicate"]
                else 0,
            )
            is_estimated_logistics = st.checkbox(
                "Логистика оценочная",
                value=bool(product["is_estimated_logistics"] or 0),
            )

        st.markdown("### Товарные параметры")
        d1, d2, d3, d4 = st.columns(4)
        with d1:
            weight = st.number_input("Вес", value=float(product["weight"] or 0.0), min_value=0.0)
        with d2:
            length = st.number_input("Длина", value=float(product["length"] or 0.0), min_value=0.0)
        with d3:
            width = st.number_input("Ширина", value=float(product["width"] or 0.0), min_value=0.0)
        with d4:
            height = st.number_input("Высота", value=float(product["height"] or 0.0), min_value=0.0)

        st.markdown("### Логистические параметры упаковки")
        p1, p2, p3, p4 = st.columns(4)
        with p1:
            package_length = st.number_input("Длина упаковки", value=float(product["package_length"] or 0.0), min_value=0.0)
        with p2:
            package_width = st.number_input("Ширина упаковки", value=float(product["package_width"] or 0.0), min_value=0.0)
        with p3:
            package_height = st.number_input("Высота упаковки", value=float(product["package_height"] or 0.0), min_value=0.0)
        with p4:
            gross_weight = st.number_input("Вес брутто", value=float(product["gross_weight"] or 0.0), min_value=0.0)

        description = st.text_area("Описание", value=product["description"] or "", height=120)
        enrichment_comment = st.text_area(
            "Комментарий по обогащению",
            value=product["enrichment_comment"] or "",
            height=100,
        )

        save_clicked = st.form_submit_button("Сохранить", type="primary")

    cta1, cta2, cta3 = st.columns(3)
    with cta1:
        if st.button("Проверить дубли"):
            conn.execute(
                "UPDATE products SET normalized_name = ?, updated_at = ? WHERE id = ?",
                (normalize_name(product["name"]), datetime.utcnow().isoformat(timespec="seconds"), product_id),
            )
            conn.commit()
            found = refresh_duplicates_for_product(conn, product_id)
            if found:
                st.warning(f"Найдено подозрений на дубли: {len(found)}")
                st.dataframe(pd.DataFrame(found), use_container_width=True, hide_index=True)
            else:
                st.success("Подозрения на дубли не найдены")
    with cta2:
        if st.button("Оценить логистические параметры"):
            result = estimate_logistics(conn, product_id)
            if result:
                st.success(
                    f"Оценка выполнена по {result['matched_count']} похожим товарам: "
                    f"{result['package_length']} x {result['package_width']} x {result['package_height']}, вес {result['gross_weight']}"
                )
                st.rerun()
            else:
                st.warning("Не удалось оценить логистику: нет похожих товаров с заполненной упаковкой")
    with cta3:
        if st.button("Вернуться в каталог"):
            st.session_state["selected_product_id"] = None
            st.rerun()

    if save_clicked:
        normalized_name = normalize_name(name.strip())
        conn.execute(
            """
            UPDATE products
            SET article = ?,
                name = ?,
                barcode = ?,
                category = ?,
                supplier_url = ?,
                weight = ?,
                length = ?,
                width = ?,
                height = ?,
                package_length = ?,
                package_width = ?,
                package_height = ?,
                gross_weight = ?,
                is_estimated_logistics = ?,
                image_url = ?,
                description = ?,
                enrichment_status = ?,
                enrichment_comment = ?,
                duplicate_status = ?,
                normalized_name = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                article.strip(),
                name.strip(),
                barcode.strip() or None,
                category.strip() or None,
                supplier_url.strip() or None,
                weight if weight > 0 else None,
                length if length > 0 else None,
                width if width > 0 else None,
                height if height > 0 else None,
                package_length if package_length > 0 else None,
                package_width if package_width > 0 else None,
                package_height if package_height > 0 else None,
                gross_weight if gross_weight > 0 else None,
                1 if is_estimated_logistics else 0,
                image_url.strip() or None,
                description.strip() or None,
                enrichment_status or None,
                enrichment_comment.strip() or None,
                duplicate_status or None,
                normalized_name,
                datetime.utcnow().isoformat(timespec="seconds"),
                product_id,
            ),
        )
        conn.commit()
        st.success("Товар сохранён")
        st.rerun()


def show_duplicates(conn) -> None:
    st.title("🔎 Подозрения на дубли")
    rows = conn.execute(
        """
        SELECT
            d.id,
            p1.article AS article_1,
            p1.name AS name_1,
            p2.article AS article_2,
            p2.name AS name_2,
            ROUND(d.similarity_score * 100, 2) AS similarity_score,
            d.reason,
            d.created_at
        FROM duplicate_candidates d
        JOIN products p1 ON d.product_id_1 = p1.id
        JOIN products p2 ON d.product_id_2 = p2.id
        ORDER BY d.similarity_score DESC, d.created_at DESC
        LIMIT 500
        """
    ).fetchall()

    if not rows:
        st.info("Подозрений на дубли пока нет.")
        return

    df = pd.DataFrame([dict(r) for r in rows])
    st.dataframe(df, use_container_width=True, hide_index=True)


def show_catalog() -> None:
    st.title("📦 Каталог товаров")
    st.caption("Базовый каталог, карточка, логистика и контроль дублей.")

    conn = get_db_connection()

    menu = st.sidebar.radio("Раздел", ["Каталог", "Подозрения на дубли"])
    if menu == "Подозрения на дубли":
        show_duplicates(conn)
        conn.close()
        return

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
                    st.warning(f"Найдены возможные дубли: {len(result.duplicates)}")
                st.rerun()

    products_df = load_products_df(conn)
    if products_df.empty:
        st.info("Товары не найдены. Загрузите каталог.")
        conn.close()
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        article_q = st.text_input("Поиск по article")
    with col2:
        name_q = st.text_input("Поиск по name")
    with col3:
        category_q = st.text_input("Фильтр по category")

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        has_supplier = st.selectbox("Есть supplier_url", ["Все", "Да", "Нет"])
    with f2:
        has_package = st.selectbox("Есть упаковка", ["Все", "Да", "Нет"])
    with f3:
        has_duplicate = st.selectbox("Есть подозрение на дубль", ["Все", "Да", "Нет"])
    with f4:
        has_photo = st.selectbox("Есть фото", ["Все", "Да", "Нет"])

    filtered = products_df.copy()
    if article_q:
        filtered = filtered[filtered["article"].fillna("").str.contains(article_q, case=False, na=False)]
    if name_q:
        filtered = filtered[filtered["name"].fillna("").str.contains(name_q, case=False, na=False)]
    if category_q:
        filtered = filtered[filtered["category"].fillna("").str.contains(category_q, case=False, na=False)]

    package_ok = filtered[["package_length", "package_width", "package_height", "gross_weight"]].notna().all(axis=1)
    if has_supplier == "Да":
        filtered = filtered[filtered["supplier_url"].fillna("").str.strip() != ""]
    elif has_supplier == "Нет":
        filtered = filtered[filtered["supplier_url"].fillna("").str.strip() == ""]

    if has_package == "Да":
        filtered = filtered[package_ok]
    elif has_package == "Нет":
        filtered = filtered[~package_ok]

    if has_duplicate == "Да":
        filtered = filtered[filtered["duplicate_status"].fillna("").str.strip() != ""]
    elif has_duplicate == "Нет":
        filtered = filtered[filtered["duplicate_status"].fillna("").str.strip() == ""]

    if has_photo == "Да":
        filtered = filtered[filtered["image_url"].fillna("").str.strip() != ""]
    elif has_photo == "Нет":
        filtered = filtered[filtered["image_url"].fillna("").str.strip() == ""]

    view_df = filtered.copy()
    view_df["dimensions"] = view_df.apply(format_dimensions, axis=1)
    view_df["package_dimensions"] = view_df.apply(lambda r: format_dimensions(r, prefix="package_"), axis=1)

    st.dataframe(
        view_df[[
            "id",
            "article",
            "name",
            "category",
            "barcode",
            "weight",
            "dimensions",
            "package_dimensions",
            "gross_weight",
            "supplier_url",
            "duplicate_status",
            "enrichment_status",
            "updated_at",
        ]],
        hide_index=True,
        use_container_width=True,
    )

    select_options = {
        f"{row['article']} — {row['name']}": int(row["id"]) for _, row in view_df.iterrows()
    }
    if select_options:
        selected_label = st.selectbox("Открыть карточку товара", list(select_options.keys()))
        if st.button("Открыть карточку выбранного товара"):
            st.session_state["selected_product_id"] = select_options[selected_label]
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
        st.info(f"Обработка выполнена. Товаров, требующих обогащения: {count}.")
        st.caption(f"Лог записан в: {LOG_PATH}")

    conn.close()


if __name__ == "__main__":
    if "selected_product_id" not in st.session_state:
        st.session_state["selected_product_id"] = None

    if st.session_state["selected_product_id"] is None:
        show_catalog()
    else:
        show_product_card(int(st.session_state["selected_product_id"]))
