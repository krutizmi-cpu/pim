from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

from db import get_connection, init_db
from services.catalog_service import (
    TEMPLATE_INSTRUCTIONS,
    build_catalog_template_excel,
    import_catalog_from_excel,
    load_product_by_id,
    load_products_df,
    save_product,
)
from services.enrichment_stub import LOG_PATH, enrich_products_stub

st.set_page_config(page_title="PIM — Каталог товаров", page_icon="📦", layout="wide")


STATUS_LABELS = {
    None: "Новый",
    "new": "Новый",
    "needs_enrichment": "Требует обогащения",
    "partial": "Частично заполнен",
    "enriched": "Готов",
    "failed": "Ошибка",
}


def get_db_connection():
    conn = get_connection()
    init_db(conn)
    return conn


def format_dimensions(row: pd.Series) -> str:
    values = [row.get("length"), row.get("width"), row.get("height")]
    if any(v is None or pd.isna(v) for v in values):
        return "—"
    cleaned = []
    for value in values:
        if float(value).is_integer():
            cleaned.append(str(int(value)))
        else:
            cleaned.append(str(round(float(value), 2)))
    return " × ".join(cleaned) + " см"


def format_weight(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{round(float(value), 3)} кг"


def status_to_label(value: str | None) -> str:
    return STATUS_LABELS.get(value, value or "Новый")


def show_product_card(product_id: int) -> None:
    st.title("🗂️ Карточка товара")
    st.caption("Здесь можно вручную дозаполнить и скорректировать товар перед дальнейшим обогащением.")

    conn = get_db_connection()
    product = load_product_by_id(conn, product_id)

    if not product:
        st.error("Товар не найден.")
        if st.button("← Вернуться в каталог"):
            st.session_state["selected_product_id"] = None
            st.rerun()
        conn.close()
        return

    top1, top2, top3 = st.columns([2, 3, 2])
    top1.metric("Артикул", product["article"] or "—")
    top2.metric("Статус", status_to_label(product["enrichment_status"]))
    top3.metric("Обновлён", product["updated_at"] or "—")

    with st.form("product_form", clear_on_submit=False):
        st.subheader("Основные данные")
        c1, c2 = st.columns(2)
        with c1:
            article = st.text_input("Артикул *", value=product["article"] or "")
            barcode = st.text_input("Штрихкод / EAN", value=product["barcode"] or "")
            supplier_url = st.text_input("Ссылка поставщика", value=product["supplier_url"] or "")
        with c2:
            name = st.text_input("Наименование *", value=product["name"] or "")
            image_url = st.text_input("Ссылка на фото", value=product["image_url"] or "")
            enrichment_status = st.selectbox(
                "Статус товара",
                options=["new", "needs_enrichment", "partial", "enriched", "failed"],
                index=["new", "needs_enrichment", "partial", "enriched", "failed"].index(
                    product["enrichment_status"] or "new"
                ),
                format_func=status_to_label,
            )

        st.subheader("Габариты и вес")
        d1, d2, d3, d4 = st.columns(4)
        weight = d1.number_input("Вес, кг", value=float(product["weight"] or 0.0), min_value=0.0, step=0.1)
        length = d2.number_input("Длина, см", value=float(product["length"] or 0.0), min_value=0.0, step=0.1)
        width = d3.number_input("Ширина, см", value=float(product["width"] or 0.0), min_value=0.0, step=0.1)
        height = d4.number_input("Высота, см", value=float(product["height"] or 0.0), min_value=0.0, step=0.1)

        st.subheader("Контент")
        description = st.text_area("Описание", value=product["description"] or "", height=160)
        enrichment_comment = st.text_area(
            "Комментарий по обогащению",
            value=product["enrichment_comment"] or "",
            height=100,
        )

        b1, b2 = st.columns([1, 1])
        save_clicked = b1.form_submit_button("💾 Сохранить", type="primary", use_container_width=True)
        back_clicked = b2.form_submit_button("← Назад в каталог", use_container_width=True)

    if save_clicked:
        errors = []
        if not article.strip():
            errors.append("Артикул обязателен.")
        if not name.strip():
            errors.append("Наименование обязательно.")

        if errors:
            for error in errors:
                st.error(error)
        else:
            save_product(
                conn,
                product_id=product_id,
                article=article.strip(),
                name=name.strip(),
                barcode=barcode.strip() or None,
                weight=weight if weight > 0 else None,
                length=length if length > 0 else None,
                width=width if width > 0 else None,
                height=height if height > 0 else None,
                supplier_url=supplier_url.strip() or None,
                image_url=image_url.strip() or None,
                description=description.strip() or None,
                enrichment_status=enrichment_status,
                enrichment_comment=enrichment_comment.strip() or None,
            )
            st.success("Товар сохранён.")
            st.session_state["selected_product_id"] = None
            conn.close()
            st.rerun()

    if back_clicked or st.button("← Вернуться без сохранения"):
        st.session_state["selected_product_id"] = None
        conn.close()
        st.rerun()

    conn.close()


def show_catalog() -> None:
    st.title("📦 Каталог товаров")
    st.caption("База товаров хранится в SQLite и используется как основа для дальнейшего обогащения и наполнения карточек.")

    conn = get_db_connection()

    with st.expander("📥 Загрузка каталога из Excel", expanded=True):
        st.markdown("**Минимум для импорта:** `article` и `name`.")
        st.caption("Остальные поля можно заполнить позже в карточке товара или через модуль обогащения.")

        template_bytes = build_catalog_template_excel()
        st.download_button(
            "⬇️ Скачать шаблон Excel",
            data=template_bytes,
            file_name="catalog_import_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
        )

        with st.popover("Поля шаблона"):
            st.markdown(TEMPLATE_INSTRUCTIONS)

        uploaded_file = st.file_uploader("Excel файл каталога", type=["xlsx", "xls"], key="catalog_uploader")

        if st.button("Загрузить каталог", type="primary", disabled=uploaded_file is None):
            if uploaded_file is None:
                st.warning("Выберите файл для загрузки.")
            else:
                uploads_dir = Path("data")
                uploads_dir.mkdir(parents=True, exist_ok=True)
                excel_path = uploads_dir / uploaded_file.name
                excel_path.write_bytes(uploaded_file.getbuffer())

                result = import_catalog_from_excel(conn, excel_path)
                st.success(
                    f"Импорт завершён: обработано {result.imported}, новых {result.created}, обновлено {result.updated}, пропущено {result.skipped}."
                )
                if result.duplicates:
                    st.warning("Найдены возможные дубли по похожему названию (>85%).")
                    st.dataframe(pd.DataFrame(result.duplicates), use_container_width=True, hide_index=True)
                if result.skipped_rows:
                    st.info("Часть строк пропущена как служебные или пустые.")
                    st.dataframe(pd.DataFrame(result.skipped_rows), use_container_width=True, hide_index=True)
                st.rerun()

    products_df = load_products_df(conn)

    if products_df.empty:
        st.info("Каталог пока пуст. Скачай шаблон Excel, заполни минимум article и name и загрузи файл.")
        conn.close()
        return

    products_df["dimensions"] = products_df.apply(format_dimensions, axis=1)
    products_df["weight_display"] = products_df["weight"].apply(format_weight)
    products_df["status_label"] = products_df["enrichment_status"].apply(status_to_label)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Товаров в базе", len(products_df))
    k2.metric("С фото", int(products_df["image_url"].fillna("").str.strip().ne("").sum()))
    k3.metric("С весом", int(products_df["weight"].notna().sum()))
    dims_mask = products_df[["length", "width", "height"]].notna().all(axis=1)
    k4.metric("С габаритами", int(dims_mask.sum()))

    st.subheader("Фильтры")
    col1, col2 = st.columns(2)
    with col1:
        article_q = st.text_input("Поиск по артикулу")
    with col2:
        name_q = st.text_input("Поиск по наименованию")

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        has_dimensions = st.selectbox("Есть ли габариты", ["Все", "Да", "Нет"])
    with f2:
        has_weight = st.selectbox("Есть ли вес", ["Все", "Да", "Нет"])
    with f3:
        has_photo = st.selectbox("Есть ли фото", ["Все", "Да", "Нет"])
    with f4:
        status_filter = st.selectbox("Статус", ["Все", "Новый", "Требует обогащения", "Частично заполнен", "Готов", "Ошибка"])

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

    if status_filter != "Все":
        filtered = filtered[filtered["status_label"] == status_filter]

    st.subheader("Текущий каталог")
    catalog_view = filtered[[
        "article",
        "name",
        "barcode",
        "weight_display",
        "dimensions",
        "supplier_url",
        "image_url",
        "status_label",
        "updated_at",
    ]].rename(
        columns={
            "article": "Артикул",
            "name": "Наименование",
            "barcode": "Штрихкод",
            "weight_display": "Вес",
            "dimensions": "Габариты",
            "supplier_url": "Ссылка поставщика",
            "image_url": "Фото",
            "status_label": "Статус",
            "updated_at": "Обновлён",
        }
    )
    st.dataframe(catalog_view, hide_index=True, use_container_width=True)

    selector_df = filtered[["id", "article", "name", "status_label"]].copy()
    selector_df["label"] = selector_df.apply(
        lambda row: f"{row['article']} — {row['name']} [{row['status_label']}]",
        axis=1,
    )

    selected_label = st.selectbox("Выбери товар для открытия карточки", selector_df["label"].tolist())
    selected_id = int(selector_df.loc[selector_df["label"] == selected_label, "id"].iloc[0])

    action1, action2 = st.columns([1, 2])
    if action1.button("🗂️ Открыть карточку", type="primary", use_container_width=True):
        st.session_state["selected_product_id"] = selected_id
        conn.close()
        st.rerun()

    export_df = filtered.drop(columns=["weight_display", "status_label"], errors="ignore")
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="catalog")
    action2.download_button(
        "⬇️ Скачать текущую выборку (Excel)",
        data=output.getvalue(),
        file_name="catalog_export.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    if st.button("Обогатить данные", use_container_width=False):
        count = enrich_products_stub(conn)
        st.info(f"Промежуточная заглушка выполнена. Товаров, требующих обогащения: {count}.")
        st.caption(f"Лог записан в: {LOG_PATH}")

    conn.close()


if __name__ == "__main__":
    if "selected_product_id" not in st.session_state:
        st.session_state["selected_product_id"] = None

    if st.session_state["selected_product_id"] is None:
        show_catalog()
    else:
        show_product_card(int(st.session_state["selected_product_id"]))
