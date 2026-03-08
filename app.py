from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

from db import get_connection, init_db
from services.catalog_service import ImportTemplateError, import_catalog_from_excel
from services.enrichment_stub import LOG_PATH, enrich_products_stub

st.set_page_config(page_title="PIM — Каталог товаров", page_icon="📦", layout="wide")

TEMPLATE_COLUMNS = [
    "article",
    "name",
    "barcode",
    "weight",
    "length",
    "width",
    "height",
    "supplier_url",
    "image_url",
    "description",
]

TEMPLATE_EXAMPLE_ROWS = [
    ["VEL-0001", "Велосипед горный Sprint 27.5", "4601234567890", 14.8, 145, 22, 78, "https://supplier.example/item/vel-0001", "https://supplier.example/item/vel-0001.jpg", "Горный велосипед, алюминиевая рама, 21 скорость"],
    ["SAM-0002", "Самокат городской Urban X", "", "", "", "", "", "https://supplier.example/item/sam-0002", "", ""],
]

COLUMN_HELP = {
    "article": "Обязательно. Уникальный артикул товара в вашей базе.",
    "name": "Обязательно. Наименование товара.",
    "barcode": "Необязательно. Штрихкод/EAN.",
    "weight": "Необязательно. Вес товара в кг.",
    "length": "Необязательно. Длина в см.",
    "width": "Необязательно. Ширина в см.",
    "height": "Необязательно. Высота в см.",
    "supplier_url": "Необязательно. Ссылка на сайт поставщика или производителя.",
    "image_url": "Необязательно. Прямая ссылка на фото товара.",
    "description": "Необязательно. Базовое описание товара.",
}


def get_db_connection():
    conn = get_connection()
    init_db(conn)
    return conn


@st.cache_data
def build_template_bytes() -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Шаблон"

    header_fill = PatternFill(fill_type="solid", fgColor="1F4E78")
    help_fill = PatternFill(fill_type="solid", fgColor="D9EAF7")
    required_fill = PatternFill(fill_type="solid", fgColor="FFF2CC")

    for col_idx, title in enumerate(TEMPLATE_COLUMNS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=title)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = header_fill
        ws.cell(row=2, column=col_idx, value=COLUMN_HELP[title]).fill = help_fill
        if title in {"article", "name"}:
            ws.cell(row=2, column=col_idx).fill = required_fill

    for row_idx, row_values in enumerate(TEMPLATE_EXAMPLE_ROWS, start=3):
        for col_idx, value in enumerate(row_values, start=1):
            ws.cell(row=row_idx, column=col_idx, value=value)

    widths = {
        "A": 16, "B": 36, "C": 18, "D": 12, "E": 12, "F": 12, "G": 12,
        "H": 40, "I": 40, "J": 42,
    }
    for col_letter, width in widths.items():
        ws.column_dimensions[col_letter].width = width
    ws.freeze_panes = "A3"

    info = wb.create_sheet("Справка")
    info["A1"] = "Как загружать новые товары"
    info["A1"].font = Font(bold=True, size=14)
    info["A3"] = "1. Заполняйте минимум два поля: article и name."
    info["A4"] = "2. Габариты указывайте в сантиметрах, вес — в килограммах."
    info["A5"] = "3. supplier_url — ссылка на карточку поставщика/производителя для будущего обогащения."
    info["A6"] = "4. image_url — прямая ссылка на фото, если она уже есть."
    info["A7"] = "5. Можно грузить файл с русскими колонками: Артикул, Наименование, Штрихкод, Вес, Длина, Ширина, Высота, Ссылка поставщика, Фото, Описание."
    info.column_dimensions["A"].width = 120

    output = BytesIO()
    wb.save(output)
    return output.getvalue()


def format_dimensions(row: pd.Series) -> str:
    values = [row.get("length"), row.get("width"), row.get("height")]
    if any(v is None or pd.isna(v) for v in values):
        return "—"
    return f"{values[0]} x {values[1]} x {values[2]}"


def load_products_df(conn) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT article, name, barcode, weight, length, width, height, supplier_url, image_url, description
        FROM products
        ORDER BY id DESC
        """
    ).fetchall()
    if not rows:
        return pd.DataFrame(
            columns=["article", "name", "barcode", "weight", "length", "width", "height", "supplier_url", "image_url", "description"]
        )
    return pd.DataFrame([dict(row) for row in rows])


def show_catalog() -> None:
    st.title("📦 Каталог товаров")
    st.caption("База товаров хранится постоянно и используется как основа для дальнейшего обогащения, наполнения карточек и выгрузок.")

    conn = get_db_connection()

    with st.expander("📥 Загрузить каталог из Excel", expanded=True):
        st.markdown("**Что грузить:** Excel-файл базового каталога. Минимум нужны поля **article** и **name**.")
        st.download_button(
            "Скачать образец шаблона Excel",
            data=build_template_bytes(),
            file_name="catalog_import_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.dataframe(
            pd.DataFrame(TEMPLATE_EXAMPLE_ROWS, columns=TEMPLATE_COLUMNS),
            hide_index=True,
            use_container_width=True,
        )

        uploaded_file = st.file_uploader("Excel файл каталога", type=["xlsx", "xls"])
        if st.button("Загрузить каталог из Excel", type="primary", disabled=uploaded_file is None):
            if uploaded_file is None:
                st.warning("Выберите файл для загрузки.")
            else:
                uploads_dir = Path("data")
                uploads_dir.mkdir(parents=True, exist_ok=True)
                excel_path = uploads_dir / uploaded_file.name
                excel_path.write_bytes(uploaded_file.getbuffer())

                try:
                    result = import_catalog_from_excel(conn, excel_path)
                except ImportTemplateError as exc:
                    st.error(f"Ошибка шаблона: {exc}")
                else:
                    st.success(
                        f"Импорт завершён: импортировано {result.imported}, новых {result.created}, обновлено {result.updated}, пропущено {result.skipped}."
                    )
                    if result.duplicates:
                        st.warning("Найдены возможные дубли по похожему name (>85%).")
                        st.dataframe(pd.DataFrame(result.duplicates), use_container_width=True, hide_index=True)
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
            view_df[["article", "name", "barcode", "weight", "dimensions", "supplier_url", "image_url"]],
            hide_index=True,
            use_container_width=True,
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            view_df.to_excel(writer, index=False, sheet_name="catalog")
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

    conn.close()


if __name__ == "__main__":
    show_catalog()
