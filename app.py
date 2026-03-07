from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import func, or_

from db import get_session, init_db, seed_demo_data
from models import (
    AttributeSynonym,
    Category,
    Client,
    DuplicateCandidate,
    Product,
    ProductAttributeDefinition,
    ProductAttributeValue,
)
from services.ai_stub import (
    generate_description,
    generate_infographic_prompt,
    generate_photo_prompt,
)
from services.barcode import needs_registration
from services.duplicates import rebuild_duplicate_candidates
from services.exports import build_export_dataframe, export_dataframe_to_excel
from services.imports import auto_map_columns, import_products_from_dataframe, read_excel_preview
from services.name_generator import generate_product_name

st.set_page_config(page_title="PIM MVP", page_icon="📦", layout="wide")

init_db()

if "seeded" not in st.session_state:
    seed_demo_data()
    st.session_state["seeded"] = True

MENU = [
    "Главная / Dashboard",
    "Клиенты",
    "Категории",
    "Товары",
    "Импорт Excel",
    "Дубли",
    "Подбор по размерам и весу",
    "Выгрузки",
    "Настройки атрибутов",
]

page = st.sidebar.radio("Раздел", MENU)


def _to_float(value: str) -> float | None:
    try:
        return float(value.replace(",", "."))
    except Exception:
        return None


def dashboard_page() -> None:
    st.title("📊 Dashboard")
    with get_session() as session:
        clients_count = session.query(func.count(Client.id)).scalar() or 0
        products_count = session.query(func.count(Product.id)).scalar() or 0
        categories_count = session.query(func.count(Category.id)).scalar() or 0
        products_no_barcode = (
            session.query(func.count(Product.id))
            .filter(or_(Product.barcode.is_(None), Product.barcode == ""))
            .scalar()
            or 0
        )
        duplicates_count = session.query(func.count(DuplicateCandidate.id)).scalar() or 0
        needs_reg_count = (
            session.query(func.count(Product.id))
            .filter(Product.needs_barcode_registration.is_(True))
            .scalar()
            or 0
        )

        col1, col2, col3 = st.columns(3)
        col1.metric("Клиенты", clients_count)
        col2.metric("Товары", products_count)
        col3.metric("Категории", categories_count)

        col4, col5, col6 = st.columns(3)
        col4.metric("Товары без штрихкода", products_no_barcode)
        col5.metric("Возможные дубли", duplicates_count)
        col6.metric("Требуют регистрации ШК", needs_reg_count)

        st.subheader("Последние товары")
        latest = session.query(Product).order_by(Product.created_at.desc()).limit(20).all()
        st.dataframe(
            [
                {
                    "ID": p.id,
                    "Название": p.base_name,
                    "Артикул": p.article,
                    "Штрихкод": p.barcode,
                    "Дата": p.created_at,
                }
                for p in latest
            ],
            use_container_width=True,
        )


def clients_page() -> None:
    st.title("👥 Клиенты")
    with get_session() as session:
        clients = session.query(Client).order_by(Client.name.asc()).all()
        st.dataframe(
            [{"ID": c.id, "Название": c.name, "Комментарий": c.comment} for c in clients],
            use_container_width=True,
        )

        st.subheader("Добавить клиента")
        with st.form("add_client"):
            name = st.text_input("Название клиента")
            comment = st.text_area("Комментарий")
            submitted = st.form_submit_button("Добавить")
            if submitted and name.strip():
                session.add(Client(name=name.strip(), comment=comment.strip() or None))
                session.commit()
                st.success("Клиент добавлен")
                st.rerun()

        st.subheader("Обновить комментарий")
        if clients:
            selected = st.selectbox("Клиент", clients, format_func=lambda c: c.name)
            new_comment = st.text_area("Новый комментарий", value=selected.comment or "")
            if st.button("Сохранить комментарий"):
                selected.comment = new_comment
                session.commit()
                st.success("Комментарий обновлён")
                st.rerun()


def categories_page() -> None:
    st.title("🗂️ Категории")
    with get_session() as session:
        categories = session.query(Category).order_by(Category.id.desc()).all()
        clients = session.query(Client).order_by(Client.name.asc()).all()

        st.dataframe(
            [
                {
                    "ID": c.id,
                    "Название": c.name,
                    "Parent ID": c.parent_id,
                    "Источник": c.source_type,
                    "Client ID": c.client_id,
                    "External ID": c.external_id,
                }
                for c in categories
            ],
            use_container_width=True,
        )

        st.subheader("Создать категорию")
        with st.form("create_category"):
            name = st.text_input("Название")
            source_type = st.selectbox("source_type", ["ozon", "1c", "custom"])
            parent = st.selectbox("Родитель", [None] + categories, format_func=lambda c: "Нет" if c is None else c.name)
            client = st.selectbox("Клиент", [None] + clients, format_func=lambda c: "Глобальная" if c is None else c.name)
            external_id = st.text_input("external_id")
            if st.form_submit_button("Создать") and name.strip():
                session.add(
                    Category(
                        name=name.strip(),
                        source_type=source_type,
                        parent_id=parent.id if parent else None,
                        client_id=client.id if client else None,
                        external_id=external_id.strip() or None,
                    )
                )
                session.commit()
                st.success("Категория создана")
                st.rerun()


def product_card(session, product: Product) -> None:
    st.markdown(f"### Карточка товара #{product.id}")
    categories = session.query(Category).order_by(Category.name.asc()).all()

    with st.form(f"edit_product_{product.id}"):
        product.base_name = st.text_input("Название", value=product.base_name)
        product.article = st.text_input("Артикул", value=product.article or "") or None
        product.color = st.text_input("Цвет", value=product.color or "") or None
        product.barcode = st.text_input("Штрихкод", value=product.barcode or "") or None

        category_options = [None] + categories
        selected_idx = 0
        if product.category_id:
            for idx, category_obj in enumerate(category_options):
                if category_obj and category_obj.id == product.category_id:
                    selected_idx = idx
                    break
        category = (
            st.selectbox(
                "Категория",
                category_options,
                index=selected_idx,
                format_func=lambda c: "Не выбрана" if c is None else c.name,
            )
            if categories
            else None
        )

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            product.length_cm = st.number_input("Длина, см", value=float(product.length_cm or 0.0), min_value=0.0)
            product.package_length_cm = st.number_input("Длина упаковки, см", value=float(product.package_length_cm or 0.0), min_value=0.0)
        with c2:
            product.width_cm = st.number_input("Ширина, см", value=float(product.width_cm or 0.0), min_value=0.0)
            product.package_width_cm = st.number_input("Ширина упаковки, см", value=float(product.package_width_cm or 0.0), min_value=0.0)
        with c3:
            product.height_cm = st.number_input("Высота, см", value=float(product.height_cm or 0.0), min_value=0.0)
            product.package_height_cm = st.number_input("Высота упаковки, см", value=float(product.package_height_cm or 0.0), min_value=0.0)
        with c4:
            product.weight_kg = st.number_input("Вес, кг", value=float(product.weight_kg or 0.0), min_value=0.0)
            product.gross_weight_kg = st.number_input("Вес брутто, кг", value=float(product.gross_weight_kg or 0.0), min_value=0.0)

        if st.form_submit_button("Сохранить изменения"):
            product.category_id = category.id if category else None
            product.needs_barcode_registration = needs_registration(product.barcode)
            session.commit()
            st.success("Товар обновлён")
            st.rerun()

    category_name = product.category.name if product.category else None
    if st.button("Сгенерировать название", key=f"gen_name_{product.id}"):
        product.generated_name = generate_product_name(
            product.base_name, product.article, product.color, category_name
        )
        session.commit()
        st.success(f"Сгенерировано: {product.generated_name}")

    st.info(f"Статус ШК: {'требует регистрации' if product.needs_barcode_registration else 'ОК'}")
    if st.button("Подготовить к регистрации ШК", key=f"prepare_barcode_{product.id}"):
        st.info("Заглушка: в следующей версии здесь будет сценарий регистрации в GS1.")

    st.subheader("AI-заглушки")
    col1, col2, col3 = st.columns(3)
    if col1.button("Сгенерировать описание", key=f"ai_desc_{product.id}"):
        st.text_area("Описание", value=generate_description(product), height=120)
    if col2.button("Сгенерировать prompt для фото", key=f"ai_photo_{product.id}"):
        st.text_area("Prompt фото", value=generate_photo_prompt(product), height=120)
    if col3.button("Сгенерировать prompt для инфографики", key=f"ai_info_{product.id}"):
        st.text_area("Prompt инфографики", value=generate_infographic_prompt(product), height=120)

    attrs = (
        session.query(ProductAttributeValue)
        .filter(ProductAttributeValue.product_id == product.id)
        .all()
    )
    if attrs:
        st.subheader("Атрибуты товара")
        st.dataframe(
            [
                {
                    "Атрибут": a.attribute_definition.display_name,
                    "Значение": a.value_string,
                    "Raw": a.raw_value,
                    "Raw unit": a.raw_unit,
                }
                for a in attrs
            ],
            use_container_width=True,
        )


def products_page() -> None:
    st.title("📦 Товары")
    with get_session() as session:
        clients = session.query(Client).order_by(Client.name.asc()).all()
        categories = session.query(Category).order_by(Category.name.asc()).all()

        c1, c2, c3, c4 = st.columns(4)
        client_filter = c1.selectbox("Клиент", [None] + clients, format_func=lambda c: "Все" if c is None else c.name)
        category_filter = c2.selectbox("Категория", [None] + categories, format_func=lambda c: "Все" if c is None else c.name)
        article_search = c3.text_input("Поиск по артикулу")
        name_search = c4.text_input("Поиск по названию")

        f1, f2 = st.columns(2)
        only_no_barcode = f1.checkbox("Без штрихкода")
        only_needs_registration = f2.checkbox("Требует регистрации ШК")

        query = session.query(Product)
        if client_filter:
            query = query.filter(Product.client_id == client_filter.id)
        if category_filter:
            query = query.filter(Product.category_id == category_filter.id)
        if article_search:
            query = query.filter(Product.article.ilike(f"%{article_search}%"))
        if name_search:
            query = query.filter(Product.base_name.ilike(f"%{name_search}%"))
        if only_no_barcode:
            query = query.filter(or_(Product.barcode.is_(None), Product.barcode == ""))
        if only_needs_registration:
            query = query.filter(Product.needs_barcode_registration.is_(True))

        products = query.order_by(Product.id.desc()).all()
        st.dataframe(
            [
                {
                    "ID": p.id,
                    "Название": p.base_name,
                    "Артикул": p.article,
                    "Клиент": p.client.name,
                    "Категория": p.category.name if p.category else "",
                    "Штрихкод": p.barcode,
                }
                for p in products
            ],
            use_container_width=True,
        )

        if products:
            selected_product = st.selectbox("Открыть карточку", products, format_func=lambda p: f"#{p.id} {p.base_name}")
            product_card(session, selected_product)


def import_excel_page() -> None:
    st.title("📥 Импорт Excel")
    uploaded = st.file_uploader("Загрузите Excel файл", type=["xlsx", "xls"])
    with get_session() as session:
        clients = session.query(Client).order_by(Client.name.asc()).all()
        client = st.selectbox("Клиент", clients, format_func=lambda c: c.name) if clients else None

        if uploaded and client:
            upload_dir = Path("uploads")
            upload_dir.mkdir(parents=True, exist_ok=True)
            file_path = upload_dir / uploaded.name
            file_path.write_bytes(uploaded.read())

            df = read_excel_preview(file_path)
            st.subheader("Предпросмотр")
            st.dataframe(df.head(20), use_container_width=True)

            auto_mapping = auto_map_columns(list(df.columns))
            st.subheader("Маппинг колонок")
            mapping: dict[str, str] = {}
            options = [
                "",
                "article",
                "base_name",
                "color",
                "barcode",
                "length",
                "width",
                "height",
                "weight",
                "package_length",
                "package_width",
                "package_height",
                "gross_weight",
                "category_name",
            ]
            for col in df.columns:
                default = auto_mapping.get(col, "")
                selected = st.selectbox(f"{col}", options, index=options.index(default) if default in options else 0)
                if selected:
                    mapping[col] = selected

            if st.button("Импортировать"):
                result = import_products_from_dataframe(session, client.id, df, mapping)
                st.success(f"Импортировано товаров: {result.imported_count}")
                if result.unrecognized_columns:
                    st.info("Нераспознанные характеристики (колонки):")
                    st.write(result.unrecognized_columns)


def duplicates_page() -> None:
    st.title("🧬 Дубли")
    with get_session() as session:
        if st.button("Пересчитать дубли"):
            count = rebuild_duplicate_candidates(session)
            st.success(f"Найдено и сохранено кандидатов: {count}")

        rows = session.query(DuplicateCandidate).order_by(DuplicateCandidate.similarity_score.desc()).all()
        st.dataframe(
            [
                {
                    "ID": d.id,
                    "Новый товар": d.new_product_id,
                    "Возможный дубль": d.existing_product_id,
                    "Score": d.similarity_score,
                    "Правило": d.matched_by,
                    "Details": json.dumps(json.loads(d.details_json or "{}"), ensure_ascii=False, indent=2),
                }
                for d in rows
            ],
            use_container_width=True,
        )


def fitting_page() -> None:
    st.title("📐 Подбор по размерам и весу")
    with get_session() as session:
        clients = session.query(Client).order_by(Client.name.asc()).all()
        client = st.selectbox("Клиент", [None] + clients, format_func=lambda c: "Все" if c is None else c.name)

        c1, c2, c3, c4 = st.columns(4)
        length = c1.number_input("Длина, см", min_value=0.0, value=0.0)
        width = c2.number_input("Ширина, см", min_value=0.0, value=0.0)
        height = c3.number_input("Высота, см", min_value=0.0, value=0.0)
        weight = c4.number_input("Вес, кг", min_value=0.0, value=0.0)

        products_query = session.query(Product)
        if client:
            products_query = products_query.filter(Product.client_id == client.id)

        target = {"length_cm": length, "width_cm": width, "height_cm": height, "weight_kg": weight}
        rows = []
        for p in products_query.all():
            deviations = []
            for field, value in target.items():
                if value <= 0:
                    continue
                p_value = getattr(p, field)
                if not p_value:
                    deviations.append(1.0)
                else:
                    deviations.append(abs(p_value - value) / max(value, 0.0001))

            if not deviations:
                continue
            avg_dev = sum(deviations) / len(deviations)
            score = max(0.0, 100 - avg_dev * 100)
            rows.append(
                {
                    "Товар": p.base_name,
                    "Артикул": p.article,
                    "Клиент": p.client.name,
                    "Размеры": f"{p.length_cm} x {p.width_cm} x {p.height_cm}",
                    "Вес": p.weight_kg,
                    "Score": round(score, 2),
                }
            )

        rows.sort(key=lambda x: x["Score"], reverse=True)
        st.dataframe(rows[:200], use_container_width=True)


def exports_page() -> None:
    st.title("📤 Выгрузки")
    with get_session() as session:
        clients = session.query(Client).order_by(Client.name.asc()).all()
        categories = session.query(Category).order_by(Category.name.asc()).all()

        c1, c2 = st.columns(2)
        client = c1.selectbox("Клиент", [None] + clients, format_func=lambda c: "Все" if c is None else c.name)
        category = c2.selectbox("Категория", [None] + categories, format_func=lambda c: "Все" if c is None else c.name)
        only_without_barcode = st.checkbox("Только без штрихкода")
        only_with_duplicates = st.checkbox("Только с дублями")

        df = build_export_dataframe(
            session,
            client.id if client else None,
            category.id if category else None,
            only_without_barcode,
            only_with_duplicates,
        )
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            st.download_button(
                "Скачать Excel",
                data=export_dataframe_to_excel(df),
                file_name="pim_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        st.info("XML/YML выгрузки будут добавлены позже.")


def attributes_page() -> None:
    st.title("⚙️ Настройки атрибутов")
    with get_session() as session:
        categories = session.query(Category).order_by(Category.name.asc()).all()
        clients = session.query(Client).order_by(Client.name.asc()).all()
        attributes = session.query(ProductAttributeDefinition).order_by(ProductAttributeDefinition.id.desc()).all()

        st.dataframe(
            [
                {
                    "ID": a.id,
                    "internal_name": a.internal_name,
                    "display_name": a.display_name,
                    "data_type": a.data_type,
                    "unit": a.base_unit,
                    "required": a.is_required,
                    "enum": a.is_enum,
                }
                for a in attributes
            ],
            use_container_width=True,
        )

        st.subheader("Создать атрибут")
        with st.form("create_attr"):
            internal_name = st.text_input("internal_name")
            display_name = st.text_input("display_name")
            data_type = st.selectbox("data_type", ["string", "number", "bool"])
            base_unit = st.text_input("unit")
            category = st.selectbox("category", [None] + categories, format_func=lambda c: "Все" if c is None else c.name)
            is_required = st.checkbox("required")
            is_enum = st.checkbox("enum")
            allowed_values_json = st.text_area("allowed_values_json", value="[]")
            if st.form_submit_button("Создать") and internal_name and display_name:
                session.add(
                    ProductAttributeDefinition(
                        internal_name=internal_name.strip(),
                        display_name=display_name.strip(),
                        data_type=data_type,
                        base_unit=base_unit.strip() or None,
                        category_id=category.id if category else None,
                        is_required=is_required,
                        is_enum=is_enum,
                        allowed_values_json=allowed_values_json.strip() or None,
                    )
                )
                session.commit()
                st.success("Атрибут создан")
                st.rerun()

        st.subheader("Добавить синоним")
        if attributes:
            with st.form("add_synonym"):
                attr = st.selectbox("Атрибут", attributes, format_func=lambda a: f"{a.display_name} ({a.internal_name})")
                client = st.selectbox("Клиент", [None] + clients, format_func=lambda c: "Глобальный" if c is None else c.name)
                synonym_name = st.text_input("Синоним")
                priority = st.number_input("priority", min_value=1, max_value=1000, value=100)
                if st.form_submit_button("Добавить") and synonym_name.strip():
                    session.add(
                        AttributeSynonym(
                            client_id=client.id if client else None,
                            attribute_definition_id=attr.id,
                            synonym_name=synonym_name.strip(),
                            priority=priority,
                        )
                    )
                    session.commit()
                    st.success("Синоним добавлен")
                    st.rerun()

        synonyms = session.query(AttributeSynonym).order_by(AttributeSynonym.priority.asc()).all()
        st.dataframe(
            [
                {
                    "ID": s.id,
                    "Синоним": s.synonym_name,
                    "Атрибут": s.attribute_definition.display_name,
                    "Client ID": s.client_id,
                    "priority": s.priority,
                }
                for s in synonyms
            ],
            use_container_width=True,
        )


if page == "Главная / Dashboard":
    dashboard_page()
elif page == "Клиенты":
    clients_page()
elif page == "Категории":
    categories_page()
elif page == "Товары":
    products_page()
elif page == "Импорт Excel":
    import_excel_page()
elif page == "Дубли":
    duplicates_page()
elif page == "Подбор по размерам и весу":
    fitting_page()
elif page == "Выгрузки":
    exports_page()
else:
    attributes_page()
