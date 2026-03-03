"""PIM — Product Information Management
Самостоятельное Streamlit-приложение для управления каталогом товаров.

Функции:
- Загрузка каталога из Excel
- Обогащение габаритов и веса через AI
- Экспорт каталога в Excel
- Фильтрация и поиск товаров
"""
import streamlit as st
import pandas as pd
import sqlite3
import pim_enrich
from io import BytesIO

st.set_page_config(
    page_title="PIM — Product Information Management",
    layout="wide",
    page_icon="📦"
)

DB_PATH = "pim_storage.db"


def normalize_value(raw, unit):
    """Нормализует значение с учётом единицы измерения."""
    try:
        v = float(str(raw).replace(",", ".").strip())
    except (ValueError, TypeError):
        return 0.0
    u = str(unit).strip().lower() if unit else ""
    if u in ("мм", "mm"):
        return v / 10.0
    if u in ("г", "g", "гр", "gr"):
        return v / 1000.0
    return v


@st.cache_resource
def init_db():
    """Инициализирует базу данных (singleton-соединение)."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    pim_enrich.init_pim_tables(conn)
    return conn


conn = init_db()

# Обработка OpenAI ключа
if "openai_key" not in st.session_state:
    secret_key = None
    try:
        secret_key = st.secrets.get("OPENAI_API_KEY", None)
    except Exception:
        pass
    st.session_state["openai_key"] = secret_key or ""

st.title("📦 PIM — Каталог товаров")
api_key = st.session_state["openai_key"]

st.divider()

# ── Блок 1: Загрузка каталога из Excel ──────────────────────────────
with st.expander("📥 Загрузить каталог из Excel", expanded=False):
    uploaded = st.file_uploader("Выберите файл Excel с товарами", type=["xlsx", "xls"])
    col1, col2 = st.columns([2, 1])
    with col1:
        dim_unit = st.selectbox("Единица габаритов в файле", ["см", "мм"], key="pim_dim_unit")
    with col2:
        weight_unit = st.selectbox("Единица веса в файле", ["кг", "г"], key="pim_weight_unit")

    if uploaded and st.button("Загрузить в БД", key="load_excel"):
        try:
            df = pd.read_excel(uploaded)
        except Exception as e:
            st.error(f"Ошибка чтения файла: {e}")
            df = None

        if df is not None:
            required = ["SKU", "Название"]
            if not all(c in df.columns for c in required):
                st.error(f"Файл должен содержать минимум колонки: {required}")
            else:
                c = conn.cursor()
                loaded = 0
                for _, row in df.iterrows():
                    sku = str(row.get("SKU", "")).strip()
                    name = str(row.get("Название", "")).strip()
                    if not sku or not name or sku == "nan" or name == "nan":
                        continue
                    length = normalize_value(row.get("Длина", 0), dim_unit)
                    width = normalize_value(row.get("Ширина", 0), dim_unit)
                    height = normalize_value(row.get("Высота", 0), dim_unit)
                    weight = normalize_value(row.get("Вес", 0), weight_unit)
                    try:
                        cost = float(row.get("Себестоимость", 0) or 0)
                    except (ValueError, TypeError):
                        cost = 0.0
                    ean = str(row.get("EAN", "") or "").strip()
                    brand = str(row.get("Бренд", "") or "").strip()
                    category = str(row.get("Категория", "") or "").strip()
                    desc = str(row.get("Описание", "") or "").strip()
                    img = str(row.get("Фото", "") or "").strip()
                    c.execute("""
                        INSERT INTO products
                            (sku, name, length_cm, width_cm, height_cm,
                             weight_kg, cost, ean, brand, category,
                             description, main_image_url)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(sku) DO UPDATE SET
                            name=excluded.name,
                            length_cm=excluded.length_cm,
                            width_cm=excluded.width_cm,
                            height_cm=excluded.height_cm,
                            weight_kg=excluded.weight_kg,
                            cost=excluded.cost,
                            ean=excluded.ean,
                            brand=excluded.brand,
                            category=excluded.category,
                            description=excluded.description,
                            main_image_url=excluded.main_image_url
                    """, (sku, name, length, width, height, weight,
                          cost, ean, brand, category, desc, img))
                    loaded += 1
                conn.commit()
                st.success(f"✅ Загружено {loaded} товаров")
                st.rerun()

st.divider()

# ── Блок 2: Просмотр каталога ─────────────────────────────────────────
c = conn.cursor()
products = c.execute("""
    SELECT id, sku, name, brand, category,
           length_cm, width_cm, height_cm, weight_kg,
           cost, ean, enrich_status, enrich_source
    FROM products
    ORDER BY id DESC
""").fetchall()

st.subheader(f"Товары в каталоге ({len(products)})")

if not products:
    st.info("Каталог пуст — загрузите Excel файл")
else:
    COLUMNS = [
        "ID", "SKU", "Название", "Бренд", "Категория",
        "Длина (см)", "Ширина (см)", "Высота (см)", "Вес (кг)",
        "Себестоимость", "EAN", "Статус обогащения", "Источник"
    ]
    df_view = pd.DataFrame(products, columns=COLUMNS)

    # Фильтры
    col1, col2, col3 = st.columns(3)
    with col1:
        filt_cat = st.multiselect(
            "Фильтр по категории",
            df_view["Категория"].dropna().unique(),
            key="filt_cat"
        )
    with col2:
        filt_brand = st.multiselect(
            "Фильтр по бренду",
            df_view["Бренд"].dropna().unique(),
            key="filt_brand"
        )
    with col3:
        show_empty = st.checkbox("Только без габаритов/веса", key="show_empty")

    df_filtered = df_view.copy()
    if filt_cat:
        df_filtered = df_filtered[df_filtered["Категория"].isin(filt_cat)]
    if filt_brand:
        df_filtered = df_filtered[df_filtered["Бренд"].isin(filt_brand)]
    if show_empty:
        dim_cols = ["Длина (см)", "Ширина (см)", "Высота (см)", "Вес (кг)"]
        mask = df_filtered[dim_cols].apply(
            lambda col: col.isna() | (col == 0) | (col.astype(str).str.strip() == "")
        ).any(axis=1)
        df_filtered = df_filtered[mask]

    st.dataframe(df_filtered, use_container_width=True, height=400)

    if len(df_filtered) > 0:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_filtered.to_excel(writer, index=False, sheet_name="Каталог")
        output.seek(0)
        st.download_button(
            label="📥 Скачать каталог в Excel",
            data=output,
            file_name=f"pim_catalog_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="export_catalog",
        )

    st.divider()

    # ── Блок 3: Обогащение габаритов и веса ───────────────────────────────
    st.subheader("🔍 Обогащение габаритов и веса")
    col1, col2 = st.columns(2)
    with col1:
        enrich_mode = st.radio(
            "Режим обогащения",
            ["Только пустые (без размеров)", "Все товары (перезаписать)"],
            key="enrich_mode"
        )
    with col2:
        if not api_key:
            st.warning(
                "⚠️ OpenAI ключ не настроен — будут использоваться "
                "только средние по категории"
            )

    if st.button("🚀 Обогатить выбранные товары", key="enrich_btn", type="primary"):
        force = (enrich_mode == "Все товары (перезаписать)")
        products_to_enrich = []
        for _, row in df_filtered.iterrows():
            products_to_enrich.append({
                "id":         row["ID"],
                "sku":        row["SKU"],
                "name":       row["Название"],
                "brand":      row["Бренд"],
                "category":   row["Категория"],
                "ean":        row["EAN"],
                "length_cm":  row["Длина (см)"],
                "width_cm":   row["Ширина (см)"],
                "height_cm":  row["Высота (см)"],
                "weight_kg":  row["Вес (кг)"],
            })

        if not products_to_enrich:
            st.warning("Нет товаров для обогащения (проверьте фильтры)")
        else:
            progress = st.progress(0)
            status = st.empty()
            results = []
            cur = conn.cursor()

            for i, prod in enumerate(products_to_enrich):
                status.text(
                    f"Обработка {i + 1}/{len(products_to_enrich)}: {prod['name']}"
                )
                updated_prod, method = pim_enrich.enrich_product(
                    prod, conn, api_key,
                    search_results=None,
                    force=force
                )
                cur.execute("""
                    UPDATE products
                    SET length_cm=?,
                        width_cm=?,
                        height_cm=?,
                        weight_kg=?,
                        enrich_source=?,
                        enrich_status=?
                    WHERE id=?
                """, (
                    updated_prod.get("length_cm"),
                    updated_prod.get("width_cm"),
                    updated_prod.get("height_cm"),
                    updated_prod.get("weight_kg"),
                    updated_prod.get("enrich_source", method),
                    updated_prod.get(
                        "enrich_status",
                        "enriched" if method != "failed" else "failed"
                    ),
                    int(prod["id"])
                ))
                conn.commit()

                success = method not in ("failed", "already_filled")
                pim_enrich.log_enrichment(conn, prod["id"], method, success)
                results.append({"SKU": prod["sku"], "Метод": method, "Успех": success})
                progress.progress((i + 1) / len(products_to_enrich))

            status.text("✅ Обогащение завершено")
            st.success(f"Обработано {len(results)} товаров")
            df_results = pd.DataFrame(results)
            st.dataframe(df_results, use_container_width=True)
            st.download_button(
                "📥 Скачать отчёт",
                df_results.to_csv(index=False).encode("utf-8"),
                "enrichment_report.csv",
                "text/csv"
            )
            st.rerun()

st.divider()
st.caption(
    "💡 Совет: сначала загрузите каталог через Excel, "
    "затем обогатите через AI или средние по категории"
)
