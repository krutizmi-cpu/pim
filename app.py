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
import os
from io import BytesIO

st.set_page_config(
    page_title="PIM — Product Information Management",
    layout="wide",
    page_icon="📦"
)

# --- 1. Путь к базе данных (персистентный) ---
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

# --- 2. Сайдбар для настроек ---
with st.sidebar:
    st.header("⚙️ Настройки")
    
    # Поле ввода API ключа
    # Пытаемся взять начальное значение из session_state или secrets
    if "openai_key" not in st.session_state:
        try:
            st.session_state["openai_key"] = st.secrets.get("OPENAI_API_KEY", "")
        except:
            st.session_state["openai_key"] = ""
            
    user_key = st.text_input(
        "OpenAI API Key", 
        value=st.session_state["openai_key"],
        type="password",
        help="Ключ необходим для AI-обогащения товаров. Если оставить пустым, будут использоваться средние по категории."
    )
    st.session_state["openai_key"] = user_key
    
    st.divider()
    st.info(f"📁 База данных: {os.path.abspath(DB_PATH)}")
    st.caption("Данные сохраняются автоматически в файл .db")

api_key = st.session_state.get("openai_key", "")

st.title("📦 PIM — Каталог товаров")
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
                        except:
                            cost = 0.0
                            
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
                        """, (sku, name, length, width, height, weight, cost, 
                              str(row.get("EAN", "")), str(row.get("Бренд", "")), 
                              str(row.get("Категория", "")), str(row.get("Описание", "")), 
                              str(row.get("Фото", ""))))
                        loaded += 1
                    conn.commit()
                    st.success(f"✅ Загружено {loaded} товаров. Данные сохранены в базу.")
                    st.rerun()
        except Exception as e:
            st.error(f"Ошибка: {e}")

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
    st.info("Каталог пуст — загрузите Excel файл. База данных готова (pim_storage.db).")
else:
    COLUMNS = [
        "ID", "SKU", "Название", "Бренд", "Категория", 
        "Длина (см)", "Ширина (см)", "Высота (см)", "Вес (кг)", 
        "Себестоимость", "EAN", "Статус", "Источник"
    ]
    df_view = pd.DataFrame(products, columns=COLUMNS)
    
    # Фильтры
    col1, col2, col3 = st.columns(3)
    with col1:
        filt_cat = st.multiselect("Категория", df_view["Категория"].dropna().unique())
    with col2:
        filt_brand = st.multiselect("Бренд", df_view["Бренд"].dropna().unique())
    with col3:
        show_empty = st.checkbox("Только без размеров")
    
    df_filtered = df_view.copy()
    if filt_cat:
        df_filtered = df_filtered[df_filtered["Категория"].isin(filt_cat)]
    if filt_brand:
        df_filtered = df_filtered[df_filtered["Бренд"].isin(filt_brand)]
    if show_empty:
        mask = df_filtered[["Длина (см)", "Ширина (см)", "Высота (см)", "Вес (кг)"]].apply(
            lambda x: (x <= 0) | x.isna()
        ).any(axis=1)
        df_filtered = df_filtered[mask]
        
    st.dataframe(df_filtered, use_container_width=True, height=400)
    
    if len(df_filtered) > 0:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_filtered.to_excel(writer, index=False)
        st.download_button(
            "📥 Скачать выбранное в Excel",
            data=output.getvalue(),
            file_name="pim_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.divider()

# ── Блок 3: Обогащение ──────────────────────────────────────────────
st.subheader("🔍 Обогащение данных через AI")

col1, col2 = st.columns(2)
with col1:
    enrich_mode = st.radio(
        "Режим", 
        ["Только пустые", "Все (перезаписать)"], 
        horizontal=True
    )
with col2:
    if not api_key:
        st.warning("⚠️ OpenAI ключ не настроен — будут использоваться только средние по категории")
    else:
        st.success("✅ OpenAI подключен")

if st.button("🚀 Запустить обогащение", type="primary"):
    force = (enrich_mode == "Все (перезаписать)")
    to_process = []
    for _, row in df_filtered.iterrows():
        to_process.append({
            "id": row["ID"], "sku": row["SKU"], "name": row["Название"],
            "brand": row["Бренд"], "category": row["Категория"],
            "length_cm": row["Длина (см)"], "width_cm": row["Ширина (см)"],
            "height_cm": row["Высота (см)"], "weight_kg": row["Вес (кг)"]
        })
    
    if not to_process:
        st.warning("Нет товаров для обработки.")
    else:
        progress = st.progress(0)
        status = st.empty()
        cur = conn.cursor()
        
        for i, prod in enumerate(to_process):
            status.text(f"Обработка {i+1}/{len(to_process)}: {prod['name']}")
            
            updated, method = pim_enrich.enrich_product(prod, conn, api_key, force=force)
            
            cur.execute("""
                UPDATE products 
                SET length_cm=?, width_cm=?, height_cm=?, weight_kg=?, 
                    enrich_source=?, enrich_status=?
                WHERE id=?
            """, (updated["length_cm"], updated["width_cm"], updated["height_cm"], 
                  updated["weight_kg"], method, "enriched" if method != "failed" else "failed", 
                  int(prod["id"])))
            conn.commit()
            progress.progress((i + 1) / len(to_process))
            
        st.success("✅ Готово! Данные в базе обновлены.")
        st.rerun()

st.divider()
st.caption("💡 База данных сохраняется в файл pim_storage.db. Вы можете перезапускать приложение, данные не пропадут.")
