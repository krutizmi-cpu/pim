# PIM — модуль базового каталога товаров

Этот этап проекта — первый модуль PIM: **базовый каталог товаров** для торговой компании.

## Что реализовано

- хранение каталога в SQLite (`data/catalog.db`);
- обязательные поля: `article` (уникальный), `name`;
- необязательные поля: `barcode`, `weight`, `length`, `width`, `height`, `supplier_url`, `image_url`, `description`;
- импорт каталога из Excel (1С) с обновлением по `article`;
- таблица `duplicate_candidates` для похожих `name` (>85%);
- Streamlit-раздел «Каталог товаров» с поиском и фильтрами;
- кнопка «Обогатить данные» как заглушка с логом в `data/enrichment.log`.

## Структура

```text
project/
  app.py
  db.py
  import_1c_catalog.py
  pim_enrich.py
  requirements.txt
  services/
    __init__.py
    catalog_service.py
    enrichment_stub.py
  data/
```

## Запуск

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Импорт Excel

Поддерживаемые колонки:

- `article`
- `name`
- `barcode`
- `weight`
- `length`
- `width`
- `height`
- `supplier_url`

Если часть колонок отсутствует, импорт продолжается.

CLI-импорт:

```bash
python import_1c_catalog.py catalog.xlsx
```
