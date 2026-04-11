# PIM — модуль базового каталога товаров

Этот этап проекта — первый модуль PIM: **базовый каталог товаров** для торговой компании.

## Проектный ориентир

- Основной метод и рамки разработки зафиксированы в файле [PIM_METHOD.md](PIM_METHOD.md).
- Перед любыми новыми доработками сверяйся с этим документом, чтобы не уходить в сторону.

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
