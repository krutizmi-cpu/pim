# PIM (MVP) — базовый каталог товаров на Streamlit

Этот этап проекта — **первый модуль PIM**: базовый каталог товаров торговой компании.

Сейчас реализовано:
- хранение базового каталога в SQLite;
- импорт из Excel (1С) с созданием/обновлением товаров;
- анализ дублей (по article и похожему name >85%);
- просмотр каталога и фильтры в Streamlit;
- заглушка кнопки «Обогатить данные» с записью в лог.

## Структура проекта

```text
project/
  app.py
  db.py
  import_1c_catalog.py
  ozon_test.py
  requirements.txt
  services/
    __init__.py
    catalog_service.py
    enrichment_stub.py
  data/
```

## Базовый каталог

Обязательные поля товара:
- `article` (уникальный)
- `name`

Необязательные поля:
- `barcode`
- `weight`
- `length`, `width`, `height`
- `supplier_url`
- `image_url`
- `description`

Таблица: `products` в `data/catalog.db`.

Также используется таблица `duplicate_candidates` для предупреждений о похожих товарах.

## Импорт Excel (1С)

Поддерживаемые колонки:
- `article`
- `name`
- `barcode`
- `weight`
- `length`
- `width`
- `height`
- `supplier_url`

Если часть колонок отсутствует — импорт продолжится, поля останутся пустыми.

Поведение импорта:
- если `article` уже есть в БД → товар обновляется;
- если `article` новый → создаётся новый товар;
- если `name` похож на существующий (>85%) → создаётся запись в `duplicate_candidates`.

## Запуск Streamlit

```bash
pip install -r requirements.txt
streamlit run app.py
```

В интерфейсе раздел «Каталог товаров» позволяет:
- загрузить Excel;
- посмотреть таблицу товаров;
- искать по `article` и `name`;
- фильтровать по наличию габаритов, веса и фото;
- запустить заглушку «Обогатить данные».

Лог заглушки сохраняется в `data/enrichment.log`.

## CLI скрипты

Импорт каталога 1С:

```bash
python import_1c_catalog.py catalog.xlsx
```

Тест Ozon API (отдельный технический скрипт):

```bash
python ozon_test.py --client-id <OZON_CLIENT_ID> --api-key <OZON_API_KEY>
```
