# Минимальный тестовый проект: Ozon + 1C Excel

Проект нужен для первичной проверки структуры данных из двух источников перед разработкой PIM:

1. Ozon Seller API (`ozon_test.py`)
2. Excel-каталог из 1С (`import_1c_catalog.py`)

## Структура

```text
project/
  ozon_test.py
  import_1c_catalog.py
  db.py
  requirements.txt
  data/
```

## Установка

```bash
pip install -r requirements.txt
```

## Запуск

```bash
python ozon_test.py --client-id <OZON_CLIENT_ID> --api-key <OZON_API_KEY>
python import_1c_catalog.py catalog.xlsx
```

Можно передавать ключи Ozon через переменные окружения:

```bash
export OZON_CLIENT_ID=xxx
export OZON_API_KEY=yyy
python ozon_test.py
```

## Что делает `ozon_test.py`

- Делает POST-запрос к `https://api-seller.ozon.ru/v2/product/list`
- Сохраняет сырой ответ в `data/ozon_products_raw.json`
- Печатает в консоль количество товаров, поля товара и пример одного товара

## Что делает `import_1c_catalog.py`

- Читает Excel через pandas
- Поддерживает поля: `article`, `name`, `brand`, `barcode`, `category_path`, `weight`, `length`, `width`, `height`, `description`
- Создаёт недостающие категории по `category_path`
- Сохраняет данные в SQLite `data/catalog.db` (таблицы `products`, `categories`)
- Копирует исходный Excel в папку `data/`
- Выводит количество товаров, количество категорий и 5 примеров товаров
