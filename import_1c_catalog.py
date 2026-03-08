from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from db import get_connection, init_db
from services.catalog_service import import_catalog_from_excel

DATA_DIR = Path("data")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Импорт базового каталога 1С из Excel в SQLite")
    parser.add_argument("excel_path", help="Путь до Excel файла каталога")
    return parser.parse_args()


def save_excel_copy(src_path: Path) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dst_path = DATA_DIR / src_path.name
    if src_path.resolve() != dst_path.resolve():
        shutil.copy2(src_path, dst_path)
    return dst_path


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel_path)
    if not excel_path.exists():
        raise SystemExit(f"Файл не найден: {excel_path}")

    copied_excel = save_excel_copy(excel_path)
    print(f"Исходный Excel скопирован в: {copied_excel}")

    conn = get_connection()
    init_db(conn)
    result = import_catalog_from_excel(conn, excel_path)

    print(f"Товаров импортировано: {result.imported}")
    print(f"Новых: {result.created}")
    print(f"Обновлено: {result.updated}")
    print(f"Кандидатов в дубли (по name): {len(result.duplicates)}")

    rows = conn.execute(
        "SELECT article, name, barcode, weight, length, width, height, supplier_url FROM products ORDER BY id DESC LIMIT 5"
    ).fetchall()
    print("\n5 примеров товаров:")
    for row in rows:
        print(dict(row))

    conn.close()


if __name__ == "__main__":
    main()
