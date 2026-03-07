from __future__ import annotations

from io import BytesIO

import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import Session

from models import Category, Client, DuplicateCandidate, Product


def build_export_dataframe(
    session: Session,
    client_id: int | None,
    category_id: int | None,
    only_without_barcode: bool,
    only_with_duplicates: bool,
) -> pd.DataFrame:
    query = session.query(Product, Client.name, Category.name).join(Client).outerjoin(Category)

    if client_id:
        query = query.filter(Product.client_id == client_id)
    if category_id:
        query = query.filter(Product.category_id == category_id)
    if only_without_barcode:
        query = query.filter((Product.barcode.is_(None)) | (Product.barcode == ""))

    rows = []
    duplicate_ids: set[int] = set()
    if only_with_duplicates:
        duplicates = session.query(DuplicateCandidate).all()
        for d in duplicates:
            duplicate_ids.add(d.new_product_id)
            duplicate_ids.add(d.existing_product_id)

    for product, client_name, category_name in query.order_by(Product.id.desc()).all():
        if only_with_duplicates and product.id not in duplicate_ids:
            continue

        rows.append(
            {
                "ID": product.id,
                "Клиент": client_name,
                "Категория": category_name,
                "Название": product.base_name,
                "Сгенерированное название": product.generated_name,
                "Артикул": product.article,
                "Цвет": product.color,
                "Штрихкод": product.barcode,
                "Требует регистрации ШК": "Да" if product.needs_barcode_registration else "Нет",
                "Длина, см": product.length_cm,
                "Ширина, см": product.width_cm,
                "Высота, см": product.height_cm,
                "Вес, кг": product.weight_kg,
                "Длина упаковки, см": product.package_length_cm,
                "Ширина упаковки, см": product.package_width_cm,
                "Высота упаковки, см": product.package_height_cm,
                "Вес брутто, кг": product.gross_weight_kg,
            }
        )

    return pd.DataFrame(rows)


def export_dataframe_to_excel(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="products")
    return output.getvalue()
