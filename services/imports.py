from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy.orm import Session

from models import (
    AttributeSynonym,
    Category,
    Product,
    ProductAttributeDefinition,
    ProductAttributeValue,
)
from services.barcode import needs_registration
from services.units import convert_to_base, extract_unit_from_column
from utils.text_normalizer import normalize_text


BASE_FIELD_SYNONYMS = {
    "article": ["артикул", "sku", "код товара", "код", "vendor code"],
    "base_name": ["название", "наименование", "товар", "item name"],
    "color": ["цвет", "color"],
    "barcode": ["штрихкод", "barcode", "ean"],
    "length": ["длина", "length"],
    "width": ["ширина", "width"],
    "height": ["высота", "height"],
    "weight": ["вес", "weight", "масса"],
    "package_length": ["длина упаковки", "упаковка длина"],
    "package_width": ["ширина упаковки", "упаковка ширина"],
    "package_height": ["высота упаковки", "упаковка высота"],
    "gross_weight": ["вес брутто", "gross weight", "вес упаковки"],
    "category_name": ["категория", "category", "группа"],
}


@dataclass
class ImportResult:
    imported_count: int
    unrecognized_columns: list[str]


def read_excel_preview(file_path: Path) -> pd.DataFrame:
    return pd.read_excel(file_path)


def auto_map_columns(columns: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for col in columns:
        normalized = normalize_text(col)
        for target, aliases in BASE_FIELD_SYNONYMS.items():
            if any(alias in normalized for alias in aliases):
                mapping[col] = target
                break
    return mapping


def _find_or_create_category(session: Session, client_id: int, name: str | None) -> Category | None:
    if not name:
        return None
    clean_name = name.strip()
    if not clean_name:
        return None

    category = (
        session.query(Category)
        .filter(Category.name == clean_name)
        .filter((Category.client_id == client_id) | (Category.client_id.is_(None)))
        .first()
    )
    if category:
        return category

    category = Category(name=clean_name, source_type="custom", client_id=client_id)
    session.add(category)
    session.flush()
    return category


def _get_synonym_map(session: Session, client_id: int) -> dict[str, ProductAttributeDefinition]:
    synonyms = (
        session.query(AttributeSynonym)
        .filter((AttributeSynonym.client_id == client_id) | (AttributeSynonym.client_id.is_(None)))
        .order_by(AttributeSynonym.priority.asc())
        .all()
    )
    result: dict[str, ProductAttributeDefinition] = {}
    for syn in synonyms:
        result[normalize_text(syn.synonym_name)] = syn.attribute_definition
    return result


def import_products_from_dataframe(
    session: Session,
    client_id: int,
    df: pd.DataFrame,
    field_mapping: dict[str, str],
) -> ImportResult:
    synonym_map = _get_synonym_map(session, client_id)
    imported = 0

    mapped_columns = set(field_mapping.keys())
    unrecognized_columns = [col for col in df.columns if col not in mapped_columns]

    for _, row in df.iterrows():
        payload: dict[str, Any] = {target: row.get(column) for column, target in field_mapping.items()}

        base_name = str(payload.get("base_name") or "").strip()
        if not base_name:
            continue

        category = _find_or_create_category(session, client_id, str(payload.get("category_name") or "").strip())
        barcode = str(payload.get("barcode") or "").strip() or None

        product = Product(
            client_id=client_id,
            category_id=category.id if category else None,
            base_name=base_name,
            article=str(payload.get("article") or "").strip() or None,
            color=str(payload.get("color") or "").strip() or None,
            barcode=barcode,
            needs_barcode_registration=needs_registration(barcode),
            source_type="excel",
            length_cm=convert_to_base(payload.get("length"), extract_unit_from_column(_column_name(field_mapping, "length")), "cm"),
            width_cm=convert_to_base(payload.get("width"), extract_unit_from_column(_column_name(field_mapping, "width")), "cm"),
            height_cm=convert_to_base(payload.get("height"), extract_unit_from_column(_column_name(field_mapping, "height")), "cm"),
            weight_kg=convert_to_base(payload.get("weight"), extract_unit_from_column(_column_name(field_mapping, "weight")), "kg"),
            package_length_cm=convert_to_base(payload.get("package_length"), extract_unit_from_column(_column_name(field_mapping, "package_length")), "cm"),
            package_width_cm=convert_to_base(payload.get("package_width"), extract_unit_from_column(_column_name(field_mapping, "package_width")), "cm"),
            package_height_cm=convert_to_base(payload.get("package_height"), extract_unit_from_column(_column_name(field_mapping, "package_height")), "cm"),
            gross_weight_kg=convert_to_base(payload.get("gross_weight"), extract_unit_from_column(_column_name(field_mapping, "gross_weight")), "kg"),
        )
        session.add(product)
        session.flush()

        for column in unrecognized_columns:
            raw_value = row.get(column)
            if pd.isna(raw_value):
                continue

            synonym_key = normalize_text(column)
            definition = synonym_map.get(synonym_key)
            if not definition:
                continue

            number_value: float | None = None
            try:
                number_value = float(str(raw_value).replace(",", "."))
            except Exception:
                pass

            session.add(
                ProductAttributeValue(
                    product_id=product.id,
                    attribute_definition_id=definition.id,
                    value_string=str(raw_value),
                    value_number=number_value,
                    raw_value=str(raw_value),
                    raw_unit=extract_unit_from_column(column),
                )
            )

        imported += 1

    session.commit()
    return ImportResult(imported_count=imported, unrecognized_columns=unrecognized_columns)


def _column_name(mapping: dict[str, str], target: str) -> str:
    for col, mapped in mapping.items():
        if mapped == target:
            return col
    return ""
