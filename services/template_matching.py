from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from services.attribute_service import list_attribute_definitions, list_channel_mapping_rules, set_product_attribute_value
from services.source_priority import can_overwrite_field
from services.source_tracking import save_field_source
from services.transforms import apply_transform


BASE_PRODUCT_FIELD_ALIASES = {
    "article": ["артикул", "sku", "vendor code", "код товара"],
    "name": ["название", "наименование", "товар", "name", "title"],
    "barcode": ["штрихкод", "ean", "barcode"],
    "brand": ["бренд", "brand", "марка"],
    "description": ["описание", "description"],
    "weight": ["вес", "weight"],
    "length": ["длина", "length"],
    "width": ["ширина", "width"],
    "height": ["высота", "height"],
    "package_length": ["длина упаковки", "упаковка длина", "глубина упаковки"],
    "package_width": ["ширина упаковки", "упаковка ширина"],
    "package_height": ["высота упаковки", "упаковка высота"],
    "gross_weight": ["вес брутто", "вес в упаковке", "gross weight"],
    "image_url": ["фото", "изображение", "image", "main image"],
}


def normalize_key(value: str) -> str:
    return " ".join(str(value).strip().lower().replace("_", " ").split())


def build_candidate_map(conn) -> dict[str, tuple[str, str, str]]:
    result: dict[str, tuple[str, str, str]] = {}

    for field_name, aliases in BASE_PRODUCT_FIELD_ALIASES.items():
        for alias in aliases:
            result[normalize_key(alias)] = ("column", field_name, alias)

    for attr in list_attribute_definitions(conn):
        result[normalize_key(attr["name"])] = ("attribute", attr["code"], attr["name"])
        result[normalize_key(attr["code"])] = ("attribute", attr["code"], attr["name"])

    return result


def auto_match_template_columns(conn, columns: list[str]) -> list[dict]:
    candidate_map = build_candidate_map(conn)
    matches: list[dict] = []

    for col in columns:
        norm = normalize_key(col)
        matched = candidate_map.get(norm)
        if matched:
            source_type, source_name, matched_by = matched
            matches.append(
                {
                    "template_column": col,
                    "status": "matched",
                    "source_type": source_type,
                    "source_name": source_name,
                    "matched_by": matched_by,
                }
            )
        else:
            matches.append(
                {
                    "template_column": col,
                    "status": "unmatched",
                    "source_type": None,
                    "source_name": None,
                    "matched_by": None,
                }
            )

    return matches


def apply_saved_mapping_rules(conn, matches: list[dict], channel_code: str, category_code: str | None = None) -> list[dict]:
    rules = list_channel_mapping_rules(conn, channel_code=channel_code, category_code=category_code)
    if not rules:
        return matches

    rule_map = {r["target_field"]: r for r in rules}
    updated = []
    for match in matches:
        rule = rule_map.get(match["template_column"])
        if rule:
            updated.append(
                {
                    "template_column": match["template_column"],
                    "status": "matched",
                    "source_type": rule["source_type"],
                    "source_name": rule["source_name"],
                    "matched_by": "saved_rule",
                    "transform_rule": rule.get("transform_rule"),
                }
            )
        else:
            if "transform_rule" not in match:
                match["transform_rule"] = None
            updated.append(match)
    return updated


def build_product_value_map(conn, product_id: int) -> dict[str, object]:
    product = conn.execute("SELECT * FROM products WHERE id = ?", (int(product_id),)).fetchone()
    if not product:
        return {}

    value_map = dict(product)

    rows = conn.execute(
        """
        SELECT pav.attribute_code, pav.value_text, pav.value_number, pav.value_boolean, pav.value_json
        FROM product_attribute_values pav
        WHERE pav.product_id = ?
        """,
        (int(product_id),),
    ).fetchall()

    for row in rows:
        value = row["value_number"]
        if value is None:
            value = row["value_boolean"]
        if value is None:
            value = row["value_json"]
        if value is None:
            value = row["value_text"]
        value_map[row["attribute_code"]] = value

    media_rows = conn.execute(
        """
        SELECT value_text, value_json
        FROM product_attribute_values
        WHERE product_id = ?
          AND attribute_code IN ('gallery_images', 'main_image')
        ORDER BY id
        """,
        (int(product_id),),
    ).fetchall()
    media_values = []
    for row in media_rows:
        if row["value_json"]:
            media_values.append(row["value_json"])
        elif row["value_text"]:
            media_values.append(row["value_text"])
    if product.get("image_url"):
        media_values.insert(0, product.get("image_url"))
    value_map["media_gallery"] = media_values

    return value_map


def fill_template_dataframe(conn, template_df: pd.DataFrame, product_ids: list[int], matches: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for product_id in product_ids:
        value_map = build_product_value_map(conn, product_id)
        row_data = {}
        for match in matches:
            col = match["template_column"]
            if match["status"] != "matched":
                row_data[col] = None
                continue
            row_data[col] = apply_transform(value_map.get(match["source_name"]), match.get("transform_rule"))
        rows.append(row_data)
    return pd.DataFrame(rows)


def apply_client_validated_values(conn, product_ids: list[int], matches: list[dict], channel_code: str | None = None) -> dict:
    applied = 0
    skipped = 0

    for product_id in product_ids:
        value_map = build_product_value_map(conn, product_id)
        for match in matches:
            if match.get("status") != "matched":
                continue
            field_name = match.get("source_name")
            source_type = match.get("source_type")
            if not field_name or not source_type:
                continue
            value = value_map.get(field_name)
            if value in (None, ""):
                continue

            if source_type == "column":
                if not can_overwrite_field(conn, product_id, field_name, "client_validated", force=False):
                    skipped += 1
                    continue
                conn.execute(
                    f"UPDATE products SET {field_name} = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (value, int(product_id)),
                )
                save_field_source(
                    conn=conn,
                    product_id=product_id,
                    field_name=field_name,
                    source_type="client_validated",
                    source_value_raw=value,
                    source_url=channel_code,
                    confidence=0.95,
                    is_manual=False,
                )
                applied += 1
            elif source_type == "attribute":
                attr_field_name = f"attr:{field_name}"
                if not can_overwrite_field(conn, product_id, attr_field_name, "client_validated", force=False):
                    skipped += 1
                    continue
                set_product_attribute_value(conn, int(product_id), field_name, value, channel_code=channel_code)
                save_field_source(
                    conn=conn,
                    product_id=product_id,
                    field_name=attr_field_name,
                    source_type="client_validated",
                    source_value_raw=value,
                    source_url=channel_code,
                    confidence=0.95,
                    is_manual=False,
                )
                applied += 1

    conn.commit()
    return {"applied": applied, "skipped": skipped}


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "template") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def detect_template_data_start_row(template_bytes: bytes, sheet_name: str | None = None) -> int:
    workbook = load_workbook(BytesIO(template_bytes), read_only=True, data_only=False)
    try:
        ws = workbook[sheet_name] if sheet_name and sheet_name in workbook.sheetnames else workbook.active
        max_scan_rows = min(int(ws.max_row or 1), 120)
        header_tokens = {
            "артикул", "article", "sku", "name", "название", "наименование",
            "barcode", "штрихкод", "бренд", "brand", "описание", "description",
        }

        candidate_header_row = None
        best_score = -1
        for row_idx in range(1, max_scan_rows + 1):
            values = []
            for cell in ws[row_idx]:
                val = cell.value
                if val is None:
                    continue
                text = str(val).strip().lower()
                if text:
                    values.append(text)
            if not values:
                continue

            score = 0
            for v in values:
                if any(token in v for token in header_tokens):
                    score += 2
                if len(v) < 50:
                    score += 1

            if score > best_score and len(values) >= 2:
                best_score = score
                candidate_header_row = row_idx

        if candidate_header_row is None:
            return 2

        return int(candidate_header_row + 1)
    finally:
        workbook.close()


def fill_template_workbook_bytes(
    conn,
    template_bytes: bytes,
    product_ids: list[int],
    matches: list[dict],
    sheet_name: str | None = None,
    data_start_row: int | None = None,
) -> bytes:
    workbook = load_workbook(BytesIO(template_bytes))
    ws = workbook[sheet_name] if sheet_name and sheet_name in workbook.sheetnames else workbook.active

    start_row = int(data_start_row or detect_template_data_start_row(template_bytes, sheet_name=ws.title))
    if start_row < 2:
        start_row = 2
    header_row = start_row - 1

    column_map: dict[str, int] = {}
    for col_idx in range(1, int(ws.max_column or 1) + 1):
        cell_value = ws.cell(row=header_row, column=col_idx).value
        if cell_value is None:
            continue
        key = str(cell_value).strip()
        if key and key not in column_map:
            column_map[key] = col_idx

    if not column_map:
        output = BytesIO()
        workbook.save(output)
        return output.getvalue()

    # Clear previous values in the data area for mapped columns.
    for row_idx in range(start_row, int(ws.max_row or start_row) + 1):
        for match in matches:
            template_column = match.get("template_column")
            if not template_column or template_column not in column_map:
                continue
            ws.cell(row=row_idx, column=column_map[template_column]).value = None

    for row_offset, product_id in enumerate(product_ids):
        target_row = start_row + row_offset
        value_map = build_product_value_map(conn, int(product_id))
        for match in matches:
            if match.get("status") != "matched":
                continue
            template_column = match.get("template_column")
            source_name = match.get("source_name")
            if not template_column or template_column not in column_map or not source_name:
                continue
            value = apply_transform(value_map.get(source_name), match.get("transform_rule"))
            ws.cell(row=target_row, column=column_map[template_column]).value = value

    output = BytesIO()
    workbook.save(output)
    return output.getvalue()
