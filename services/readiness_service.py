from __future__ import annotations

import re
from typing import Any

import pandas as pd


ROW_LABEL_CANDIDATES = [
    "article",
    "name",
    "vendor_code",
    "title",
    "barcode",
    "supplier_article",
]


def _mapping_is_matched(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status:
        return status == "matched"
    return (
        bool(str(row.get("template_column") or "").strip())
        and bool(str(row.get("source_type") or "").strip())
        and bool(str(row.get("source_name") or "").strip())
    )


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _row_label(row: pd.Series, fallback_index: int) -> str:
    for column in ROW_LABEL_CANDIDATES:
        if column in row.index and not _is_missing(row[column]):
            return str(row[column])
    return f"row_{fallback_index + 1}"


_DUPLICATE_SLOT_RE = re.compile(r"\.\d+$")


def _base_template_column(column: object) -> str:
    return _DUPLICATE_SLOT_RE.sub("", str(column or "").strip())


def _template_column_is_required(column: object) -> bool:
    return "*" in _base_template_column(column)


def analyze_template_readiness(filled_df: pd.DataFrame, mapping_rows: list[dict[str, Any]]) -> dict[str, Any]:
    matched_rows = [row for row in mapping_rows if _mapping_is_matched(row) and row.get("template_column") in filled_df.columns]
    unmatched_rows = [row for row in mapping_rows if not _mapping_is_matched(row)]

    if filled_df.empty:
        return {
            "summary": {
                "rows": 0,
                "matched_columns": len(matched_rows),
                "unmatched_columns": len(unmatched_rows),
                "avg_readiness": 0,
                "ready_rows": 0,
                "partial_rows": 0,
                "blocked_rows": 0,
            },
            "column_coverage": [],
            "row_readiness": [],
        }

    column_coverage: list[dict[str, Any]] = []
    for row in matched_rows:
        column = row["template_column"]
        missing_mask = filled_df[column].apply(_is_missing)
        filled_count = int((~missing_mask).sum())
        total = int(len(filled_df))
        column_coverage.append(
            {
                "Колонка": column,
                "Статус": "matched",
                "Источник": f"{row.get('source_type') or ''}:{row.get('source_name') or ''}".strip(':'),
                "Обязательный": int(_template_column_is_required(column)),
                "Заполнено": filled_count,
                "Пусто": total - filled_count,
                "Покрытие, %": round((filled_count / total) * 100) if total else 0,
            }
        )

    for row in unmatched_rows:
        column_coverage.append(
            {
                "Колонка": row.get("template_column"),
                "Статус": "unmatched",
                "Источник": "",
                "Обязательный": int(_template_column_is_required(row.get("template_column"))),
                "Заполнено": 0,
                "Пусто": int(len(filled_df)),
                "Покрытие, %": 0,
            }
        )

    readiness_values: list[int] = []
    row_readiness: list[dict[str, Any]] = []
    ready_rows = 0
    partial_rows = 0
    blocked_rows = 0

    matched_columns = [row["template_column"] for row in matched_rows]
    matched_required_groups: dict[str, list[str]] = {}
    matched_optional_groups: dict[str, list[str]] = {}
    for row in matched_rows:
        column = str(row.get("template_column") or "").strip()
        if not column:
            continue
        source_signature = f"{row.get('source_type') or ''}:{row.get('source_name') or ''}".strip(":")
        group_key = f"{_base_template_column(column)}|{source_signature}"
        target = matched_required_groups if _template_column_is_required(column) else matched_optional_groups
        target.setdefault(group_key, []).append(column)

    unmatched_required_columns = [
        row.get("template_column")
        for row in unmatched_rows
        if row.get("template_column") and _template_column_is_required(row.get("template_column"))
    ]

    for idx, (_, data_row) in enumerate(filled_df.iterrows()):
        if matched_required_groups:
            missing_columns = []
            required_total = len(matched_required_groups)
            filled_total = 0
            for group_columns in matched_required_groups.values():
                if any(not _is_missing(data_row[column]) for column in group_columns):
                    filled_total += 1
                else:
                    missing_columns.append(_base_template_column(group_columns[0]))
            readiness_pct = round((filled_total / required_total) * 100) if required_total else 0
        else:
            missing_columns = [column for column in matched_columns if _is_missing(data_row[column])]
            matched_total = len(matched_columns)
            filled_total = matched_total - len(missing_columns)
            readiness_pct = round((filled_total / matched_total) * 100) if matched_total else 0
        readiness_values.append(readiness_pct)

        if readiness_pct == 100 and not unmatched_required_columns:
            status = "ready"
            ready_rows += 1
        elif readiness_pct >= 80:
            status = "partial"
            partial_rows += 1
        else:
            status = "blocked"
            blocked_rows += 1

        if missing_columns or unmatched_columns:
            row_readiness.append(
                {
                    "Товар": _row_label(data_row, idx),
                    "Готовность, %": readiness_pct,
                    "Статус": status,
                    "Пустые matched поля": ", ".join(missing_columns),
                    "Несматченные колонки": ", ".join(unmatched_required_columns),
                }
            )

    return {
        "summary": {
            "rows": int(len(filled_df)),
            "matched_columns": len(matched_rows),
            "unmatched_columns": len(unmatched_rows),
            "avg_readiness": round(sum(readiness_values) / len(readiness_values)) if readiness_values else 0,
            "ready_rows": ready_rows,
            "partial_rows": partial_rows,
            "blocked_rows": blocked_rows,
        },
        "column_coverage": sorted(column_coverage, key=lambda item: (item["Покрытие, %"], item["Колонка"])),
        "row_readiness": sorted(row_readiness, key=lambda item: (item["Готовность, %"], item["Товар"])),
    }
