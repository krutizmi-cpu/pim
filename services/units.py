from __future__ import annotations

import re


def mm_to_cm(value: float) -> float:
    return value / 10.0


def m_to_cm(value: float) -> float:
    return value * 100.0


def g_to_kg(value: float) -> float:
    return value / 1000.0


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    match = re.match(r"^(-?\d+(?:\.\d+)?)", text)
    if not match:
        return None
    return float(match.group(1))


def convert_to_base(value: object, unit: str | None, target: str) -> float | None:
    """Convert supported units to cm or kg."""
    number = _safe_float(value)
    if number is None:
        return None

    unit_norm = (unit or "").strip().lower()
    if target == "cm":
        if unit_norm in {"", "см", "cm"}:
            return number
        if unit_norm in {"мм", "mm"}:
            return mm_to_cm(number)
        if unit_norm in {"м", "m"}:
            return m_to_cm(number)
    if target == "kg":
        if unit_norm in {"", "кг", "kg"}:
            return number
        if unit_norm in {"г", "гр", "g", "gr"}:
            return g_to_kg(number)
    return number


def extract_unit_from_column(column_name: str) -> str | None:
    name = column_name.lower()
    for token in ["мм", "см", "м", "г", "кг", "mm", "cm", "kg", "gr", "g", "m"]:
        if token in name:
            return token
    return None
