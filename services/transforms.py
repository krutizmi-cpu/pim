from __future__ import annotations


def _to_float(value):
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(',', '.'))
    except Exception:
        return None


def apply_transform(value, transform_rule: str | None):
    if transform_rule in (None, ""):
        return value

    rule = str(transform_rule).strip().lower()
    if not rule:
        return value

    num = _to_float(value)

    if rule == "cm_to_mm" and num is not None:
        return round(num * 10, 2)
    if rule == "mm_to_cm" and num is not None:
        return round(num / 10, 2)
    if rule == "m_to_cm" and num is not None:
        return round(num * 100, 2)
    if rule == "kg_to_g" and num is not None:
        return round(num * 1000, 2)
    if rule == "g_to_kg" and num is not None:
        return round(num / 1000, 3)
    if rule == "inch_to_cm" and num is not None:
        return round(num * 2.54, 2)
    if rule == "lower":
        return str(value).lower() if value is not None else value
    if rule == "upper":
        return str(value).upper() if value is not None else value
    if rule == "strip":
        return str(value).strip() if value is not None else value

    if rule.startswith("prefix:"):
        return rule.split(":", 1)[1] + ("" if value is None else str(value))
    if rule.startswith("suffix:"):
        return ("" if value is None else str(value)) + rule.split(":", 1)[1]

    return value
