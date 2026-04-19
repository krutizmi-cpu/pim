from __future__ import annotations

import re


def _to_float(value):
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(',', '.'))
    except Exception:
        return None


def _normalize_media_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith('[') and text.endswith(']'):
        try:
            import json
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            pass
    parts = [x.strip() for x in text.replace(';', ',').split(',')]
    return [p for p in parts if p]


_CM_FIELDS = {
    "length",
    "width",
    "height",
    "package_length",
    "package_width",
    "package_height",
}
_KG_FIELDS = {"weight", "gross_weight"}
_INCH_FIELDS = {"wheel_diameter_inch"}

_UNIT_TRANSFORMS = {
    ("cm", "mm"): "cm_to_mm",
    ("cm", "m"): "cm_to_m",
    ("cm", "inch"): "cm_to_inch",
    ("mm", "cm"): "mm_to_cm",
    ("mm", "m"): "mm_to_m",
    ("m", "cm"): "m_to_cm",
    ("m", "mm"): "m_to_mm",
    ("kg", "g"): "kg_to_g",
    ("kg", "lb"): "kg_to_lb",
    ("g", "kg"): "g_to_kg",
    ("lb", "kg"): "lb_to_kg",
    ("inch", "cm"): "inch_to_cm",
}


def _normalize_label(label: str | None) -> str:
    return " ".join(str(label or "").strip().lower().replace("_", " ").replace("ё", "е").split())


def _detect_target_unit(label: str, source_unit: str) -> str | None:
    text = _normalize_label(label)
    if not text:
        return None

    if source_unit in {"cm", "mm", "m", "inch"}:
        if re.search(r"\b(мм|mm|миллиметр\w*)\b", text):
            return "mm"
        if re.search(r"\b(см|cm|сантиметр\w*)\b", text):
            return "cm"
        if re.search(r"\b(м|m|метр\w*)\b", text):
            return "m"
        if re.search(r"\b(дюйм\w*|inch|inches)\b", text):
            return "inch"
        return None

    if source_unit in {"kg", "g", "lb"}:
        if re.search(r"\b(кг|kg|килограмм\w*)\b", text):
            return "kg"
        if re.search(r"\b(г|гр|g|грамм\w*)\b", text):
            return "g"
        if re.search(r"\b(lb|lbs|фунт\w*)\b", text):
            return "lb"
        return None

    return None


def infer_transform_rule(
    template_column: str | None,
    source_type: str | None,
    source_name: str | None,
) -> str | None:
    if not template_column or source_type != "column" or not source_name:
        return None

    source = str(source_name or "").strip().lower()
    source_unit = None
    if source in _CM_FIELDS:
        source_unit = "cm"
    elif source in _KG_FIELDS:
        source_unit = "kg"
    elif source in _INCH_FIELDS:
        source_unit = "inch"

    if not source_unit:
        return None

    target_unit = _detect_target_unit(str(template_column), source_unit)
    if not target_unit or target_unit == source_unit:
        return None
    return _UNIT_TRANSFORMS.get((source_unit, target_unit))


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
    if rule == "m_to_mm" and num is not None:
        return round(num * 1000, 2)
    if rule == "cm_to_m" and num is not None:
        return round(num / 100, 4)
    if rule == "mm_to_m" and num is not None:
        return round(num / 1000, 4)
    if rule == "kg_to_g" and num is not None:
        return round(num * 1000, 2)
    if rule == "g_to_kg" and num is not None:
        return round(num / 1000, 3)
    if rule == "inch_to_cm" and num is not None:
        return round(num * 2.54, 2)
    if rule == "cm_to_inch" and num is not None:
        return round(num / 2.54, 3)
    if rule == "kg_to_lb" and num is not None:
        return round(num * 2.20462, 3)
    if rule == "lb_to_kg" and num is not None:
        return round(num / 2.20462, 3)
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

    media = _normalize_media_list(value)
    if rule == "first_image":
        return media[0] if media else None
    if rule == "join_images":
        return ", ".join(media) if media else None
    if rule == "join_images_semicolon":
        return ";".join(media) if media else None
    if rule.startswith("image_"):
        try:
            idx = int(rule.split("_", 1)[1]) - 1
            return media[idx] if 0 <= idx < len(media) else None
        except Exception:
            return None

    return value
