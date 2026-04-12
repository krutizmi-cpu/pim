from __future__ import annotations


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
