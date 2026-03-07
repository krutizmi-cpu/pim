import re


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = value.lower().replace("ё", "е")
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text
