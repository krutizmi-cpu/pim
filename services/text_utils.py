from __future__ import annotations

import re


def normalize_name(name: str | None) -> str:
    if not name:
        return ""
    text = name.lower().replace("ё", "е")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\b(арт|article|sku)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
