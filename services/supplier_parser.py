from __future__ import annotations

import re
from typing import Any

import httpx
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}


DIMENSION_PATTERNS = {
    "weight": [r"вес[^\d]{0,20}(\d+[\.,]?\d*)\s*(кг|г)", r"weight[^\d]{0,20}(\d+[\.,]?\d*)\s*(kg|g)"],
    "gross_weight": [r"вес\s*брутто[^\d]{0,20}(\d+[\.,]?\d*)\s*(кг|г)", r"gross\s*weight[^\d]{0,20}(\d+[\.,]?\d*)\s*(kg|g)"],
    "length": [r"длина[^\d]{0,20}(\d+[\.,]?\d*)\s*(мм|см|м)", r"length[^\d]{0,20}(\d+[\.,]?\d*)\s*(mm|cm|m)"],
    "width": [r"ширина[^\d]{0,20}(\d+[\.,]?\d*)\s*(мм|см|м)", r"width[^\d]{0,20}(\d+[\.,]?\d*)\s*(mm|cm|m)"],
    "height": [r"высота[^\d]{0,20}(\d+[\.,]?\d*)\s*(мм|см|м)", r"height[^\d]{0,20}(\d+[\.,]?\d*)\s*(mm|cm|m)"],
    "package_length": [r"длина\s*упаковки[^\d]{0,20}(\d+[\.,]?\d*)\s*(мм|см|м)", r"package\s*length[^\d]{0,20}(\d+[\.,]?\d*)\s*(mm|cm|m)"],
    "package_width": [r"ширина\s*упаковки[^\d]{0,20}(\d+[\.,]?\d*)\s*(мм|см|м)", r"package\s*width[^\d]{0,20}(\d+[\.,]?\d*)\s*(mm|cm|m)"],
    "package_height": [r"высота\s*упаковки[^\d]{0,20}(\d+[\.,]?\d*)\s*(мм|см|м)", r"package\s*height[^\d]{0,20}(\d+[\.,]?\d*)\s*(mm|cm|m)"],
}


def _to_float(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


def _convert_dimension(value: float | None, unit: str | None) -> float | None:
    if value is None or not unit:
        return value
    unit = unit.lower()
    if unit in ("мм", "mm"):
        return round(value / 10.0, 2)
    if unit in ("см", "cm"):
        return round(value, 2)
    if unit in ("м", "m"):
        return round(value * 100.0, 2)
    return value


def _convert_weight(value: float | None, unit: str | None) -> float | None:
    if value is None or not unit:
        return value
    unit = unit.lower()
    if unit in ("г", "g"):
        return round(value / 1000.0, 3)
    if unit in ("кг", "kg"):
        return round(value, 3)
    return value


def fetch_supplier_page(url: str, timeout: float = 20.0) -> str:
    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=timeout) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def extract_supplier_data(html: str, url: str | None = None) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    title = (soup.title.get_text(" ", strip=True) if soup.title else "")
    h1 = soup.select_one("h1")
    product_name = h1.get_text(" ", strip=True) if h1 else None

    description = None
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        description = meta_desc.get("content").strip()

    if not description:
        for selector in [".description", ".product-description", "#description", ".tab-description"]:
            node = soup.select_one(selector)
            if node:
                text = node.get_text(" ", strip=True)
                if text:
                    description = text
                    break

    image_urls: list[str] = []
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src")
        if not src:
            continue
        src = src.strip()
        if src.startswith("//"):
            src = "https:" + src
        if src.startswith("http") and src not in image_urls:
            image_urls.append(src)
        if len(image_urls) >= 10:
            break

    page_text = soup.get_text(" ", strip=True)
    lowered = page_text.lower()

    category_guess = None
    breadcrumb_selectors = [
        "nav.breadcrumbs a",
        ".breadcrumbs a",
        "[itemtype*='BreadcrumbList'] a",
    ]
    for selector in breadcrumb_selectors:
        crumbs = [x.get_text(" ", strip=True) for x in soup.select(selector) if x.get_text(" ", strip=True)]
        if len(crumbs) >= 2:
            category_guess = crumbs[-1]
            break

    result: dict[str, Any] = {
        "title": title or None,
        "name": product_name or title or None,
        "brand": None,
        "category": category_guess,
        "description": description or None,
        "image_urls": image_urls,
        "attributes": {},
        "weight": None,
        "gross_weight": None,
        "length": None,
        "width": None,
        "height": None,
        "package_length": None,
        "package_width": None,
        "package_height": None,
    }

    for field, patterns in DIMENSION_PATTERNS.items():
        for pattern in patterns:
            match = re.search(pattern, lowered, flags=re.IGNORECASE)
            if not match:
                continue
            value = _to_float(match.group(1))
            unit = match.group(2)
            if field in ("weight", "gross_weight"):
                result[field] = _convert_weight(value, unit)
            else:
                result[field] = _convert_dimension(value, unit)
            break

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for tr in rows:
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = cells[0].get_text(" ", strip=True)
            value = cells[1].get_text(" ", strip=True)
            if key and value:
                result["attributes"][key] = value
    # Also parse key-value blocks like "Характеристика: значение" from non-table chunks.
    for node in soup.select("li, p, div"):
        text = node.get_text(" ", strip=True)
        if not text or ":" not in text or len(text) > 220:
            continue
        left, right = text.split(":", 1)
        key = left.strip()
        value = right.strip()
        if not key or not value or len(key) > 80:
            continue
        if key not in result["attributes"]:
            result["attributes"][key] = value

    brand_keys = {"бренд", "торговая марка", "brand", "производитель", "manufacturer"}
    for key, value in result["attributes"].items():
        if str(key).strip().lower() in brand_keys and str(value).strip():
            result["brand"] = str(value).strip()
            break

    return result


def normalize_supplier_data(raw_data: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "name": raw_data.get("name") or raw_data.get("title"),
        "brand": raw_data.get("brand"),
        "category": raw_data.get("category"),
        "description": raw_data.get("description") or raw_data.get("title"),
        "image_url": None,
        "weight": raw_data.get("weight"),
        "gross_weight": raw_data.get("gross_weight"),
        "length": raw_data.get("length"),
        "width": raw_data.get("width"),
        "height": raw_data.get("height"),
        "package_length": raw_data.get("package_length"),
        "package_width": raw_data.get("package_width"),
        "package_height": raw_data.get("package_height"),
        "attributes": raw_data.get("attributes") or {},
        "image_urls": raw_data.get("image_urls") or [],
    }

    if normalized["image_urls"]:
        normalized["image_url"] = normalized["image_urls"][0]

    return normalized
