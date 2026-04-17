from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

PRODUCT_LINK_HINTS = (
    "product",
    "товар",
    "item",
    "/p/",
    "/products/",
    "/catalog/",
)

GENERIC_CATEGORY_WORDS = {
    "товары",
    "каталог",
    "все товары",
    "products",
    "catalog",
    "shop",
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


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _extract_json_ld_products(soup: BeautifulSoup) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    for node in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = node.string or node.get_text(strip=True)
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue

        queue: list[Any] = []
        if isinstance(parsed, list):
            queue.extend(parsed)
        else:
            queue.append(parsed)

        while queue:
            current = queue.pop(0)
            if isinstance(current, list):
                queue.extend(current)
                continue
            if not isinstance(current, dict):
                continue
            t = str(current.get("@type") or "").lower()
            if t == "product" or (isinstance(current.get("@type"), list) and any(str(x).lower() == "product" for x in current.get("@type") or [])):
                products.append(current)
            for key in ("@graph", "itemListElement", "mainEntity"):
                nested = current.get(key)
                if isinstance(nested, (list, dict)):
                    queue.append(nested)
    return products


def _normalize_image_url(url: str | None, page_url: str | None = None) -> str | None:
    text = _clean_text(url)
    if not text:
        return None
    if text.startswith("//"):
        text = "https:" + text
    if text.startswith("/") and page_url:
        text = urljoin(page_url, text)
    if not text.lower().startswith(("http://", "https://")):
        return None
    return text


def _extract_image_urls(soup: BeautifulSoup, page_url: str | None = None) -> list[str]:
    image_urls: list[str] = []
    seen: set[str] = set()

    selectors = [
        "meta[property='og:image']",
        "meta[name='twitter:image']",
        ".product-gallery img",
        ".gallery img",
        ".product img",
        "img",
    ]
    for selector in selectors:
        nodes = soup.select(selector)
        for node in nodes:
            candidate = None
            if node.name == "meta":
                candidate = node.get("content")
            else:
                candidate = node.get("src") or node.get("data-src") or node.get("data-original")
            normalized = _normalize_image_url(candidate, page_url=page_url)
            if not normalized or normalized in seen:
                continue
            if not re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", normalized.lower()):
                # Allow image URLs without explicit extension only for OG/Twitter images.
                if selector not in ("meta[property='og:image']", "meta[name='twitter:image']"):
                    continue
            seen.add(normalized)
            image_urls.append(normalized)
            if len(image_urls) >= 12:
                return image_urls
    return image_urls


def _extract_attributes(soup: BeautifulSoup) -> dict[str, str]:
    attributes: dict[str, str] = {}

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = _clean_text(cells[0].get_text(" ", strip=True))
            value = _clean_text(cells[1].get_text(" ", strip=True))
            if key and value and key not in attributes:
                attributes[key] = value

    for node in soup.select("li, p, div"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text or ":" not in text or len(text) > 260:
            continue
        left, right = text.split(":", 1)
        key = _clean_text(left)
        value = _clean_text(right)
        if not key or not value or len(key) > 90:
            continue
        if key not in attributes:
            attributes[key] = value

    return attributes


def _extract_breadcrumb_category(soup: BeautifulSoup) -> str | None:
    selectors = [
        "nav.breadcrumbs a",
        ".breadcrumbs a",
        "[itemtype*='BreadcrumbList'] a",
        ".breadcrumb a",
    ]
    for selector in selectors:
        crumbs = [_clean_text(x.get_text(" ", strip=True)) for x in soup.select(selector)]
        crumbs = [c for c in crumbs if c]
        if len(crumbs) < 2:
            continue
        # last crumb is often product name, so prefer previous one.
        for idx in range(len(crumbs) - 2, -1, -1):
            candidate = crumbs[idx]
            if candidate.lower() not in GENERIC_CATEGORY_WORDS:
                return candidate
    return None


def _parse_dimensions_from_text(page_text: str) -> dict[str, float | None]:
    lowered = page_text.lower()
    out: dict[str, float | None] = {
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
                out[field] = _convert_weight(value, unit)
            else:
                out[field] = _convert_dimension(value, unit)
            break
    return out


def _apply_dimension_attributes(result: dict[str, Any]) -> None:
    key_map = {
        "weight": ("вес", "weight"),
        "gross_weight": ("вес брутто", "gross", "gross weight"),
        "length": ("длина", "length"),
        "width": ("ширина", "width"),
        "height": ("высота", "height"),
        "package_length": ("длина упаковки", "package length"),
        "package_width": ("ширина упаковки", "package width"),
        "package_height": ("высота упаковки", "package height"),
    }
    for attr_name, attr_value in (result.get("attributes") or {}).items():
        key_l = _clean_text(attr_name).lower()
        value_t = _clean_text(attr_value)
        if not value_t:
            continue
        for field, hints in key_map.items():
            if result.get(field) is not None:
                continue
            if not any(h in key_l for h in hints):
                continue
            match = re.search(r"(\d+[\.,]?\d*)\s*(кг|г|kg|g|мм|см|м|mm|cm|m)?", value_t.lower())
            if not match:
                continue
            val = _to_float(match.group(1))
            unit = match.group(2)
            if field in ("weight", "gross_weight"):
                result[field] = _convert_weight(val, unit)
            else:
                result[field] = _convert_dimension(val, unit)


def _extract_candidate_product_urls(soup: BeautifulSoup, page_url: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    parsed_base = urlparse(page_url)
    for a in soup.find_all("a", href=True):
        href = _clean_text(a.get("href"))
        if not href or href.startswith("#") or href.lower().startswith("javascript"):
            continue
        full = urljoin(page_url, href)
        parsed = urlparse(full)
        if parsed.netloc and parsed.netloc != parsed_base.netloc:
            continue
        lower_full = full.lower()
        if not any(h in lower_full for h in PRODUCT_LINK_HINTS):
            # skip generic links that don't look like product pages
            if not re.search(r"[a-z0-9\-_]{5,}", lower_full):
                continue
        if full in seen:
            continue
        seen.add(full)
        links.append(full)
        if len(links) >= 200:
            break
    return links


def _score_link_by_hints(url: str, title: str, hints: list[str]) -> float:
    src = f"{url} {title}".lower()
    score = 0.0
    for hint in hints:
        clean_hint = _clean_text(hint).lower()
        if not clean_hint:
            continue
        if clean_hint in src:
            score += 2.0
        tokens = [t for t in re.findall(r"[a-zA-Zа-яА-Я0-9]+", clean_hint) if len(t) >= 3]
        for tok in tokens:
            if tok in src:
                score += 0.6
    if any(h in url.lower() for h in PRODUCT_LINK_HINTS):
        score += 0.8
    return score


def _best_product_url_from_listing(soup: BeautifulSoup, page_url: str, hints: list[str]) -> str | None:
    scored: list[tuple[float, str]] = []
    for a in soup.find_all("a", href=True):
        href = _clean_text(a.get("href"))
        if not href:
            continue
        full = urljoin(page_url, href)
        title = _clean_text(a.get_text(" ", strip=True))
        score = _score_link_by_hints(full, title, hints)
        if score <= 0:
            continue
        scored.append((score, full))
    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def _detect_page_kind(soup: BeautifulSoup, page_url: str, raw_product_count: int) -> str:
    listing_signals = 0
    listing_signals += len(soup.select(".product-card, .catalog-item, .products-item, .product-item, .item-card"))
    listing_signals += len(soup.select(".pagination a, .pager a"))
    if re.search(r"(search=|/search|\?q=|\?s=)", page_url.lower()):
        listing_signals += 2

    product_signals = 0
    if soup.select_one("h1"):
        product_signals += 1
    if soup.select_one("meta[property='og:type'][content='product']"):
        product_signals += 2
    if raw_product_count > 0:
        product_signals += 2
    if soup.select_one("[itemtype*='Product']"):
        product_signals += 2

    if listing_signals >= 4 and product_signals <= 2:
        return "listing"
    return "product"


def fetch_supplier_page(url: str, timeout: float = 10.0) -> str:
    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=timeout) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def extract_supplier_data(html: str, url: str | None = None) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    page_url = url or ""

    json_ld_products = _extract_json_ld_products(soup)
    product_ld = json_ld_products[0] if json_ld_products else {}

    title = _clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")
    h1 = soup.select_one("h1")
    h1_text = _clean_text(h1.get_text(" ", strip=True) if h1 else "")

    name = _clean_text(product_ld.get("name") or h1_text or title)

    description = None
    if isinstance(product_ld.get("description"), str):
        description = _clean_text(product_ld.get("description"))
    if not description:
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            description = _clean_text(meta_desc.get("content"))
    if not description:
        for selector in (".description", ".product-description", "#description", ".tab-description"):
            node = soup.select_one(selector)
            if not node:
                continue
            text = _clean_text(node.get_text(" ", strip=True))
            if text:
                description = text
                break

    attributes = _extract_attributes(soup)

    image_urls = _extract_image_urls(soup, page_url=page_url)
    ld_images = product_ld.get("image")
    if isinstance(ld_images, str):
        normalized = _normalize_image_url(ld_images, page_url=page_url)
        if normalized and normalized not in image_urls:
            image_urls.insert(0, normalized)
    elif isinstance(ld_images, list):
        for item in ld_images:
            normalized = _normalize_image_url(str(item), page_url=page_url)
            if normalized and normalized not in image_urls:
                image_urls.append(normalized)

    category_guess = _extract_breadcrumb_category(soup)

    brand = None
    brand_ld = product_ld.get("brand")
    if isinstance(brand_ld, dict):
        brand = _clean_text(brand_ld.get("name"))
    elif isinstance(brand_ld, str):
        brand = _clean_text(brand_ld)

    page_text = soup.get_text(" ", strip=True)
    dim_values = _parse_dimensions_from_text(page_text)

    result: dict[str, Any] = {
        "title": title or None,
        "name": name or None,
        "brand": brand or None,
        "category": category_guess,
        "description": description or None,
        "image_urls": image_urls,
        "attributes": attributes,
        "weight": dim_values.get("weight"),
        "gross_weight": dim_values.get("gross_weight"),
        "length": dim_values.get("length"),
        "width": dim_values.get("width"),
        "height": dim_values.get("height"),
        "package_length": dim_values.get("package_length"),
        "package_width": dim_values.get("package_width"),
        "package_height": dim_values.get("package_height"),
    }

    _apply_dimension_attributes(result)

    brand_keys = {"бренд", "торговая марка", "brand", "производитель", "manufacturer"}
    for key, value in attributes.items():
        if _clean_text(key).lower() in brand_keys and _clean_text(value):
            result["brand"] = _clean_text(value)
            break

    page_kind = _detect_page_kind(soup, page_url, raw_product_count=len(json_ld_products))
    candidate_product_urls = _extract_candidate_product_urls(soup, page_url=page_url) if page_url else []

    result["page_kind"] = page_kind
    result["candidate_product_urls"] = candidate_product_urls
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
        "page_kind": raw_data.get("page_kind") or "product",
    }

    if normalized["image_urls"]:
        normalized["image_url"] = normalized["image_urls"][0]

    return normalized


def has_meaningful_supplier_data(parsed: dict[str, Any]) -> bool:
    if not parsed:
        return False
    scalar_fields = [
        parsed.get("description"),
        parsed.get("image_url"),
        parsed.get("weight"),
        parsed.get("gross_weight"),
        parsed.get("length"),
        parsed.get("width"),
        parsed.get("height"),
        parsed.get("package_length"),
        parsed.get("package_width"),
        parsed.get("package_height"),
    ]
    if any(v not in (None, "") for v in scalar_fields):
        return True
    attrs = parsed.get("attributes") or {}
    return len(attrs) >= 2


def parse_supplier_product_page(
    url: str,
    hints: list[str] | None = None,
    timeout: float = 10.0,
    max_hops: int = 1,
) -> dict[str, Any]:
    raw_url = _clean_text(url)
    if not raw_url:
        raise ValueError("Пустой supplier_url")

    html = fetch_supplier_page(raw_url, timeout=timeout)
    raw = extract_supplier_data(html, raw_url)
    parsed = normalize_supplier_data(raw)
    resolved_url = raw_url

    if parsed.get("page_kind") != "product" and max_hops > 0:
        soup = BeautifulSoup(html, "html.parser")
        best_url = _best_product_url_from_listing(
            soup,
            page_url=raw_url,
            hints=[h for h in (hints or []) if _clean_text(h)],
        )
        if best_url and best_url != raw_url:
            second_html = fetch_supplier_page(best_url, timeout=timeout)
            second_raw = extract_supplier_data(second_html, best_url)
            second_parsed = normalize_supplier_data(second_raw)
            if has_meaningful_supplier_data(second_parsed):
                parsed = second_parsed
                resolved_url = best_url

    parsed["resolved_url"] = resolved_url
    parsed["resolved_from_listing"] = bool(resolved_url != raw_url)
    return parsed


def fallback_search_product_data(query: str, timeout: float = 8.0, max_results: int = 3) -> dict[str, Any]:
    text_query = _clean_text(query)
    if not text_query:
        return {}

    try:
        search_url = f"https://duckduckgo.com/html/?q={quote_plus(text_query)}"
        html = fetch_supplier_page(search_url, timeout=timeout)
        soup = BeautifulSoup(html, "html.parser")
        links: list[str] = []
        for a in soup.select("a.result__a, a[href]"):
            href = _clean_text(a.get("href"))
            if not href:
                continue
            if href.startswith("/"):
                href = urljoin("https://duckduckgo.com", href)
            if not href.lower().startswith(("http://", "https://")):
                continue
            if "duckduckgo.com" in href:
                continue
            links.append(href)
            if len(links) >= max(3, int(max_results) * 2):
                break

        best: dict[str, Any] | None = None
        best_score = -1
        for link in links[: max(1, int(max_results))]:
            try:
                page_html = fetch_supplier_page(link, timeout=timeout)
                parsed = normalize_supplier_data(extract_supplier_data(page_html, link))
            except Exception:
                continue
            score = 0
            if parsed.get("description"):
                score += 2
            if parsed.get("image_url"):
                score += 2
            if any(parsed.get(x) is not None for x in ("weight", "length", "width", "height", "gross_weight")):
                score += 2
            score += min(3, len(parsed.get("attributes") or {}))
            if score > best_score:
                best_score = score
                best = parsed
                best["fallback_url"] = link
        return best or {}
    except Exception:
        return {}
