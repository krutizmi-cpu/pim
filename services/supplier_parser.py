from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

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

GENERIC_PRODUCT_PAGE_WORDS = {
    "каталог",
    "товары",
    "продукция",
    "поиск",
    "главная",
    "новинки",
    "catalog",
    "products",
    "search",
    "shop",
}

SEARCH_BLOCKED_DOMAINS = (
    "duckduckgo.com",
    "bing.com",
    "google.",
    "yandex.",
    "youtube.com",
    "facebook.com",
    "instagram.com",
    "tiktok.com",
    "vk.com",
    "ok.ru",
    "wikipedia.org",
)

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


def _compact_text(value: str | None) -> str:
    return re.sub(r"[\s\-_]+", "", _clean_text(value).lower())


def _is_probably_product_code(token: str) -> bool:
    token = str(token or "").strip().lower()
    if len(token) < 4:
        return False
    has_digit = any(ch.isdigit() for ch in token)
    has_alpha = any(ch.isalpha() for ch in token)
    return has_digit and has_alpha


def _build_hint_tokens(hints: list[str] | None) -> tuple[list[str], list[str]]:
    strong_phrases: list[str] = []
    tokens: list[str] = []
    seen_tokens: set[str] = set()
    seen_phrases: set[str] = set()
    for raw in hints or []:
        clean = _clean_text(raw).lower()
        if not clean:
            continue
        compact = _compact_text(clean)
        if len(compact) >= 4 and _is_probably_product_code(compact) and compact not in seen_phrases:
            seen_phrases.add(compact)
            strong_phrases.append(compact)
        for tok in re.findall(r"[a-zA-Zа-яА-Я0-9]+", clean):
            low = tok.lower()
            if len(low) < 3:
                continue
            if low in seen_tokens:
                continue
            seen_tokens.add(low)
            tokens.append(low)
    return strong_phrases, tokens


def _is_listing_like_url(url: str) -> bool:
    low = str(url or "").lower()
    return bool(re.search(r"(category=|/catalog/?$|/search|[?&]q=|[?&]s=|/catalog/\?|/search\?)", low))


def _is_blocked_search_domain(url: str, preferred_domain: str | None = None) -> bool:
    host = (urlparse(url).netloc or "").lower()
    if not host:
        return True
    pref = str(preferred_domain or "").lower().replace("www.", "").strip()
    if pref and pref in host.replace("www.", ""):
        return False
    return any(d in host for d in SEARCH_BLOCKED_DOMAINS)


def _normalize_result_url(href: str) -> str | None:
    candidate = _clean_text(href)
    if not candidate:
        return None
    if candidate.startswith("//"):
        candidate = "https:" + candidate
    if candidate.startswith("/"):
        candidate = urljoin("https://duckduckgo.com", candidate)
    if not candidate.lower().startswith(("http://", "https://")):
        return None
    parsed = urlparse(candidate)
    if "duckduckgo.com" in parsed.netloc:
        qs = parse_qs(parsed.query or "")
        if "uddg" in qs and qs["uddg"]:
            target = unquote(str(qs["uddg"][0]))
            if target.lower().startswith(("http://", "https://")):
                candidate = target
    return candidate


def _extract_article_like_value(parsed: dict[str, Any]) -> str:
    attrs = parsed.get("attributes") or {}
    for key, value in attrs.items():
        key_l = _clean_text(key).lower()
        if key_l in ("артикул", "код товара", "sku", "article", "код", "модель"):
            value_t = _clean_text(value)
            if value_t:
                return value_t
    name = _clean_text(parsed.get("name") or "")
    m = re.search(r"\b([a-zа-я0-9][a-zа-я0-9\-_]{3,})\b", name, flags=re.IGNORECASE)
    return m.group(1) if m else ""


def _data_quality_score(parsed: dict[str, Any]) -> float:
    score = 0.0
    if _clean_text(parsed.get("name")):
        score += 1.2
    if _clean_text(parsed.get("description")):
        score += 1.3
    if parsed.get("image_url"):
        score += 1.2
    dims = ("weight", "gross_weight", "length", "width", "height", "package_length", "package_width", "package_height")
    filled_dims = sum(1 for k in dims if parsed.get(k) not in (None, "", 0, 0.0))
    score += min(2.5, float(filled_dims) * 0.5)
    attrs = parsed.get("attributes") or {}
    score += min(2.0, float(len(attrs)) * 0.12)
    if parsed.get("page_kind") == "product":
        score += 1.4
    if parsed.get("listing_only"):
        score -= 2.5
    return score


def _relevance_score(
    parsed: dict[str, Any],
    url: str,
    hints: list[str] | None = None,
    preferred_domain: str | None = None,
) -> float:
    score = _data_quality_score(parsed)
    low_url = str(url or "").lower()
    name = _clean_text(parsed.get("name") or "").lower()
    title = _clean_text(parsed.get("title") or "").lower()
    article_like = _clean_text(_extract_article_like_value(parsed)).lower()
    source_text = f"{low_url} {name} {title} {article_like}"
    source_compact = _compact_text(source_text)

    strong_phrases, tokens = _build_hint_tokens(hints)
    strong_hits = 0
    for phrase in strong_phrases:
        if phrase and phrase in source_compact:
            score += 4.5
            strong_hits += 1
    token_hits = 0
    for tok in tokens:
        if len(tok) < 3:
            continue
        if tok in source_text:
            score += 0.5 if len(tok) < 5 else 0.9
            token_hits += 1

    if strong_phrases and strong_hits == 0:
        score -= 4.0
    if tokens and token_hits == 0:
        score -= 1.5

    if preferred_domain:
        pref = preferred_domain.lower().replace("www.", "")
        host = (urlparse(url).netloc or "").lower().replace("www.", "")
        if pref and pref in host:
            score += 1.8
        else:
            score -= 0.8

    if _is_listing_like_url(low_url):
        score -= 1.2
    if _clean_text(name).lower() in GENERIC_PRODUCT_PAGE_WORDS:
        score -= 2.0
    if not _clean_text(name):
        score -= 0.8
    return score

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

    # Parse combined patterns like "размеры 120x30x70 см" or "ДхШхВ 120*30*70 мм".
    triplet_patterns = [
        r"(упаков[а-яa-z\s]{0,20})?(\d+[\.,]?\d*)\s*[xх\*]\s*(\d+[\.,]?\d*)\s*[xх\*]\s*(\d+[\.,]?\d*)\s*(мм|см|м|mm|cm|m)",
    ]
    for pattern in triplet_patterns:
        for match in re.finditer(pattern, lowered, flags=re.IGNORECASE):
            prefix = (match.group(1) or "").strip()
            v1 = _to_float(match.group(2))
            v2 = _to_float(match.group(3))
            v3 = _to_float(match.group(4))
            unit = match.group(5)
            if v1 is None or v2 is None or v3 is None:
                continue
            d1 = _convert_dimension(v1, unit)
            d2 = _convert_dimension(v2, unit)
            d3 = _convert_dimension(v3, unit)
            if "упаков" in prefix:
                if out["package_length"] is None:
                    out["package_length"] = d1
                if out["package_width"] is None:
                    out["package_width"] = d2
                if out["package_height"] is None:
                    out["package_height"] = d3
            else:
                if out["length"] is None:
                    out["length"] = d1
                if out["width"] is None:
                    out["width"] = d2
                if out["height"] is None:
                    out["height"] = d3
    return out


def _parse_triplet_value(value_t: str) -> tuple[float, float, float, str] | None:
    m = re.search(
        r"(\d+[\.,]?\d*)\s*[xх\*]\s*(\d+[\.,]?\d*)\s*[xх\*]\s*(\d+[\.,]?\d*)\s*(мм|см|м|mm|cm|m)?",
        value_t.lower(),
    )
    if not m:
        return None
    v1 = _to_float(m.group(1))
    v2 = _to_float(m.group(2))
    v3 = _to_float(m.group(3))
    unit = m.group(4) or "см"
    if v1 is None or v2 is None or v3 is None:
        return None
    return (
        float(_convert_dimension(v1, unit) or 0.0),
        float(_convert_dimension(v2, unit) or 0.0),
        float(_convert_dimension(v3, unit) or 0.0),
        unit,
    )


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
        triplet = _parse_triplet_value(value_t)
        if triplet:
            d1, d2, d3, _unit = triplet
            if any(h in key_l for h in ("габарит", "размер", "дхшхв", "lwh", "dimension")):
                if any(h in key_l for h in ("упаков", "короб", "package", "packing", "box")):
                    if result.get("package_length") is None:
                        result["package_length"] = d1
                    if result.get("package_width") is None:
                        result["package_width"] = d2
                    if result.get("package_height") is None:
                        result["package_height"] = d3
                else:
                    if result.get("length") is None:
                        result["length"] = d1
                    if result.get("width") is None:
                        result["width"] = d2
                    if result.get("height") is None:
                        result["height"] = d3
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


def _build_strong_hint_sets(hints: list[str]) -> tuple[set[str], set[str]]:
    strong_tokens: set[str] = set()
    strong_phrases: set[str] = set()
    for hint in hints:
        clean = _clean_text(hint).lower()
        if not clean:
            continue
        compact = re.sub(r"[\s\-_]+", "", clean)
        has_digit = any(ch.isdigit() for ch in compact)
        has_alpha = any(ch.isalpha() for ch in compact)
        if len(compact) >= 5 and has_digit and has_alpha:
            strong_phrases.add(compact)
        for tok in re.findall(r"[a-zA-Zа-яА-Я0-9]+", clean):
            if len(tok) >= 4 and (any(ch.isdigit() for ch in tok) or len(tok) >= 6):
                strong_tokens.add(tok.lower())
    return strong_tokens, strong_phrases


def _best_product_url_from_listing(soup: BeautifulSoup, page_url: str, hints: list[str]) -> str | None:
    urls = _ranked_product_urls_from_listing(soup, page_url=page_url, hints=hints, max_urls=1)
    return urls[0] if urls else None


def _ranked_product_urls_from_listing(
    soup: BeautifulSoup,
    page_url: str,
    hints: list[str],
    max_urls: int = 6,
) -> list[str]:
    scored: list[tuple[float, str]] = []
    strong_tokens, strong_phrases = _build_strong_hint_sets(hints)
    has_strong_hints = bool(strong_tokens or strong_phrases)
    for a in soup.find_all("a", href=True):
        href = _clean_text(a.get("href"))
        if not href:
            continue
        full = urljoin(page_url, href)
        low_full = full.lower()
        # De-prioritize obvious listing/search links.
        if re.search(r"(category=|/catalog/?$|/search|[?&]q=|[?&]s=)", low_full):
            continue
        title = _clean_text(a.get_text(" ", strip=True))
        score = _score_link_by_hints(full, title, hints)
        src_norm = re.sub(r"[\s\-_]+", "", f"{low_full} {title.lower()}")
        strong_hit = False
        for token in strong_tokens:
            if token in low_full or token in title.lower():
                score += 4.0
                strong_hit = True
        for phrase in strong_phrases:
            if phrase in src_norm:
                score += 6.0
                strong_hit = True
        if has_strong_hints and not strong_hit:
            score -= 3.0
        if score <= 0:
            continue
        scored.append((score, full))
    if not scored:
        return []
    scored.sort(key=lambda x: x[0], reverse=True)
    ranked: list[str] = []
    seen: set[str] = set()
    for _, url in scored:
        if url in seen:
            continue
        seen.add(url)
        ranked.append(url)
        if len(ranked) >= max(1, int(max_urls)):
            break
    return ranked


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

    preferred_domain = (urlparse(raw_url).netloc or "").lower().replace("www.", "")
    clean_hints = [h for h in (hints or []) if _clean_text(h)]

    html = fetch_supplier_page(raw_url, timeout=timeout)
    raw = extract_supplier_data(html, raw_url)
    parsed = normalize_supplier_data(raw)
    resolved_url = raw_url
    best_score = _relevance_score(parsed, resolved_url, hints=clean_hints, preferred_domain=preferred_domain)

    should_try_candidates = (
        max_hops > 0
        and (
            parsed.get("page_kind") != "product"
            or not has_meaningful_supplier_data(parsed)
            or _is_listing_like_url(raw_url)
        )
    )
    if should_try_candidates:
        soup = BeautifulSoup(html, "html.parser")
        ranked_urls = _ranked_product_urls_from_listing(
            soup,
            page_url=raw_url,
            hints=clean_hints,
            max_urls=max(2, min(8, int(max_hops) * 4)),
        )
        if not ranked_urls:
            ranked_urls = list(raw.get("candidate_product_urls") or [])[: max(2, min(8, int(max_hops) * 4))]

        for candidate_url in ranked_urls:
            if candidate_url == raw_url:
                continue
            try:
                second_html = fetch_supplier_page(candidate_url, timeout=timeout)
                second_raw = extract_supplier_data(second_html, candidate_url)
                second_parsed = normalize_supplier_data(second_raw)
            except Exception:
                continue

            candidate_score = _relevance_score(
                second_parsed,
                candidate_url,
                hints=clean_hints,
                preferred_domain=preferred_domain,
            )
            if candidate_score > best_score:
                parsed = second_parsed
                resolved_url = candidate_url
                best_score = candidate_score

    listing_only = parsed.get("page_kind") != "product"
    if listing_only:
        # Avoid writing generic listing data to all products.
        for key in (
            "weight",
            "gross_weight",
            "length",
            "width",
            "height",
            "package_length",
            "package_width",
            "package_height",
            "category",
        ):
            parsed[key] = None
        parsed["attributes"] = {}
        parsed["image_urls"] = []
        parsed["image_url"] = None

    parsed["resolved_url"] = resolved_url
    parsed["resolved_from_listing"] = bool(resolved_url != raw_url)
    parsed["listing_only"] = bool(listing_only)
    parsed["relevance_score"] = round(float(best_score), 3)
    return parsed


def _search_duckduckgo_links(query: str, timeout: float, max_links: int, preferred_domain: str | None = None) -> list[str]:
    links: list[str] = []
    try:
        search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
        html = fetch_supplier_page(search_url, timeout=timeout)
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("a.result__a, .result__title a, a[href]"):
            normalized = _normalize_result_url(str(a.get("href") or ""))
            if not normalized:
                continue
            if _is_blocked_search_domain(normalized, preferred_domain=preferred_domain):
                continue
            links.append(normalized)
            if len(links) >= max_links:
                break
    except Exception:
        return []
    return links


def _search_bing_links(query: str, timeout: float, max_links: int, preferred_domain: str | None = None) -> list[str]:
    links: list[str] = []
    try:
        search_url = f"https://www.bing.com/search?q={quote_plus(query)}"
        html = fetch_supplier_page(search_url, timeout=timeout)
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("li.b_algo h2 a, h2 a[href], a[href]"):
            href = _normalize_result_url(str(a.get("href") or ""))
            if not href:
                continue
            if _is_blocked_search_domain(href, preferred_domain=preferred_domain):
                continue
            links.append(href)
            if len(links) >= max_links:
                break
    except Exception:
        return []
    return links


def _build_search_queries(
    query: str,
    hints: list[str] | None = None,
    preferred_domain: str | None = None,
) -> list[str]:
    base = _clean_text(query)
    if not base:
        return []
    strong_phrases, tokens = _build_hint_tokens((hints or []) + [base])
    variants: list[str] = [base]
    if strong_phrases:
        variants.append(f"\"{strong_phrases[0]}\" {base}")
    if tokens:
        variants.append(" ".join(tokens[:5]))
    if preferred_domain:
        dom = preferred_domain.lower().replace("www.", "").strip()
        if dom:
            variants.append(f"site:{dom} {base}")
            if strong_phrases:
                variants.append(f"site:{dom} \"{strong_phrases[0]}\"")
    deduped: list[str] = []
    seen: set[str] = set()
    for v in variants:
        clean = _clean_text(v)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        deduped.append(clean)
    return deduped[:5]


def fallback_search_product_data(
    query: str,
    timeout: float = 8.0,
    max_results: int = 3,
    hints: list[str] | None = None,
    preferred_domain: str | None = None,
) -> dict[str, Any]:
    text_query = _clean_text(query)
    if not text_query:
        return {}

    query_variants = _build_search_queries(text_query, hints=hints, preferred_domain=preferred_domain)
    all_hints = [text_query] + [h for h in (hints or []) if _clean_text(h)]
    seen_links: set[str] = set()
    best: dict[str, Any] | None = None
    best_score = -999.0
    considered = 0
    max_candidates = max(6, int(max_results) * 6)

    for q in query_variants:
        candidate_links: list[str] = []
        candidate_links.extend(
            _search_duckduckgo_links(
                q,
                timeout=timeout,
                max_links=max(6, int(max_results) * 3),
                preferred_domain=preferred_domain,
            )
        )
        candidate_links.extend(
            _search_bing_links(
                q,
                timeout=timeout,
                max_links=max(6, int(max_results) * 3),
                preferred_domain=preferred_domain,
            )
        )
        for link in candidate_links:
            if link in seen_links:
                continue
            seen_links.add(link)
            considered += 1
            if considered > max_candidates:
                break
            try:
                parsed = parse_supplier_product_page(link, hints=all_hints, timeout=timeout, max_hops=1)
            except Exception:
                continue

            resolved = str(parsed.get("resolved_url") or link)
            score = _relevance_score(
                parsed,
                resolved,
                hints=all_hints,
                preferred_domain=preferred_domain,
            )
            if q != text_query:
                score += 0.2
            if score > best_score and has_meaningful_supplier_data(parsed):
                best = parsed
                best_score = score
                best["fallback_url"] = resolved
                best["fallback_query"] = q
                best["fallback_score"] = round(float(score), 3)
        if considered > max_candidates:
            break

    if not best:
        return {}
    if best_score < 1.8 and not any(best.get(k) not in (None, "", 0, 0.0) for k in ("weight", "length", "width", "height", "image_url")):
        return {}
    return best
