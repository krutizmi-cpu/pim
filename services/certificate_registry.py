from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import httpx


FSA_ALLOWED_HOSTS = ("pub.fsa.gov.ru",)
DECLARATION_PATTERNS = ("/rds/declaration", "/rds/declaration/view")
CERTIFICATE_PATTERNS = ("/rss/certificate", "/rss/certificate/view")


def _clean_text(value: Any) -> str:
    text = str(value or "")
    text = unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _safe_slug(value: str, fallback: str = "doc") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    text = re.sub(r'[<>:"/\\|?*\x00-\x1F]+', "_", text)
    text = re.sub(r"\s+", "_", text).strip("._ ")
    return text[:140] or fallback


def _is_fsa_url(url: str | None) -> bool:
    host = (urlparse(str(url or "")).netloc or "").lower()
    return any(host.endswith(item) for item in FSA_ALLOWED_HOSTS)


def _build_registry_queries(
    legal_entity: str,
    product_name: str,
    product_kind: str,
    tnved_code: str,
) -> list[tuple[str, str]]:
    site_queries: list[tuple[str, str]] = []
    terms = [str(legal_entity or "").strip(), str(product_kind or "").strip(), str(product_name or "").strip(), str(tnved_code or "").strip()]
    compact = [t for t in terms if t]
    if not compact:
        return []
    declaration_query = " ".join(["site:pub.fsa.gov.ru/rds/declaration"] + compact)
    certificate_query = " ".join(["site:pub.fsa.gov.ru/rss/certificate"] + compact)
    site_queries.append(("declaration", declaration_query))
    site_queries.append(("certificate", certificate_query))
    if tnved_code:
        site_queries.append(("declaration", " ".join(["site:pub.fsa.gov.ru/rds/declaration", str(legal_entity or "").strip(), str(tnved_code or "").strip()])))
        site_queries.append(("certificate", " ".join(["site:pub.fsa.gov.ru/rss/certificate", str(legal_entity or "").strip(), str(tnved_code or "").strip()])))
    if product_name:
        site_queries.append(("declaration", " ".join(["site:pub.fsa.gov.ru/rds/declaration", str(product_name or "").strip()])))
        site_queries.append(("certificate", " ".join(["site:pub.fsa.gov.ru/rss/certificate", str(product_name or "").strip()])))
    return site_queries


def _bing_rss_search(query: str, timeout: float = 25.0) -> list[dict[str, str]]:
    url = "https://www.bing.com/search?format=rss&q=" + quote(query)
    headers = {"User-Agent": "Mozilla/5.0"}
    with httpx.Client(follow_redirects=True, timeout=timeout, headers=headers) as client:
        response = client.get(url)
        response.raise_for_status()
    root = ET.fromstring(response.text)
    results: list[dict[str, str]] = []
    for item in root.findall(".//item"):
        title = _clean_text(item.findtext("title") or "")
        link = str(item.findtext("link") or "").strip()
        description = _clean_text(item.findtext("description") or "")
        if not link:
            continue
        results.append({"title": title, "link": link, "description": description})
    return results


def search_fsa_registry_candidates(
    legal_entity: str,
    product_name: str,
    product_kind: str,
    tnved_code: str,
    max_results: int = 10,
    timeout: float = 25.0,
) -> dict[str, Any]:
    queries = _build_registry_queries(
        legal_entity=legal_entity,
        product_name=product_name,
        product_kind=product_kind,
        tnved_code=tnved_code,
    )
    if not queries:
        return {"ok": False, "error": "Недостаточно данных для поиска", "items": []}

    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    errors: list[str] = []
    for search_kind, query in queries:
        try:
            rows = _bing_rss_search(query, timeout=timeout)
        except Exception as e:
            errors.append(f"{search_kind}: {e}")
            continue
        for row in rows:
            link = str(row.get("link") or "").strip()
            if not _is_fsa_url(link):
                continue
            if link in seen:
                continue
            kind = search_kind
            low_link = link.lower()
            if any(p in low_link for p in DECLARATION_PATTERNS):
                kind = "declaration"
            elif any(p in low_link for p in CERTIFICATE_PATTERNS):
                kind = "certificate"
            items.append(
                {
                    "kind": kind,
                    "title": str(row.get("title") or "").strip(),
                    "link": link,
                    "description": str(row.get("description") or "").strip(),
                    "query": query,
                }
            )
            seen.add(link)
            if len(items) >= int(max_results):
                return {"ok": True, "items": items, "errors": errors, "queries": [q for _, q in queries]}
    return {"ok": bool(items), "items": items, "errors": errors, "queries": [q for _, q in queries]}


def _extract_pdf_links(html: str, base_url: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        href = str(match.group(1) or "").strip()
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        if ".pdf" in abs_url.lower() and abs_url not in seen:
            seen.add(abs_url)
            links.append(abs_url)
    return links


def _parse_dates(text: str) -> dict[str, str | None]:
    patterns = {
        "valid_from": [
            r"(?:дата\s+регистрации|дата\s+начала\s+действия|действует\s+с)\s*[:\-]?\s*(\d{2}\.\d{2}\.\d{4})",
        ],
        "valid_to": [
            r"(?:срок\s+действия\s+до|дата\s+окончания\s+действия|действует\s+до)\s*[:\-]?\s*(\d{2}\.\d{2}\.\d{4})",
        ],
    }
    out: dict[str, str | None] = {"valid_from": None, "valid_to": None}
    for key, regexes in patterns.items():
        for regex in regexes:
            match = re.search(regex, text, flags=re.IGNORECASE)
            if match:
                out[key] = str(match.group(1)).strip()
                break
    return out


def _parse_doc_number(text: str) -> str | None:
    regexes = [
        r"(?:регистрационный\s+номер|номер\s+декларации|номер\s+сертификата)\s*[:\-]?\s*([A-ZА-Я0-9\.\-\/\s№]+)",
        r"\b(ЕАЭС\s*N\s*[A-ZА-Я0-9\.\-\/]+)\b",
        r"\b(РОСС\s*[A-ZА-Я0-9\.\-\/]+)\b",
    ]
    for regex in regexes:
        match = re.search(regex, text, flags=re.IGNORECASE)
        if match:
            return _clean_text(match.group(1))[:240]
    return None


def _parse_authority(text: str) -> str | None:
    regexes = [
        r"(?:орган\s+по\s+сертификации|зарегистрировавший\s+орган)\s*[:\-]?\s*(.{5,240})",
    ]
    for regex in regexes:
        match = re.search(regex, text, flags=re.IGNORECASE)
        if match:
            value = _clean_text(match.group(1))
            if value:
                return value[:240]
    return None


def _parse_applicant(text: str) -> str | None:
    regexes = [
        r"(?:заявитель|изготовитель|продавец)\s*[:\-]?\s*(.{5,240})",
    ]
    for regex in regexes:
        match = re.search(regex, text, flags=re.IGNORECASE)
        if match:
            value = _clean_text(match.group(1))
            if value:
                return value[:240]
    return None


def _parse_tnved(text: str) -> str | None:
    match = re.search(r"(?:тн\s*вэд|тнвэд)\s*[:\-]?\s*([0-9\s]{6,20})", text, flags=re.IGNORECASE)
    if match:
        return re.sub(r"\D+", "", str(match.group(1)))
    return None


def _extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    try:
        from pypdf import PdfReader
    except Exception:
        return ""
    try:
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        chunks: list[str] = []
        for page in reader.pages[:8]:
            try:
                chunks.append(page.extract_text() or "")
            except Exception:
                continue
        return _clean_text(" ".join(chunks))
    except Exception:
        return ""


def parse_fsa_document_resource(
    source_url: str,
    timeout: float = 35.0,
) -> dict[str, Any]:
    url = str(source_url or "").strip()
    if not url:
        return {"ok": False, "error": "Пустой URL"}
    headers = {"User-Agent": "Mozilla/5.0"}
    with httpx.Client(follow_redirects=True, timeout=timeout, headers=headers) as client:
        response = client.get(url)
        response.raise_for_status()
        content_type = str(response.headers.get("content-type") or "").lower()
        final_url = str(response.url)
        pdf_bytes: bytes | None = None
        pdf_url = ""
        text = ""
        html = ""
        if ".pdf" in final_url.lower() or "application/pdf" in content_type:
            pdf_bytes = response.content
            pdf_url = final_url
            text = _extract_text_from_pdf_bytes(pdf_bytes)
        else:
            html = response.text
            text = _clean_text(html)
            pdf_links = _extract_pdf_links(html, final_url)
            if pdf_links:
                pdf_url = pdf_links[0]
                try:
                    pdf_resp = client.get(pdf_url)
                    pdf_resp.raise_for_status()
                    pdf_bytes = pdf_resp.content
                    pdf_text = _extract_text_from_pdf_bytes(pdf_bytes)
                    if pdf_text:
                        text = f"{text} {pdf_text}".strip()
                except Exception:
                    pass

    kind = "unknown"
    low_url = final_url.lower()
    if any(p in low_url for p in DECLARATION_PATTERNS):
        kind = "declaration"
    elif any(p in low_url for p in CERTIFICATE_PATTERNS):
        kind = "certificate"
    if kind == "unknown":
        low_text = text.lower()
        if "декларац" in low_text:
            kind = "declaration"
        elif "сертификат" in low_text:
            kind = "certificate"

    dates = _parse_dates(text)
    result = {
        "ok": True,
        "kind": kind,
        "source_url": final_url,
        "pdf_url": pdf_url or None,
        "pdf_bytes": pdf_bytes,
        "doc_number": _parse_doc_number(text),
        "valid_from": dates.get("valid_from"),
        "valid_to": dates.get("valid_to"),
        "authority_name": _parse_authority(text),
        "applicant_name": _parse_applicant(text),
        "tnved_code": _parse_tnved(text),
        "raw_text_excerpt": text[:4000],
    }
    return result


def save_fsa_document(
    conn,
    product_id: int,
    document: dict[str, Any],
    pdf_bytes: bytes | None = None,
) -> dict[str, Any]:
    pid = int(product_id or 0)
    if pid <= 0:
        raise ValueError("Некорректный product_id")

    file_path = None
    if pdf_bytes:
        file_dir = Path("data") / "registry_docs" / str(pid)
        file_dir.mkdir(parents=True, exist_ok=True)
        file_name = _safe_slug(str(document.get("doc_number") or document.get("kind") or "registry_doc")) + ".pdf"
        file_path = file_dir / file_name
        file_path.write_bytes(pdf_bytes)

    cur = conn.execute(
        """
        INSERT INTO product_registry_documents (
            product_id,
            doc_kind,
            doc_number,
            valid_from,
            valid_to,
            authority_name,
            applicant_name,
            tnved_code,
            source_url,
            pdf_url,
            local_file_path,
            raw_payload,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (
            pid,
            str(document.get("kind") or "").strip() or None,
            str(document.get("doc_number") or "").strip() or None,
            str(document.get("valid_from") or "").strip() or None,
            str(document.get("valid_to") or "").strip() or None,
            str(document.get("authority_name") or "").strip() or None,
            str(document.get("applicant_name") or "").strip() or None,
            str(document.get("tnved_code") or "").strip() or None,
            str(document.get("source_url") or "").strip() or None,
            str(document.get("pdf_url") or "").strip() or None,
            str(file_path) if file_path else None,
            json.dumps({k: v for k, v in document.items() if k != "pdf_bytes"}, ensure_ascii=False),
        ),
    )
    conn.commit()
    return {"id": int(cur.lastrowid), "file_path": str(file_path) if file_path else None}


def list_fsa_documents(conn, product_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM product_registry_documents
        WHERE product_id = ?
        ORDER BY id DESC
        """,
        (int(product_id),),
    ).fetchall()
    return [dict(r) for r in rows]


def delete_fsa_document(conn, document_id: int) -> dict[str, Any]:
    row = conn.execute(
        "SELECT local_file_path FROM product_registry_documents WHERE id = ? LIMIT 1",
        (int(document_id),),
    ).fetchone()
    deleted_file = False
    if row and row["local_file_path"]:
        try:
            Path(str(row["local_file_path"])).unlink(missing_ok=True)
            deleted_file = True
        except Exception:
            deleted_file = False
    cur = conn.execute("DELETE FROM product_registry_documents WHERE id = ?", (int(document_id),))
    conn.commit()
    return {"deleted": int(cur.rowcount or 0), "deleted_file": deleted_file}
