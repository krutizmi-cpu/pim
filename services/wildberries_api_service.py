from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any

import httpx


COMMON_BASE_URL = "https://common-api.wildberries.ru"
CONTENT_BASE_URL = "https://content-api.wildberries.ru"
DEFAULT_TIMEOUT = 30.0
WB_TOKEN_SETTING_KEY = "wildberries.api_token"


def _ensure_system_settings_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _get_setting(conn: sqlite3.Connection, key: str) -> str | None:
    _ensure_system_settings_table(conn)
    row = conn.execute("SELECT value FROM system_settings WHERE key = ? LIMIT 1", (str(key),)).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _set_setting(conn: sqlite3.Connection, key: str, value: str | None) -> None:
    _ensure_system_settings_table(conn)
    conn.execute(
        """
        INSERT INTO system_settings (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = CURRENT_TIMESTAMP
        """,
        (str(key), value),
    )
    conn.commit()


def get_env_token() -> str | None:
    token = os.getenv("WILDBERRIES_API_TOKEN") or os.getenv("WB_API_TOKEN") or None
    return str(token).strip() if token not in (None, "") else None


def load_settings(conn: sqlite3.Connection) -> dict[str, Any]:
    token = _get_setting(conn, WB_TOKEN_SETTING_KEY)
    return {"api_token": str(token).strip() if token not in (None, "") else ""}


def save_settings(conn: sqlite3.Connection, settings: dict[str, Any]) -> None:
    token = str(settings.get("api_token") or "").strip()
    _set_setting(conn, WB_TOKEN_SETTING_KEY, token or None)


def clear_settings(conn: sqlite3.Connection) -> None:
    _set_setting(conn, WB_TOKEN_SETTING_KEY, None)


def resolve_token(conn: sqlite3.Connection | None = None, api_token: str | None = None) -> str | None:
    token = str(api_token).strip() if api_token not in (None, "") else None
    if token:
        return token
    if conn is not None:
        saved = load_settings(conn).get("api_token")
        if saved:
            return str(saved).strip()
    return get_env_token()


def is_configured(conn: sqlite3.Connection | None = None, api_token: str | None = None) -> bool:
    return bool(resolve_token(conn, api_token))


def _headers(api_token: str | None) -> dict[str, str]:
    if not api_token:
        raise ValueError("Не задан Wildberries API token")
    return {
        "Authorization": str(api_token),
        "Content-Type": "application/json",
    }


def _request(
    method: str,
    base_url: str,
    path: str,
    api_token: str | None,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | list[Any] | None = None,
    max_retries: int = 3,
    retry_backoff_seconds: float = 1.0,
) -> dict[str, Any]:
    attempts = max(1, int(max_retries))
    for attempt in range(1, attempts + 1):
        try:
            with httpx.Client(base_url=base_url, timeout=DEFAULT_TIMEOUT) as client:
                response = client.request(
                    method.upper(),
                    path,
                    headers=_headers(api_token),
                    params=params,
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
            if isinstance(data, dict) and data.get("error") and data.get("errorText"):
                raise RuntimeError(str(data.get("errorText")))
            return data if isinstance(data, dict) else {"data": data}
        except httpx.HTTPStatusError as e:
            status_code = int(e.response.status_code) if e.response is not None else 0
            retriable = status_code in (429, 500, 502, 503, 504)
            if (not retriable) or attempt >= attempts:
                raise
            time.sleep(float(retry_backoff_seconds) * attempt)
        except (httpx.TimeoutException, httpx.RequestError):
            if attempt >= attempts:
                raise
            time.sleep(float(retry_backoff_seconds) * attempt)
    raise RuntimeError("Не удалось выполнить запрос к Wildberries API")


def check_connection(conn: sqlite3.Connection | None = None, api_token: str | None = None) -> dict[str, Any]:
    token = resolve_token(conn, api_token)
    if not token:
        return {"ok": False, "message": "Не задан Wildberries API token"}
    try:
        response = _request("GET", COMMON_BASE_URL, "/api/v1/seller-info", token)
        return {
            "ok": True,
            "message": "Подключение к Wildberries API подтверждено.",
            "seller_name": response.get("name"),
            "seller_sid": response.get("sid"),
            "trade_mark": response.get("tradeMark"),
        }
    except Exception as e:
        return {"ok": False, "message": str(e)}


def list_parent_categories(conn: sqlite3.Connection | None = None, api_token: str | None = None, locale: str = "ru") -> list[dict[str, Any]]:
    token = resolve_token(conn, api_token)
    response = _request("GET", CONTENT_BASE_URL, "/content/v2/object/parent/all", token, params={"locale": locale})
    return list(response.get("data") or [])


def search_subjects(
    conn: sqlite3.Connection | None = None,
    api_token: str | None = None,
    *,
    name: str | None = None,
    parent_id: int | None = None,
    locale: str = "ru",
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    token = resolve_token(conn, api_token)
    params: dict[str, Any] = {
        "locale": locale,
        "limit": max(1, min(1000, int(limit))),
        "offset": max(0, int(offset)),
    }
    if name:
        params["name"] = str(name).strip()
    if parent_id:
        params["parentID"] = int(parent_id)
    response = _request("GET", CONTENT_BASE_URL, "/content/v2/object/all", token, params=params)
    return list(response.get("data") or [])


def get_subject_characteristics(
    conn: sqlite3.Connection | None = None,
    api_token: str | None = None,
    *,
    subject_id: int,
    locale: str = "ru",
) -> list[dict[str, Any]]:
    token = resolve_token(conn, api_token)
    response = _request("GET", CONTENT_BASE_URL, f"/content/v2/object/charcs/{int(subject_id)}", token, params={"locale": locale})
    return list(response.get("data") or [])


def list_failed_cards(
    conn: sqlite3.Connection | None = None,
    api_token: str | None = None,
    *,
    limit: int = 100,
    updated_at: str | None = None,
    batch_uuid: str | None = None,
) -> dict[str, Any]:
    token = resolve_token(conn, api_token)
    cursor: dict[str, Any] = {"limit": max(1, min(100, int(limit)))}
    if updated_at:
        cursor["updatedAt"] = str(updated_at)
    if batch_uuid:
        cursor["batchUUID"] = str(batch_uuid)
    payload = {
        "cursor": cursor,
        "order": {"ascending": True},
    }
    return _request("POST", CONTENT_BASE_URL, "/content/v2/cards/error/list", token, payload=payload)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _to_int_dimension(value: Any, fallback: int = 1) -> int:
    num = _safe_float(value, float(fallback))
    return max(int(round(num)), int(fallback))


def _build_wb_dimensions(product_row: dict[str, Any]) -> dict[str, int]:
    length = product_row.get("package_length") or product_row.get("length") or 1
    width = product_row.get("package_width") or product_row.get("width") or 1
    height = product_row.get("package_height") or product_row.get("height") or 1
    weight = product_row.get("gross_weight") or product_row.get("weight") or 0.1
    return {
        "length": _to_int_dimension(length, 1),
        "width": _to_int_dimension(width, 1),
        "height": _to_int_dimension(height, 1),
        "weightBrutto": max(round(_safe_float(weight, 0.1), 3), 0.001),
    }


def _build_wb_sizes(product_row: dict[str, Any]) -> list[dict[str, Any]]:
    barcode = str(product_row.get("barcode") or product_row.get("article") or product_row.get("internal_article") or "").strip()
    return [
        {
            "techSize": "ONE SIZE",
            "wbSize": "",
            "skus": [barcode] if barcode else [],
        }
    ]


def build_card_draft(
    conn: sqlite3.Connection,
    *,
    product_id: int,
    subject_id: int,
    extra_characteristics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM products WHERE id = ? LIMIT 1", (int(product_id),)).fetchone()
    if not row:
        raise ValueError("Товар не найден")
    product_row = dict(row)
    characteristics = []
    for item in extra_characteristics or []:
        char_id = int(item.get("id") or 0)
        value = item.get("value")
        if char_id <= 0 or value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            prepared_value = value
        else:
            prepared_value = [value]
        characteristics.append({"id": char_id, "value": prepared_value})
    draft = {
        "vendorCode": str(product_row.get("article") or product_row.get("internal_article") or product_row.get("id")),
        "brand": str(product_row.get("brand") or "").strip(),
        "title": str(product_row.get("name") or "").strip(),
        "description": str(product_row.get("description") or "").strip(),
        "dimensions": _build_wb_dimensions(product_row),
        "characteristics": characteristics,
        "sizes": _build_wb_sizes(product_row),
        "subjectID": int(subject_id),
    }
    return draft


def upload_product_cards(
    conn: sqlite3.Connection | None = None,
    api_token: str | None = None,
    *,
    cards: list[dict[str, Any]],
) -> dict[str, Any]:
    token = resolve_token(conn, api_token)
    if not token:
        return {"ok": False, "message": "Не задан Wildberries API token"}
    if not cards:
        return {"ok": False, "message": "Нет карточек для отправки"}
    try:
        response = _request("POST", CONTENT_BASE_URL, "/content/v2/cards/upload", token, payload=cards)
        return {"ok": True, "response": response}
    except Exception as e:
        return {"ok": False, "message": str(e)}

