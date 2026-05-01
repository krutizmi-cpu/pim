from __future__ import annotations

import base64
import json
import os
import re
import sqlite3
from typing import Any
from urllib.parse import quote

import httpx

from services.attribute_service import set_product_attribute_value
from services.source_priority import can_overwrite_field
from services.source_tracking import save_field_source


AI_SETTINGS_DEFAULTS: dict[str, Any] = {
    "enabled": True,
    "provider": "openai",
    "base_url": "",
    "chat_model": "",
    "image_model": "",
    "api_key": "",
    "use_env_api_key": True,
    "temperature": 0.3,
    "max_tokens": 1800,
    "image_size": "1024x1024",
    "openrouter_referer": "",
    "openrouter_title": "pim",
}

PROVIDER_DEFAULTS: dict[str, dict[str, str]] = {
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "chat_model": "gpt-4o-mini",
        "image_model": "gpt-image-1",
    },
    "openrouter": {
        "base_url": "https://openrouter.ai/api/v1",
        "chat_model": "openai/gpt-4o-mini",
        "image_model": "openai/gpt-image-1",
    },
    "nvidia": {
        "base_url": "https://integrate.api.nvidia.com/v1",
        "chat_model": "meta/llama-3.1-70b-instruct",
        "image_model": "",
    },
}


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


def _get_setting(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    _ensure_system_settings_table(conn)
    row = conn.execute("SELECT value FROM system_settings WHERE key = ? LIMIT 1", (str(key),)).fetchone()
    if not row:
        return default
    return row["value"]


def _set_setting(conn: sqlite3.Connection, key: str, value: Any) -> None:
    _ensure_system_settings_table(conn)
    conn.execute(
        """
        INSERT INTO system_settings (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = CURRENT_TIMESTAMP
        """,
        (str(key), str(value) if value is not None else None),
    )


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _resolve_provider_defaults(provider: str) -> dict[str, str]:
    key = str(provider or "openai").strip().lower()
    return PROVIDER_DEFAULTS.get(key, PROVIDER_DEFAULTS["openai"])


def load_ai_settings(conn: sqlite3.Connection) -> dict[str, Any]:
    provider = str(_get_setting(conn, "ai.provider", AI_SETTINGS_DEFAULTS["provider"]) or "openai").strip().lower()
    defaults = _resolve_provider_defaults(provider)
    settings = {
        "enabled": _to_bool(_get_setting(conn, "ai.enabled", AI_SETTINGS_DEFAULTS["enabled"]), True),
        "provider": provider,
        "base_url": str(_get_setting(conn, "ai.base_url", defaults["base_url"]) or defaults["base_url"]).strip(),
        "chat_model": str(_get_setting(conn, "ai.chat_model", defaults["chat_model"]) or defaults["chat_model"]).strip(),
        "image_model": str(_get_setting(conn, "ai.image_model", defaults["image_model"]) or defaults["image_model"]).strip(),
        "api_key": str(_get_setting(conn, "ai.api_key", AI_SETTINGS_DEFAULTS["api_key"]) or "").strip(),
        "use_env_api_key": _to_bool(_get_setting(conn, "ai.use_env_api_key", AI_SETTINGS_DEFAULTS["use_env_api_key"]), True),
        "temperature": float(_get_setting(conn, "ai.temperature", AI_SETTINGS_DEFAULTS["temperature"]) or AI_SETTINGS_DEFAULTS["temperature"]),
        "max_tokens": int(float(_get_setting(conn, "ai.max_tokens", AI_SETTINGS_DEFAULTS["max_tokens"]) or AI_SETTINGS_DEFAULTS["max_tokens"])),
        "image_size": str(_get_setting(conn, "ai.image_size", AI_SETTINGS_DEFAULTS["image_size"]) or AI_SETTINGS_DEFAULTS["image_size"]).strip(),
        "openrouter_referer": str(_get_setting(conn, "ai.openrouter_referer", AI_SETTINGS_DEFAULTS["openrouter_referer"]) or "").strip(),
        "openrouter_title": str(_get_setting(conn, "ai.openrouter_title", AI_SETTINGS_DEFAULTS["openrouter_title"]) or "pim").strip(),
    }
    settings["temperature"] = max(0.0, min(1.5, float(settings["temperature"])))
    settings["max_tokens"] = max(256, min(65536, int(settings["max_tokens"])))
    if settings["image_size"] not in {"1024x1024", "1536x1024", "1024x1536"}:
        settings["image_size"] = "1024x1024"
    return settings


def save_ai_settings(conn: sqlite3.Connection, settings: dict[str, Any]) -> None:
    provider = str(settings.get("provider") or "openai").strip().lower()
    defaults = _resolve_provider_defaults(provider)
    _set_setting(conn, "ai.enabled", 1 if bool(settings.get("enabled", True)) else 0)
    _set_setting(conn, "ai.provider", provider)
    _set_setting(conn, "ai.base_url", str(settings.get("base_url") or defaults["base_url"]).strip())
    _set_setting(conn, "ai.chat_model", str(settings.get("chat_model") or defaults["chat_model"]).strip())
    _set_setting(conn, "ai.image_model", str(settings.get("image_model") or defaults["image_model"]).strip())
    _set_setting(conn, "ai.api_key", str(settings.get("api_key") or "").strip())
    _set_setting(conn, "ai.use_env_api_key", 1 if bool(settings.get("use_env_api_key", True)) else 0)
    _set_setting(conn, "ai.temperature", float(settings.get("temperature", 0.3)))
    _set_setting(conn, "ai.max_tokens", int(settings.get("max_tokens", 1800)))
    _set_setting(conn, "ai.image_size", str(settings.get("image_size") or "1024x1024").strip())
    _set_setting(conn, "ai.openrouter_referer", str(settings.get("openrouter_referer") or "").strip())
    _set_setting(conn, "ai.openrouter_title", str(settings.get("openrouter_title") or "pim").strip())
    conn.commit()


def _resolve_api_key(settings: dict[str, Any]) -> str:
    explicit = str(settings.get("api_key") or "").strip()
    if explicit:
        return explicit
    if not bool(settings.get("use_env_api_key", True)):
        return ""
    provider = str(settings.get("provider") or "").strip().lower()
    env_candidates = ["AI_API_KEY"]
    if provider == "openai":
        env_candidates = ["OPENAI_API_KEY"] + env_candidates
    elif provider == "openrouter":
        env_candidates = ["OPENROUTER_API_KEY"] + env_candidates
    elif provider == "nvidia":
        env_candidates = ["NVIDIA_API_KEY"] + env_candidates
    for key in env_candidates:
        val = str(os.getenv(key) or "").strip()
        if val:
            return val
    return ""


def _build_request_headers(settings: dict[str, Any]) -> dict[str, str]:
    api_key = _resolve_api_key(settings)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    provider = str(settings.get("provider") or "").strip().lower()
    if provider == "openrouter":
        referer = str(settings.get("openrouter_referer") or "").strip() or os.getenv("OPENROUTER_SITE_URL", "").strip()
        title = str(settings.get("openrouter_title") or "").strip() or os.getenv("OPENROUTER_APP_NAME", "").strip() or "pim"
        if referer:
            headers["HTTP-Referer"] = referer
        if title:
            headers["X-Title"] = title
    return headers


def _build_http_timeout(provider: str, *, for_chat: bool) -> httpx.Timeout:
    key = str(provider or "").strip().lower()
    if for_chat:
        if key == "nvidia":
            return httpx.Timeout(connect=20.0, read=180.0, write=45.0, pool=45.0)
        if key == "openrouter":
            return httpx.Timeout(connect=20.0, read=120.0, write=40.0, pool=40.0)
        return httpx.Timeout(connect=15.0, read=90.0, write=30.0, pool=30.0)
    return httpx.Timeout(connect=12.0, read=30.0, write=20.0, pool=20.0)


def _check_model_endpoint(settings: dict[str, Any]) -> dict[str, Any]:
    ok, reason = ai_is_configured(settings)
    if not ok:
        return {"ok": False, "error": reason}
    base_url = str(settings.get("base_url") or "").rstrip("/")
    provider = str(settings.get("provider") or "").strip().lower()
    model = str(settings.get("chat_model") or "").strip()
    headers = _build_request_headers(settings)
    encoded_model = quote(model, safe="/:-_")
    try:
        with httpx.Client(timeout=_build_http_timeout(provider, for_chat=False)) as client:
            response = client.get(f"{base_url}/models/{encoded_model}", headers=headers)
            if response.status_code == 404:
                list_response = client.get(f"{base_url}/models", headers=headers)
                list_response.raise_for_status()
                data = list_response.json()
                models = data.get("data") if isinstance(data, dict) else None
                model_ids = {
                    str(item.get("id") or "").strip()
                    for item in (models or [])
                    if isinstance(item, dict)
                }
                if model in model_ids:
                    return {
                        "ok": True,
                        "provider": provider,
                        "model": model,
                        "mode": "models_list",
                        "text": f"Модель `{model}` найдена в каталоге провайдера.",
                    }
                return {
                    "ok": False,
                    "provider": provider,
                    "model": model,
                    "error": f"Провайдер отвечает, но модель `{model}` не найдена в `/models`.",
                }
            response.raise_for_status()
            return {
                "ok": True,
                "provider": provider,
                "model": model,
                "mode": "model_endpoint",
                "text": f"Провайдер отвечает, модель `{model}` доступна.",
            }
    except Exception as e:
        return {"ok": False, "provider": provider, "model": model, "error": str(e)}


def ai_is_configured(settings: dict[str, Any]) -> tuple[bool, str]:
    if not bool(settings.get("enabled", True)):
        return False, "AI отключен в настройках."
    if not str(settings.get("base_url") or "").strip():
        return False, "Не задан base_url."
    if not str(settings.get("chat_model") or "").strip():
        return False, "Не задан chat_model."
    if not _resolve_api_key(settings):
        return False, "Не задан API key (в настройках или env)."
    return True, "AI настроен."


def _extract_message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
        return "\n".join(parts)
    if content is None:
        return ""
    return str(content)


def _chat_completion(
    settings: dict[str, Any],
    system_prompt: str,
    user_prompt: str,
    temperature: float | None = None,
    max_tokens: int | None = None,
    force_json: bool = False,
) -> dict[str, Any]:
    ok, reason = ai_is_configured(settings)
    if not ok:
        return {"ok": False, "error": reason}

    base_url = str(settings.get("base_url") or "").rstrip("/")
    model = str(settings.get("chat_model") or "").strip()
    provider = str(settings.get("provider") or "").strip().lower()

    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": str(system_prompt)},
            {"role": "user", "content": str(user_prompt)},
        ],
        "temperature": float(temperature if temperature is not None else settings.get("temperature", 0.3)),
        "max_tokens": int(max_tokens if max_tokens is not None else settings.get("max_tokens", 1800)),
    }
    if force_json:
        payload["response_format"] = {"type": "json_object"}

    headers = _build_request_headers(settings)
    try:
        with httpx.Client(timeout=_build_http_timeout(provider, for_chat=True)) as client:
            response = client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
        choices = data.get("choices") or []
        if not choices:
            return {"ok": False, "error": "AI не вернул choices."}
        message = choices[0].get("message") or {}
        text = _extract_message_text(message.get("content")).strip()
        if not text:
            return {"ok": False, "error": "AI вернул пустой ответ."}
        usage = data.get("usage") or {}
        return {
            "ok": True,
            "text": text,
            "model": model,
            "provider": provider,
            "usage": usage,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_ai_connection(settings: dict[str, Any]) -> dict[str, Any]:
    model_check = _check_model_endpoint(settings)
    if not model_check.get("ok"):
        return model_check
    provider = str(settings.get("provider") or "").strip().lower()
    if provider == "nvidia":
        return {
            **model_check,
            "text": f"{model_check.get('text')} Для NVIDIA это быстрая проверка доступа к модели без ожидания долгой генерации.",
        }
    chat_check = _chat_completion(
        settings=settings,
        system_prompt="Ты сервис проверки соединения.",
        user_prompt="Ответь ровно: OK",
        temperature=0.0,
        max_tokens=12,
    )
    if chat_check.get("ok"):
        return chat_check
    return {
        "ok": True,
        "provider": provider,
        "model": str(settings.get("chat_model") or "").strip(),
        "mode": "model_endpoint_only",
        "text": (
            f"{model_check.get('text')} Короткий chat-ping не завершился быстро: "
            f"{chat_check.get('error') or 'без текста ошибки'}"
        ),
        "warning": chat_check.get("error"),
    }


def _product_row(conn: sqlite3.Connection, product_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM products WHERE id = ?", (int(product_id),)).fetchone()
    return dict(row) if row else {}


def _collect_product_attributes(conn: sqlite3.Connection, product_id: int, limit: int = 120) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            pav.id,
            pav.attribute_code,
            pav.value_text,
            pav.value_number,
            pav.value_boolean,
            pav.value_json,
            ad.name AS attribute_name
        FROM product_attribute_values pav
        LEFT JOIN attribute_definitions ad ON ad.code = pav.attribute_code
        WHERE pav.product_id = ?
        ORDER BY pav.id DESC
        """,
        (int(product_id),),
    ).fetchall()
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        code = str(row["attribute_code"] or "").strip()
        if not code or code in seen:
            continue
        value = row["value_number"]
        if value is None:
            value = row["value_boolean"]
        if value is None:
            value = row["value_json"]
        if value is None:
            value = row["value_text"]
        text_value = _clean_text(value)
        if not text_value:
            continue
        seen.add(code)
        items.append(
            {
                "attribute_code": code,
                "attribute_name": str(row["attribute_name"] or code),
                "value": text_value,
            }
        )
        if len(items) >= int(limit):
            break
    return items


def _collect_missing_ozon_attributes(conn: sqlite3.Connection, product_id: int, limit: int = 20) -> list[dict[str, Any]]:
    product = _product_row(conn, int(product_id))
    desc_id = int(product.get("ozon_description_category_id") or 0)
    type_id = int(product.get("ozon_type_id") or 0)
    if desc_id <= 0 or type_id <= 0:
        return []
    category_code = f"ozon:{desc_id}:{type_id}"
    rows = conn.execute(
        """
        SELECT
            r.attribute_code,
            IFNULL(ad.name, r.attribute_code) AS attribute_name,
            IFNULL(r.is_required, 0) AS is_required
        FROM channel_attribute_requirements r
        LEFT JOIN attribute_definitions ad ON ad.code = r.attribute_code
        WHERE r.channel_code = 'ozon'
          AND r.category_code = ?
        ORDER BY IFNULL(r.is_required, 0) DESC, IFNULL(r.sort_order, 100), r.attribute_code
        """,
        (category_code,),
    ).fetchall()
    if not rows:
        return []
    value_rows = conn.execute(
        """
        SELECT attribute_code, value_text, value_number, value_boolean, value_json
        FROM product_attribute_values
        WHERE product_id = ?
        ORDER BY id DESC
        """,
        (int(product_id),),
    ).fetchall()
    filled_codes: set[str] = set()
    for row in value_rows:
        code = str(row["attribute_code"] or "").strip()
        if not code:
            continue
        raw = row["value_text"]
        if raw is None:
            raw = row["value_number"]
        if raw is None:
            raw = row["value_boolean"]
        if raw is None:
            raw = row["value_json"]
        if _clean_text(raw):
            filled_codes.add(code)

    missing: list[dict[str, Any]] = []
    for row in rows:
        code = str(row["attribute_code"] or "").strip()
        if not code or code in filled_codes:
            continue
        missing.append(
            {
                "attribute_code": code,
                "attribute_name": str(row["attribute_name"] or code),
                "is_required": int(row["is_required"] or 0),
            }
        )
        if len(missing) >= int(limit):
            break
    return missing


def _attributes_for_prompt(attr_rows: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for row in attr_rows:
        name = _clean_text(row.get("attribute_name") or row.get("attribute_code"))
        val = _clean_text(row.get("value"))
        if not name or not val:
            continue
        lines.append(f"- {name}: {val}")
    return "\n".join(lines) if lines else "- Нет заполненных атрибутов"


def generate_seo_description_for_product(
    conn: sqlite3.Connection,
    product_id: int,
    settings: dict[str, Any],
) -> dict[str, Any]:
    product = _product_row(conn, int(product_id))
    if not product:
        return {"ok": False, "error": "Товар не найден."}
    attr_rows = _collect_product_attributes(conn, int(product_id), limit=80)
    attrs_block = _attributes_for_prompt(attr_rows)
    product_name = _clean_text(product.get("name"))
    article = _clean_text(product.get("article") or product.get("supplier_article"))
    category_path = _clean_text(product.get("ozon_category_path") or product.get("category"))
    brand = _clean_text(product.get("brand"))

    system_prompt = (
        "Ты — сильный e-commerce копирайтер и редактор карточек товара для маркетплейсов и интернет-магазинов. "
        "Пиши только готовый текст на русском в Markdown. Не выдумывай характеристики и не добавляй факты, которых нет во входных данных."
    )
    user_prompt = f"""
Напиши описание товара для карточки интернет-магазина, используя входные данные:
- Название: {product_name or "-"}
- Артикул: {article or "-"}
- Бренд: {brand or "-"}
- Категория: {category_path or "-"}
- Атрибуты/характеристики:
{attrs_block}

Требования:
1) Структура:
- Вводный абзац (1-3 предложения): что это за товар, для кого он подходит и какую задачу закрывает.
- Ключевые выгоды: 3-5 пунктов в формате «Характеристика -> Польза».
- Технические параметры: маркированный список; артикул укажи один раз в этом блоке.
- Сценарии использования / для кого подходит: 2-4 предложения, с ориентацией на целевую аудиторию.
- Финал: спокойный CTA без давления.
2) SEO:
- Выбери 1 основное ключевое слово и 2-3 смежных.
- Основное: в H2 и первом абзаце.
- Смежные: в подзаголовках или списке выгод.
- Добавь блоки Meta Title (до 60 символов) и Meta Description (до 160 символов).
3) Тон:
- Профессиональный, честный, без «маркетинговой пенки».
- Запрещены слова: лучший, уникальный, революционный, хит продаж, невероятно, идеальный.
- Если данных нет — не придумывай.
4) Длина: 300-550 слов.
5) Формат:
- Только готовый Markdown.
- Сразу начинай с заголовка H2.
"""
    result = _chat_completion(
        settings=settings,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=float(settings.get("temperature", 0.3)),
        max_tokens=int(settings.get("max_tokens", 1800)),
    )
    if not result.get("ok"):
        return result
    return {
        "ok": True,
        "text": str(result.get("text") or "").strip(),
        "provider": result.get("provider"),
        "model": result.get("model"),
        "usage": result.get("usage"),
        "attr_count": len(attr_rows),
    }


def generate_selling_title_for_product(
    conn: sqlite3.Connection,
    product_id: int,
    settings: dict[str, Any],
) -> dict[str, Any]:
    product = _product_row(conn, int(product_id))
    if not product:
        return {"ok": False, "error": "Товар не найден."}
    attr_rows = _collect_product_attributes(conn, int(product_id), limit=60)
    attrs_block = _attributes_for_prompt(attr_rows[:14])
    current_name = _clean_text(product.get("name"))
    article = _clean_text(product.get("article") or product.get("supplier_article"))
    brand = _clean_text(product.get("brand"))
    category_path = _clean_text(product.get("ozon_category_path") or product.get("category"))

    system_prompt = (
        "Ты редактор товарных названий для маркетплейсов. "
        "Твоя задача — собирать короткое, продающее и честное название товара по входным данным. "
        "Не выдумывай характеристики. Отвечай только JSON."
    )
    user_prompt = f"""
Собери 1 итоговое название товара для карточки.

Входные данные:
- Текущее название: {current_name or "-"}
- Артикул: {article or "-"}
- Бренд: {brand or "-"}
- Категория: {category_path or "-"}
- Атрибуты:
{attrs_block}

Требования к названию:
- Русский язык.
- 70-140 символов.
- Формат: тип товара + бренд/модель + ключевые характеристики + целевая аудитория/применение, если это реально следует из данных.
- Не дублируй артикул больше одного раза.
- Не пиши пустой маркетинг вроде «лучший», «супер», «топ».
- Не вставляй характеристики, которых нет во входных данных.
- Не делай название слишком техническим или слишком коротким.

Верни JSON:
{{
  "title": "готовое название",
  "reason": "кратко, какие данные использованы"
}}
"""
    result = _chat_completion(
        settings=settings,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=min(0.45, float(settings.get("temperature", 0.3))),
        max_tokens=min(1200, int(settings.get("max_tokens", 1800))),
        force_json=True,
    )
    if not result.get("ok"):
        return result

    text = str(result.get("text") or "").strip()
    try:
        parsed = json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return {"ok": False, "error": "AI вернул невалидный JSON для названия."}
        try:
            parsed = json.loads(match.group(0))
        except Exception:
            return {"ok": False, "error": "AI вернул невалидный JSON для названия."}
    title = _clean_text(parsed.get("title")) if isinstance(parsed, dict) else ""
    if not title:
        return {"ok": False, "error": "AI не вернул итоговое название."}
    reason = _clean_text(parsed.get("reason")) if isinstance(parsed, dict) else ""
    return {
        "ok": True,
        "title": title,
        "reason": reason,
        "provider": result.get("provider"),
        "model": result.get("model"),
        "usage": result.get("usage"),
    }


def generate_ai_attribute_suggestions_for_product(
    conn: sqlite3.Connection,
    product_id: int,
    settings: dict[str, Any],
    limit: int = 20,
) -> dict[str, Any]:
    product = _product_row(conn, int(product_id))
    if not product:
        return {"ok": False, "error": "Товар не найден."}

    missing = _collect_missing_ozon_attributes(conn, int(product_id), limit=int(limit))
    if not missing:
        return {"ok": False, "error": "Нет пустых Ozon-атрибутов для AI-подсказки."}

    existing_attrs = _collect_product_attributes(conn, int(product_id), limit=80)
    attrs_block = _attributes_for_prompt(existing_attrs)
    missing_block = "\n".join(
        [f"- {m['attribute_code']} | {m['attribute_name']} | required={int(m.get('is_required') or 0)}" for m in missing]
    )
    product_name = _clean_text(product.get("name"))
    article = _clean_text(product.get("article") or product.get("supplier_article"))
    category_path = _clean_text(product.get("ozon_category_path") or product.get("category"))

    system_prompt = (
        "Ты специалист по структурированию карточек для маркетплейсов. "
        "Нужно предложить вероятные значения недостающих атрибутов. "
        "Отвечай только JSON-объектом."
    )
    user_prompt = f"""
Товар:
- Название: {product_name or "-"}
- Артикул: {article or "-"}
- Категория Ozon: {category_path or "-"}

Уже заполненные атрибуты:
{attrs_block}

Нужно предложить значения для пустых атрибутов:
{missing_block}

Верни JSON формата:
{{
  "suggestions": [
    {{
      "attribute_code": "код",
      "value": "значение",
      "confidence": 0.0-1.0,
      "reason": "кратко"
    }}
  ]
}}

Правила:
- Не выдумывай точные цифры, если нет опоры.
- Для неуверенных значений confidence <= 0.45.
- Для уверенных значений confidence >= 0.7.
- Не включай атрибуты вне списка.
"""
    result = _chat_completion(
        settings=settings,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=min(0.5, float(settings.get("temperature", 0.3))),
        max_tokens=min(2600, int(settings.get("max_tokens", 1800))),
        force_json=True,
    )
    if not result.get("ok"):
        return result

    text = str(result.get("text") or "").strip()
    try:
        parsed = json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return {"ok": False, "error": "AI вернул невалидный JSON для атрибутов."}
        try:
            parsed = json.loads(match.group(0))
        except Exception:
            return {"ok": False, "error": "AI вернул невалидный JSON для атрибутов."}

    suggestions_raw = parsed.get("suggestions") if isinstance(parsed, dict) else None
    if not isinstance(suggestions_raw, list):
        return {"ok": False, "error": "AI не вернул список suggestions."}

    missing_map = {str(m["attribute_code"]): m for m in missing}
    suggestions: list[dict[str, Any]] = []
    for item in suggestions_raw:
        if not isinstance(item, dict):
            continue
        code = str(item.get("attribute_code") or "").strip()
        if not code or code not in missing_map:
            continue
        value = _clean_text(item.get("value"))
        if not value:
            continue
        conf_raw = item.get("confidence")
        try:
            confidence = float(conf_raw)
        except Exception:
            confidence = 0.5
        confidence = max(0.0, min(1.0, confidence))
        suggestions.append(
            {
                "attribute_code": code,
                "attribute_name": str(missing_map[code]["attribute_name"]),
                "value": value,
                "confidence": confidence,
                "reason": _clean_text(item.get("reason")),
                "is_required": int(missing_map[code].get("is_required") or 0),
            }
        )

    return {
        "ok": True,
        "suggestions": suggestions,
        "missing_total": len(missing),
        "provider": result.get("provider"),
        "model": result.get("model"),
        "usage": result.get("usage"),
    }


def apply_ai_attribute_suggestions(
    conn: sqlite3.Connection,
    product_id: int,
    suggestions: list[dict[str, Any]],
    channel_code: str | None = None,
    source_url: str | None = None,
) -> dict[str, Any]:
    saved = 0
    skipped = 0
    errors = 0
    for item in suggestions or []:
        code = str(item.get("attribute_code") or "").strip()
        value = _clean_text(item.get("value"))
        if not code or not value:
            continue
        field_name = f"attr:{code}"
        if not can_overwrite_field(conn, int(product_id), field_name, "ai", force=False):
            skipped += 1
            continue
        try:
            set_product_attribute_value(
                conn=conn,
                product_id=int(product_id),
                attribute_code=code,
                value=value,
                channel_code=channel_code,
            )
            save_field_source(
                conn=conn,
                product_id=int(product_id),
                field_name=field_name,
                source_type="ai",
                source_value_raw=value,
                source_url=source_url,
                confidence=float(item.get("confidence") or 0.5),
                is_manual=False,
            )
            saved += 1
        except Exception:
            errors += 1
    conn.commit()
    return {"saved": saved, "skipped": skipped, "errors": errors}


def run_ai_enrichment_for_product(
    conn: sqlite3.Connection,
    product_id: int,
    settings: dict[str, Any],
    *,
    include_title: bool = True,
    include_description: bool = True,
    include_attributes: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    product = _product_row(conn, int(product_id))
    if not product:
        return {"ok": False, "error": "Товар не найден."}

    result: dict[str, Any] = {
        "ok": True,
        "title_applied": False,
        "description_applied": False,
        "attributes_saved": 0,
        "attributes_skipped": 0,
        "title_candidate": "",
        "description_candidate": "",
        "errors": [],
    }

    ai_source_label = f"{str(settings.get('provider') or '-')}/{str(settings.get('chat_model') or '-')}"

    if include_title:
        title_res = generate_selling_title_for_product(conn, int(product_id), settings)
        if title_res.get("ok"):
            title_candidate = _clean_text(title_res.get("title"))
            result["title_candidate"] = title_candidate
            if title_candidate and can_overwrite_field(conn, int(product_id), "name", "ai", force=bool(force)):
                conn.execute(
                    """
                    UPDATE products
                    SET name = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (title_candidate, int(product_id)),
                )
                save_field_source(
                    conn=conn,
                    product_id=int(product_id),
                    field_name="name",
                    source_type="ai",
                    source_value_raw=title_candidate,
                    source_url=ai_source_label,
                    confidence=0.72,
                    is_manual=False,
                )
                conn.commit()
                result["title_applied"] = True
        else:
            result["errors"].append(f"title: {title_res.get('error')}")

    if include_description:
        desc_res = generate_seo_description_for_product(conn, int(product_id), settings)
        if desc_res.get("ok"):
            description_candidate = str(desc_res.get("text") or "").strip()
            result["description_candidate"] = description_candidate
            if description_candidate and can_overwrite_field(conn, int(product_id), "description", "ai", force=bool(force)):
                conn.execute(
                    """
                    UPDATE products
                    SET description = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (description_candidate, int(product_id)),
                )
                save_field_source(
                    conn=conn,
                    product_id=int(product_id),
                    field_name="description",
                    source_type="ai",
                    source_value_raw=description_candidate,
                    source_url=ai_source_label,
                    confidence=0.7,
                    is_manual=False,
                )
                conn.commit()
                result["description_applied"] = True
        else:
            result["errors"].append(f"description: {desc_res.get('error')}")

    if include_attributes:
        attr_res = generate_ai_attribute_suggestions_for_product(conn, int(product_id), settings, limit=20)
        if attr_res.get("ok"):
            apply_res = apply_ai_attribute_suggestions(
                conn=conn,
                product_id=int(product_id),
                suggestions=attr_res.get("suggestions") or [],
                channel_code=None,
                source_url=ai_source_label,
            )
            result["attributes_saved"] = int(apply_res.get("saved") or 0)
            result["attributes_skipped"] = int(apply_res.get("skipped") or 0)
        else:
            result["errors"].append(f"attributes: {attr_res.get('error')}")

    return result


def build_marketing_image_prompts_for_product(conn: sqlite3.Connection, product_id: int) -> dict[str, str]:
    product = _product_row(conn, int(product_id))
    if not product:
        return {"context_prompt": "", "color_prompt": ""}

    attrs = _collect_product_attributes(conn, int(product_id), limit=12)
    attrs_short = "; ".join([f"{a['attribute_name']}: {a['value']}" for a in attrs[:8]])
    name = _clean_text(product.get("name"))
    article = _clean_text(product.get("article") or product.get("supplier_article"))
    description = _clean_text(product.get("description"))
    category = _clean_text(product.get("ozon_category_path") or product.get("category"))
    main_image = _clean_text(product.get("image_url"))

    common = (
        f"Товар: {name or '-'}; Артикул: {article or '-'}; Категория: {category or '-'}; "
        f"Описание: {description[:400] or '-'}; Атрибуты: {attrs_short or '-'}; "
        f"Референс фото: {main_image or '-'}."
    )
    context_prompt = (
        "Ты AI-дизайнер e-commerce и маркетплейс-инфографики. "
        "Сгенерируй квадратное коммерческое фото 1:1 с реалистичным контекстным фоном. "
        "Товар должен быть главным объектом, без людей, без лишнего шума, без искажения формы товара. "
        "Добавь 3-5 инфографических выносок с опорой только на реальные свойства товара: короткие фразы до 5 слов, читабельные, без фейковых обещаний. "
        "Сделай результат пригодным для карточки маркетплейса."
        + common
    )
    color_prompt = (
        "Ты AI-дизайнер e-commerce и маркетплейс-инфографики. "
        "Сгенерируй квадратное коммерческое фото 1:1 на чистом однотонном или мягком градиентном фоне под категорию товара. "
        "Товар в фокусе, аккуратный свет, без людей, без лишних декоративных объектов. "
        "Добавь 3-5 инфографических выносок только по реальным свойствам товара, в стиле карточки маркетплейса."
        + common
    )
    return {"context_prompt": context_prompt, "color_prompt": color_prompt}


def generate_images_from_prompts(
    settings: dict[str, Any],
    prompts: list[str],
    size: str | None = None,
) -> list[dict[str, Any]]:
    ok, reason = ai_is_configured(settings)
    if not ok:
        return [{"ok": False, "error": reason}]
    base_url = str(settings.get("base_url") or "").rstrip("/")
    image_model = str(settings.get("image_model") or "").strip()
    if not image_model:
        return [{"ok": False, "error": "Не задан image_model в AI-настройках."}]

    api_key = _resolve_api_key(settings)
    provider = str(settings.get("provider") or "").strip().lower()
    image_size = str(size or settings.get("image_size") or "1024x1024")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if provider == "openrouter":
        referer = str(settings.get("openrouter_referer") or "").strip() or os.getenv("OPENROUTER_SITE_URL", "").strip()
        title = str(settings.get("openrouter_title") or "").strip() or os.getenv("OPENROUTER_APP_NAME", "").strip() or "pim"
        if referer:
            headers["HTTP-Referer"] = referer
        if title:
            headers["X-Title"] = title

    results: list[dict[str, Any]] = []
    for prompt in prompts:
        prompt_text = str(prompt or "").strip()
        if not prompt_text:
            continue
        payload = {
            "model": image_model,
            "prompt": prompt_text,
            "size": image_size,
            "n": 1,
        }
        try:
            with httpx.Client(timeout=120.0) as client:
                response = client.post(f"{base_url}/images/generations", headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
            items = data.get("data") or []
            if not items:
                results.append({"ok": False, "prompt": prompt_text, "error": "Пустой ответ image API"})
                continue
            first = items[0] or {}
            image_url = first.get("url")
            b64 = first.get("b64_json")
            image_bytes = None
            if b64:
                try:
                    image_bytes = base64.b64decode(str(b64))
                except Exception:
                    image_bytes = None
            if not image_url and not image_bytes:
                results.append({"ok": False, "prompt": prompt_text, "error": "В ответе нет url/b64 изображения"})
                continue
            results.append(
                {
                    "ok": True,
                    "prompt": prompt_text,
                    "image_url": image_url,
                    "image_bytes": image_bytes,
                    "provider": provider,
                    "model": image_model,
                }
            )
        except Exception as e:
            results.append({"ok": False, "prompt": prompt_text, "error": str(e)})
    return results
