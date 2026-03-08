from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import requests

OZON_API_URL = "https://api-seller.ozon.ru/v2/product/list"
OUTPUT_PATH = Path("data/ozon_products_raw.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Тестовый запрос к Ozon Seller API")
    parser.add_argument("--client-id", default=os.getenv("OZON_CLIENT_ID"), help="Ozon Client ID")
    parser.add_argument("--api-key", default=os.getenv("OZON_API_KEY"), help="Ozon API Key")
    parser.add_argument("--limit", type=int, default=100, help="Лимит товаров в запросе")
    return parser.parse_args()


def extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result = payload.get("result")
    if isinstance(result, dict):
        items = result.get("items", [])
        return items if isinstance(items, list) else []
    return []


def main() -> None:
    args = parse_args()

    if not args.client_id or not args.api_key:
        raise SystemExit(
            "Нужно передать OZON_CLIENT_ID и OZON_API_KEY (через аргументы или переменные окружения)."
        )

    headers = {
        "Client-Id": str(args.client_id),
        "Api-Key": str(args.api_key),
        "Content-Type": "application/json",
    }
    body = {
        "filter": {"visibility": "ALL"},
        "last_id": "",
        "limit": int(args.limit),
    }

    response = requests.post(OZON_API_URL, headers=headers, json=body, timeout=30)
    response.raise_for_status()
    payload = response.json()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    items = extract_items(payload)
    sample = items[0] if items else {}
    main_fields = sorted(sample.keys()) if sample else []

    print(f"Сохранено: {OUTPUT_PATH}")
    print(f"Количество товаров: {len(items)}")
    print("Основные поля товара:")
    print(main_fields)
    print("Пример одного товара:")
    print(json.dumps(sample, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
