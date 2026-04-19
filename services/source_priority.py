from __future__ import annotations

import sqlite3

from services.source_tracking import get_latest_field_source


SOURCE_PRIORITY = {
    "manual": 100,
    "client_validated": 90,
    "supplier_page": 70,
    "ozon_search_fallback": 62,
    "yandex_search_fallback": 60,
    "web_search_fallback": 58,
    "ai": 50,
    "name_category_inference": 40,
    "category_stats_fallback": 35,
    "category_defaults_fallback": 30,
    "default": 10,
}


def get_source_priority(source_type: str | None) -> int:
    if not source_type:
        return 0
    return SOURCE_PRIORITY.get(str(source_type), 0)


def can_overwrite_field(
    conn: sqlite3.Connection,
    product_id: int,
    field_name: str,
    incoming_source_type: str,
    force: bool = False,
) -> bool:
    if force:
        return True

    latest = get_latest_field_source(conn, product_id, field_name)
    if not latest:
        return True

    current_source = latest.get("source_type")
    current_priority = get_source_priority(current_source)
    incoming_priority = get_source_priority(incoming_source_type)

    return incoming_priority >= current_priority
