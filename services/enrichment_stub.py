from __future__ import annotations

import logging
from pathlib import Path
import sqlite3

LOG_PATH = Path("data/enrichment.log")


def enrich_products_stub(conn: sqlite3.Connection) -> int:
    """Заглушка: логирует товары без веса и/или габаритов."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("enrichment_stub")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(handler)

    rows = conn.execute("SELECT article, name, weight, length, width, height FROM products").fetchall()
    count = 0
    for row in rows:
        missing_weight = row["weight"] is None
        missing_dimensions = row["length"] is None or row["width"] is None or row["height"] is None
        if missing_weight or missing_dimensions:
            count += 1
            logger.info("товар требует обогащения данных: article=%s name=%s", row["article"], row["name"])
    return count
