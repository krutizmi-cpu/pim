from __future__ import annotations

import logging
from pathlib import Path
import sqlite3


LOG_PATH = Path("data/enrichment.log")


def enrich_products_stub(conn: sqlite3.Connection) -> int:
    """Stub: log products that miss weight or dimensions."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s %(message)s")

    rows = conn.execute(
        """
        SELECT article, name, weight, length, width, height
        FROM products
        """
    ).fetchall()

    need_enrichment = 0
    for row in rows:
        missing_weight = row["weight"] is None
        missing_dimensions = row["length"] is None or row["width"] is None or row["height"] is None
        if missing_weight or missing_dimensions:
            need_enrichment += 1
            logging.info(
                "товар требует обогащения данных: article=%s name=%s",
                row["article"],
                row["name"],
            )

    return need_enrichment
