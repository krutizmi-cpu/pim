from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("data/catalog.db")


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Return sqlite3 connection and ensure parent folder exists."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create base tables for categories and products if they do not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_id INTEGER,
            name TEXT NOT NULL,
            path TEXT NOT NULL UNIQUE,
            FOREIGN KEY(parent_id) REFERENCES categories(id)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article TEXT,
            name TEXT,
            brand TEXT,
            barcode TEXT,
            category_id INTEGER,
            weight REAL,
            length REAL,
            width REAL,
            height REAL,
            description TEXT,
            FOREIGN KEY(category_id) REFERENCES categories(id)
        )
        """
    )
    conn.commit()


def get_or_create_category(conn: sqlite3.Connection, category_path: str | None) -> int | None:
    """Get category id by full path or create nested categories from path."""
    if not category_path:
        return None

    normalized_path = str(category_path).strip()
    if not normalized_path:
        return None

    existing = conn.execute(
        "SELECT id FROM categories WHERE path = ?",
        (normalized_path,),
    ).fetchone()
    if existing:
        return int(existing["id"])

    parts = [part.strip() for part in normalized_path.split(">") if part.strip()]
    if not parts:
        parts = [normalized_path]

    parent_id: int | None = None
    current_parts: list[str] = []

    for part in parts:
        current_parts.append(part)
        current_path = " > ".join(current_parts)

        row = conn.execute(
            "SELECT id FROM categories WHERE path = ?",
            (current_path,),
        ).fetchone()

        if row:
            parent_id = int(row["id"])
            continue

        cursor = conn.execute(
            "INSERT INTO categories (parent_id, name, path) VALUES (?, ?, ?)",
            (parent_id, part, current_path),
        )
        parent_id = int(cursor.lastrowid)

    conn.commit()
    return parent_id


def count_rows(conn: sqlite3.Connection, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) AS c FROM {table_name}").fetchone()
    return int(row["c"]) if row else 0
