from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from models import Base, Client

DB_PATH = Path("data/pim.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def init_db() -> None:
    """Create DB file/folders and tables on first launch."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)


def get_session() -> Session:
    return SessionLocal()


def seed_demo_data() -> None:
    """Optional helper to seed a demo client only once."""
    with get_session() as session:
        exists = session.query(Client).filter(Client.name == "Demo клиент").first()
        if not exists:
            session.add(Client(name="Demo клиент", comment="Тестовый клиент"))
            session.commit()
