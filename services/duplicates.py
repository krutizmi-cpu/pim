from __future__ import annotations

import json
from difflib import SequenceMatcher

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from models import DuplicateCandidate, Product
from utils.text_normalizer import normalize_text


def _name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def rebuild_duplicate_candidates(session: Session) -> int:
    """Recalculate duplicate table using simple MVP rules."""
    session.query(DuplicateCandidate).delete()
    products = session.query(Product).order_by(Product.id.asc()).all()
    created = 0

    for idx, new_product in enumerate(products):
        for existing in products[:idx]:
            score = 0.0
            matched_by = None
            details: dict[str, str | float] = {}

            new_article = (new_product.article or "").strip().lower()
            old_article = (existing.article or "").strip().lower()

            new_name = normalize_text(new_product.base_name)
            old_name = normalize_text(existing.base_name)

            if new_article and old_article and new_article == old_article:
                score = 1.0
                matched_by = "exact_article"

            elif new_name and old_name and new_name == old_name:
                score = 0.95
                matched_by = "exact_clean_name"

            else:
                sim = _name_similarity(new_name, old_name)
                details["name_similarity"] = round(sim, 4)
                if sim >= 0.86:
                    score = sim
                    matched_by = "similar_name"

                if new_article and old_article and sim >= 0.72 and new_article[:4] == old_article[:4]:
                    score = max(score, sim + 0.1)
                    matched_by = "hybrid_name_article"

            if matched_by and score >= 0.80:
                details.update(
                    {
                        "new_article": new_product.article or "",
                        "existing_article": existing.article or "",
                        "new_name": new_product.base_name,
                        "existing_name": existing.base_name,
                    }
                )
                session.add(
                    DuplicateCandidate(
                        new_product_id=new_product.id,
                        existing_product_id=existing.id,
                        similarity_score=min(round(score, 4), 1.0),
                        matched_by=matched_by,
                        details_json=json.dumps(details, ensure_ascii=False),
                    )
                )
                created += 1

    session.commit()
    return created


def product_has_duplicate(session: Session, product_id: int) -> bool:
    return (
        session.query(DuplicateCandidate)
        .filter(
            or_(
                DuplicateCandidate.new_product_id == product_id,
                DuplicateCandidate.existing_product_id == product_id,
            )
        )
        .first()
        is not None
    )
