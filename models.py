from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""


class Client(Base):
    __tablename__ = "clients"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)

    products: Mapped[list[Product]] = relationship(back_populates="client")
    categories: Mapped[list[Category]] = relationship(back_populates="client")


class Category(Base):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    parent_id: Mapped[int | None] = mapped_column(ForeignKey("categories.id"), nullable=True)
    source_type: Mapped[str] = mapped_column(String(32), default="custom")
    client_id: Mapped[int | None] = mapped_column(ForeignKey("clients.id"), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    external_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    parent: Mapped[Category | None] = relationship(remote_side=[id], backref="children")
    client: Mapped[Client | None] = relationship(back_populates="categories")

    products: Mapped[list[Product]] = relationship(back_populates="category")


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id"), nullable=False)
    category_id: Mapped[int | None] = mapped_column(ForeignKey("categories.id"), nullable=True)

    base_name: Mapped[str] = mapped_column(String(255), nullable=False)
    generated_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    article: Mapped[str | None] = mapped_column(String(128), nullable=True)
    color: Mapped[str | None] = mapped_column(String(128), nullable=True)
    barcode: Mapped[str | None] = mapped_column(String(128), nullable=True)

    needs_barcode_registration: Mapped[bool] = mapped_column(Boolean, default=False)
    barcode_registered_in_gs1: Mapped[bool] = mapped_column(Boolean, default=False)

    length_cm: Mapped[float | None] = mapped_column(Float, nullable=True)
    width_cm: Mapped[float | None] = mapped_column(Float, nullable=True)
    height_cm: Mapped[float | None] = mapped_column(Float, nullable=True)
    weight_kg: Mapped[float | None] = mapped_column(Float, nullable=True)

    package_length_cm: Mapped[float | None] = mapped_column(Float, nullable=True)
    package_width_cm: Mapped[float | None] = mapped_column(Float, nullable=True)
    package_height_cm: Mapped[float | None] = mapped_column(Float, nullable=True)
    gross_weight_kg: Mapped[float | None] = mapped_column(Float, nullable=True)

    source_type: Mapped[str] = mapped_column(String(32), default="custom")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    client: Mapped[Client] = relationship(back_populates="products")
    category: Mapped[Category | None] = relationship(back_populates="products")
    attributes: Mapped[list[ProductAttributeValue]] = relationship(back_populates="product")


class ProductAttributeDefinition(Base):
    __tablename__ = "attribute_definitions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    category_id: Mapped[int | None] = mapped_column(ForeignKey("categories.id"), nullable=True)
    internal_name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    data_type: Mapped[str] = mapped_column(String(32), default="string")
    base_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    is_required: Mapped[bool] = mapped_column(Boolean, default=False)
    is_enum: Mapped[bool] = mapped_column(Boolean, default=False)
    allowed_values_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    synonyms: Mapped[list[AttributeSynonym]] = relationship(back_populates="attribute_definition")


class AttributeSynonym(Base):
    __tablename__ = "attribute_synonyms"
    __table_args__ = (
        UniqueConstraint("client_id", "synonym_name", name="uq_synonym_per_client"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    client_id: Mapped[int | None] = mapped_column(ForeignKey("clients.id"), nullable=True)
    attribute_definition_id: Mapped[int] = mapped_column(
        ForeignKey("attribute_definitions.id"), nullable=False
    )
    synonym_name: Mapped[str] = mapped_column(String(255), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=100)

    attribute_definition: Mapped[ProductAttributeDefinition] = relationship(back_populates="synonyms")


class ProductAttributeValue(Base):
    __tablename__ = "attribute_values"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False)
    attribute_definition_id: Mapped[int] = mapped_column(
        ForeignKey("attribute_definitions.id"), nullable=False
    )
    value_string: Mapped[str | None] = mapped_column(Text, nullable=True)
    value_number: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)

    product: Mapped[Product] = relationship(back_populates="attributes")
    attribute_definition: Mapped[ProductAttributeDefinition] = relationship()


class DuplicateCandidate(Base):
    __tablename__ = "duplicate_candidates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    new_product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False)
    existing_product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False)
    similarity_score: Mapped[float] = mapped_column(Float, nullable=False)
    matched_by: Mapped[str] = mapped_column(String(64), nullable=False)
    details_json: Mapped[str | None] = mapped_column(Text, nullable=True)
