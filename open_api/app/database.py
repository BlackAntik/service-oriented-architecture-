from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4
import os

from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Enum as SQLEnum, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

from generated.models.schemas import ProductStatus, OrderStatus, DiscountType

Base = declarative_base()

SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/products")

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class ProductDB(Base):
    __tablename__ = "products"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    name = Column(String(255), nullable=False)
    description = Column(String(2000), nullable=True)
    price = Column(Float, nullable=False)
    category = Column(String(100), nullable=False)
    stock = Column(Integer, nullable=False)
    status = Column(SQLEnum(ProductStatus), nullable=False, default=ProductStatus.ACTIVE)
    seller_id = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class PromoCodeDB(Base):
    __tablename__ = "promo_codes"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    code = Column(String(20), unique=True, nullable=False)
    discount_type = Column(SQLEnum(DiscountType), nullable=False)
    discount_value = Column(Float, nullable=False)
    min_order_amount = Column(Float, nullable=False)
    max_uses = Column(Integer, nullable=False)
    current_uses = Column(Integer, default=0)
    valid_from = Column(DateTime, nullable=False)
    valid_until = Column(DateTime, nullable=False)
    active = Column(Boolean, default=True)


class OrderDB(Base):
    __tablename__ = "orders"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    user_id = Column(String, nullable=False)
    status = Column(SQLEnum(OrderStatus), nullable=False, default=OrderStatus.CREATED)
    promo_code_id = Column(String, ForeignKey("promo_codes.id"), nullable=True)
    total_amount = Column(Float, nullable=False)
    discount_amount = Column(Float, default=0)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    items = relationship("OrderItemDB", back_populates="order", cascade="all, delete-orphan")
    promo_code = relationship("PromoCodeDB")


class OrderItemDB(Base):
    __tablename__ = "order_items"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    order_id = Column(String, ForeignKey("orders.id"), nullable=False)
    product_id = Column(String, ForeignKey("products.id"), nullable=False)
    quantity = Column(Integer, nullable=False)
    price_at_order = Column(Float, nullable=False)

    order = relationship("OrderDB", back_populates="items")
    product = relationship("ProductDB")


class UserOperationDB(Base):
    __tablename__ = "user_operations"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    user_id = Column(String, nullable=False)
    operation_type = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))


def init_db():
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()