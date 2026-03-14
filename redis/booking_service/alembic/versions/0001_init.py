from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "passengers",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("email", sa.String(length=254), nullable=False),
    )
    op.create_index("uq_passengers_email", "passengers", ["email"], unique=True)

    op.create_table(
        "bookings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("flight_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("passenger_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("passengers.id"), nullable=False),
        sa.Column("seat_count", sa.Integer(), nullable=False),
        sa.Column("total_price_cents", sa.BigInteger(), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("seat_count > 0", name="ck_bookings_seat_count_positive"),
        sa.CheckConstraint("total_price_cents > 0", name="ck_bookings_total_price_positive"),
    )
    op.create_index("ix_bookings_user_id", "bookings", ["user_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_bookings_user_id", table_name="bookings")
    op.drop_table("bookings")

    op.drop_index("uq_passengers_email", table_name="passengers")
    op.drop_table("passengers")
