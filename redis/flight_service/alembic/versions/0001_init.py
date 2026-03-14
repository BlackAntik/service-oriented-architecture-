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
        "airlines",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("iata_code", sa.String(length=2), nullable=False, unique=True),
        sa.Column("name", sa.String(length=200), nullable=False),
    )

    op.create_table(
        "airports",
        sa.Column("iata_code", sa.String(length=3), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=200), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("country", sa.String(length=120), nullable=True),
    )

    op.create_table(
        "flights",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("airline_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("airlines.id"), nullable=False),
        sa.Column("flight_number", sa.String(length=16), nullable=False),
        sa.Column("origin_iata", sa.String(length=3), sa.ForeignKey("airports.iata_code"), nullable=False),
        sa.Column("destination_iata", sa.String(length=3), sa.ForeignKey("airports.iata_code"), nullable=False),
        sa.Column("departure_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("arrival_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("departure_date", sa.Date(), nullable=False),
        sa.Column("total_seats", sa.Integer(), nullable=False),
        sa.Column("available_seats", sa.Integer(), nullable=False),
        sa.Column("price_cents", sa.BigInteger(), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.CheckConstraint("total_seats > 0", name="ck_flights_total_seats_positive"),
        sa.CheckConstraint("available_seats >= 0", name="ck_flights_available_seats_nonnegative"),
        sa.CheckConstraint("available_seats <= total_seats", name="ck_flights_available_seats_le_total"),
        sa.CheckConstraint("price_cents > 0", name="ck_flights_price_positive"),
        sa.CheckConstraint("origin_iata <> destination_iata", name="ck_flights_origin_ne_destination"),
    )
    op.create_index("uq_flight_number_departure_date", "flights", ["flight_number", "departure_date"], unique=True)

    op.create_table(
        "seat_reservations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("flight_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("flights.id"), nullable=False),
        sa.Column("booking_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("seats", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reserved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("seats > 0", name="ck_seat_reservations_seats_positive"),
    )
    op.create_index("uq_seat_reservations_booking_id", "seat_reservations", ["booking_id"], unique=True)


def downgrade() -> None:
    op.drop_index("uq_seat_reservations_booking_id", table_name="seat_reservations")
    op.drop_table("seat_reservations")

    op.drop_index("uq_flight_number_departure_date", table_name="flights")
    op.drop_table("flights")

    op.drop_table("airports")
    op.drop_table("airlines")
