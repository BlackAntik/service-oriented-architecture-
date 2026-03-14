"""seed data

Revision ID: 0002_seed_data
Revises: 0001_init
Create Date: 2026-03-14

"""
from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import UUID

from alembic import op
import sqlalchemy as sa


revision = "0002_seed_data"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    airlines = sa.table(
        "airlines",
        sa.column("id", sa.UUID()),
        sa.column("iata_code", sa.String()),
        sa.column("name", sa.String()),
    )

    airports = sa.table(
        "airports",
        sa.column("iata_code", sa.String()),
        sa.column("name", sa.String()),
        sa.column("city", sa.String()),
        sa.column("country", sa.String()),
    )

    flights = sa.table(
        "flights",
        sa.column("id", sa.UUID()),
        sa.column("airline_id", sa.UUID()),
        sa.column("flight_number", sa.String()),
        sa.column("origin_iata", sa.String()),
        sa.column("destination_iata", sa.String()),
        sa.column("departure_time", sa.DateTime(timezone=True)),
        sa.column("arrival_time", sa.DateTime(timezone=True)),
        sa.column("departure_date", sa.Date()),
        sa.column("total_seats", sa.Integer()),
        sa.column("available_seats", sa.Integer()),
        sa.column("price_cents", sa.BigInteger()),
        sa.column("currency", sa.String()),
        sa.column("status", sa.String()),
    )

    su_id = UUID("00000000-0000-0000-0000-000000000001")

    op.bulk_insert(
        airlines,
        [
            {"id": su_id, "iata_code": "SU", "name": "Aeroflot"},
        ],
    )

    op.bulk_insert(
        airports,
        [
            {"iata_code": "SVO", "name": "Sheremetyevo", "city": "Moscow", "country": "RU"},
            {"iata_code": "LED", "name": "Pulkovo", "city": "Saint Petersburg", "country": "RU"},
            {"iata_code": "VKO", "name": "Vnukovo", "city": "Moscow", "country": "RU"},
        ],
    )

    d = date(2026, 4, 1)

    def dt(y: int, m: int, day: int, hh: int, mm: int) -> datetime:
        return datetime(y, m, day, hh, mm, tzinfo=timezone.utc)

    op.bulk_insert(
        flights,
        [
            {
                "id": UUID("10000000-0000-0000-0000-000000000001"),
                "airline_id": su_id,
                "flight_number": "SU1234",
                "origin_iata": "SVO",
                "destination_iata": "LED",
                "departure_time": dt(2026, 4, 1, 7, 0),
                "arrival_time": dt(2026, 4, 1, 8, 30),
                "departure_date": d,
                "total_seats": 180,
                "available_seats": 180,
                "price_cents": 550000,
                "currency": "RUB",
                "status": "SCHEDULED",
            },
            {
                "id": UUID("10000000-0000-0000-0000-000000000002"),
                "airline_id": su_id,
                "flight_number": "SU4321",
                "origin_iata": "VKO",
                "destination_iata": "LED",
                "departure_time": dt(2026, 4, 1, 12, 0),
                "arrival_time": dt(2026, 4, 1, 13, 30),
                "departure_date": d,
                "total_seats": 150,
                "available_seats": 150,
                "price_cents": 490000,
                "currency": "RUB",
                "status": "SCHEDULED",
            },
        ],
    )


def downgrade() -> None:
    conn = op.get_bind()

    conn.execute(sa.text("DELETE FROM seat_reservations"))
    conn.execute(sa.text("DELETE FROM flights"))
    conn.execute(sa.text("DELETE FROM airports"))
    conn.execute(sa.text("DELETE FROM airlines"))