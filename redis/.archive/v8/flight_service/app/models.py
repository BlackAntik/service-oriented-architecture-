import uuid
from datetime import date, datetime

from sqlalchemy import BigInteger, CheckConstraint, Date, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Airline(Base):
    __tablename__ = "airlines"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    iata_code: Mapped[str] = mapped_column(String(2), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)


class Airport(Base):
    __tablename__ = "airports"

    iata_code: Mapped[str] = mapped_column(String(3), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    city: Mapped[str | None] = mapped_column(String(120), nullable=True)
    country: Mapped[str | None] = mapped_column(String(120), nullable=True)


class Flight(Base):
    __tablename__ = "flights"
    __table_args__ = (
        UniqueConstraint("flight_number", "departure_date", name="uq_flight_number_departure_date"),
        CheckConstraint("total_seats > 0", name="ck_flights_total_seats_positive"),
        CheckConstraint("available_seats >= 0", name="ck_flights_available_seats_nonnegative"),
        CheckConstraint("available_seats <= total_seats", name="ck_flights_available_seats_le_total"),
        CheckConstraint("price_cents > 0", name="ck_flights_price_positive"),
        CheckConstraint("origin_iata <> destination_iata", name="ck_flights_origin_ne_destination"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    airline_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("airlines.id"), nullable=False)
    flight_number: Mapped[str] = mapped_column(String(16), nullable=False)

    origin_iata: Mapped[str] = mapped_column(String(3), ForeignKey("airports.iata_code"), nullable=False)
    destination_iata: Mapped[str] = mapped_column(String(3), ForeignKey("airports.iata_code"), nullable=False)

    departure_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    arrival_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    departure_date: Mapped[date] = mapped_column(Date, nullable=False)

    total_seats: Mapped[int] = mapped_column(Integer, nullable=False)
    available_seats: Mapped[int] = mapped_column(Integer, nullable=False)

    price_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)

    status: Mapped[str] = mapped_column(String(32), nullable=False)

    airline: Mapped[Airline] = relationship(lazy="joined")


class SeatReservation(Base):
    __tablename__ = "seat_reservations"
    __table_args__ = (
        UniqueConstraint("booking_id", name="uq_seat_reservations_booking_id"),
        CheckConstraint("seats > 0", name="ck_seat_reservations_seats_positive"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    flight_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("flights.id"), nullable=False)
    booking_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    seats: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)

    reserved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    flight: Mapped[Flight] = relationship(lazy="joined")