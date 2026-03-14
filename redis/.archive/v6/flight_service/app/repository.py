import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from config import RESERVATION_TTL_SECONDS
from models import Flight, SeatReservation

FLIGHT_STATUS_SCHEDULED = "SCHEDULED"

RESERVATION_STATUS_ACTIVE = "ACTIVE"
RESERVATION_STATUS_RELEASED = "RELEASED"


class NotFound(Exception):
    pass


class ResourceExhausted(Exception):
    pass


class FailedPrecondition(Exception):
    pass


def search_flights(
    session: Session,
    origin: str,
    destination: str,
    departure_from: datetime | None,
    departure_to: datetime | None,
    seats_required: int,
) -> list[Flight]:
    conditions = [
        Flight.origin_iata == origin,
        Flight.destination_iata == destination,
        Flight.status == FLIGHT_STATUS_SCHEDULED,
    ]
    if departure_from is not None:
        conditions.append(Flight.departure_time >= departure_from)
    if departure_to is not None:
        conditions.append(Flight.departure_time < departure_to)
    if seats_required and seats_required > 0:
        conditions.append(Flight.available_seats >= seats_required)

    stmt = select(Flight).where(and_(*conditions)).order_by(Flight.departure_time.asc())
    return list(session.execute(stmt).scalars().all())


def get_flight(session: Session, flight_id: uuid.UUID) -> Flight:
    flight = session.get(Flight, flight_id)
    if not flight:
        raise NotFound("flight not found")
    return flight


def reserve_seats(session: Session, flight_id: uuid.UUID, booking_id: uuid.UUID, seats: int) -> SeatReservation:
    if seats <= 0:
        raise FailedPrecondition("seat_count must be > 0")

    # Check if reservation already exists for this booking_id
    existing_stmt = select(SeatReservation).where(SeatReservation.booking_id == booking_id)
    existing = session.execute(existing_stmt).scalars().first()
    if existing:
        # If it exists and is active, return it (idempotency)
        if existing.status == RESERVATION_STATUS_ACTIVE:
            return existing
        # If it exists but not active (e.g. released/expired), we can't reuse it for a new reservation logic easily without more context,
        # but for this task let's assume we fail if any reservation exists for this booking_id to avoid confusion.
        raise FailedPrecondition("reservation already exists for this booking_id")

    # Lock flight row for update
    flight_stmt = select(Flight).where(Flight.id == flight_id).with_for_update(of=Flight)
    flight = session.execute(flight_stmt).scalars().first()
    if not flight:
        raise NotFound("flight not found")

    if flight.status != FLIGHT_STATUS_SCHEDULED:
        raise FailedPrecondition("flight is not SCHEDULED")

    if flight.available_seats < seats:
        raise ResourceExhausted("not enough seats")

    # Decrement available seats
    flight.available_seats -= seats

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=RESERVATION_TTL_SECONDS)

    reservation = SeatReservation(
        flight_id=flight.id,
        booking_id=booking_id,
        seats=seats,
        status=RESERVATION_STATUS_ACTIVE,
        reserved_at=now,
        expires_at=expires_at,
    )
    session.add(reservation)
    session.flush()
    return reservation


def release_reservation(session: Session, booking_id: uuid.UUID) -> None:
    res_stmt = (
        select(SeatReservation)
        .where(
            SeatReservation.booking_id == booking_id,
        )
        .with_for_update()
    )
    reservation = session.execute(res_stmt).scalars().first()
    if not reservation:
        raise NotFound("reservation not found")

    if reservation.status != RESERVATION_STATUS_ACTIVE:
        # If already released or expired, we can consider it a success (idempotency) or error.
        # For this task, let's just return if it's already released.
        if reservation.status == RESERVATION_STATUS_RELEASED:
            return
        # If expired, we might want to do nothing or error. Let's assume we can't release expired.
        # But usually release is called on cancellation.
        # Let's just proceed if active.
        raise NotFound("active reservation not found")

    flight_stmt = select(Flight).where(Flight.id == reservation.flight_id).with_for_update(of=Flight)
    flight = session.execute(flight_stmt).scalars().first()
    if not flight:
        raise NotFound("flight not found")

    flight.available_seats += reservation.seats
    reservation.status = RESERVATION_STATUS_RELEASED
