import uuid
from datetime import date, datetime, timezone

import grpc
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select

from db import session_scope
from grpc_client import FlightClient
from models import Booking, Passenger


BOOKING_STATUS_CONFIRMED = "CONFIRMED"
BOOKING_STATUS_CANCELLED = "CANCELLED"

app = FastAPI(title="Booking Service")

flight_client = FlightClient()


class FlightOut(BaseModel):
    id: str
    flight_number: str
    airline_iata_code: str
    airline_name: str
    origin: str
    destination: str
    departure_time: datetime
    arrival_time: datetime
    total_seats: int
    available_seats: int
    price_cents: int
    currency: str
    status: str


class BookingCreateIn(BaseModel):
    user_id: str = Field(min_length=1, max_length=64)
    flight_id: uuid.UUID
    passenger_name: str = Field(min_length=1, max_length=200)
    passenger_email: EmailStr
    seat_count: int = Field(gt=0)


class BookingOut(BaseModel):
    id: uuid.UUID
    user_id: str
    flight_id: uuid.UUID
    passenger_name: str
    passenger_email: str
    seat_count: int
    total_price_cents: int
    currency: str
    status: str
    created_at: datetime


def _flight_to_out(f) -> FlightOut:
    return FlightOut(
        id=f.id,
        flight_number=f.flight_number,
        airline_iata_code=f.airline_iata_code,
        airline_name=f.airline_name,
        origin=f.route.origin,
        destination=f.route.destination,
        departure_time=f.departure_time.ToDatetime().replace(tzinfo=timezone.utc),
        arrival_time=f.arrival_time.ToDatetime().replace(tzinfo=timezone.utc),
        total_seats=f.total_seats,
        available_seats=f.available_seats,
        price_cents=f.price_cents,
        currency=f.currency,
        status=str(f.status),
    )


def _booking_to_out(b: Booking) -> BookingOut:
    return BookingOut(
        id=b.id,
        user_id=b.user_id,
        flight_id=b.flight_id,
        passenger_name=b.passenger.name,
        passenger_email=b.passenger.email,
        seat_count=b.seat_count,
        total_price_cents=b.total_price_cents,
        currency=b.currency,
        status=b.status,
        created_at=b.created_at,
    )


@app.get("/flights", response_model=list[FlightOut])
def search_flights(
    origin: str = Query(..., min_length=3, max_length=3),
    destination: str = Query(..., min_length=3, max_length=3),
    date_: str | None = Query(None, alias="date"),
):
    d: date | None = None
    if date_ is not None:
        try:
            d = date.fromisoformat(date_)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid date format, expected YYYY-MM-DD")

    flights = flight_client.search_flights(origin=origin, destination=destination, d=d)
    return [_flight_to_out(f) for f in flights]


@app.get("/flights/{id}", response_model=FlightOut)
def get_flight(id: uuid.UUID):
    try:
        f = flight_client.get_flight(id)
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail="flight not found")
        raise HTTPException(status_code=502, detail="flight service error")
    return _flight_to_out(f)


@app.post("/bookings", response_model=BookingOut)
def create_booking(payload: BookingCreateIn):
    booking_id = uuid.uuid4()

    try:
        flight = flight_client.get_flight(payload.flight_id)
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail="flight not found")
        raise HTTPException(status_code=502, detail="flight service error")

    try:
        flight_client.reserve_seats(payload.flight_id, booking_id, payload.seat_count)
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
            raise HTTPException(status_code=409, detail="not enough seats")
        if e.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail="flight not found")
        if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
            raise HTTPException(status_code=409, detail=e.details())
        raise HTTPException(status_code=400, detail="cannot reserve seats")

    total_price = payload.seat_count * int(flight.price_cents)
    currency = flight.currency

    try:
        with session_scope() as session:
            existing_passenger = session.execute(select(Passenger).where(Passenger.email == str(payload.passenger_email))).scalars().first()
            if existing_passenger:
                passenger = existing_passenger
            else:
                passenger = Passenger(name=payload.passenger_name, email=str(payload.passenger_email))
                session.add(passenger)
                session.flush()

            booking = Booking(
                id=booking_id,
                user_id=payload.user_id,
                flight_id=payload.flight_id,
                passenger_id=passenger.id,
                seat_count=payload.seat_count,
                total_price_cents=total_price,
                currency=currency,
                status=BOOKING_STATUS_CONFIRMED,
                created_at=datetime.now(timezone.utc),
            )
            session.add(booking)
            session.flush()
            session.refresh(booking)

            return _booking_to_out(booking)
    except Exception:
        try:
            flight_client.release_reservation(booking_id)
        except:
            pass
        raise


@app.get("/bookings/{id}", response_model=BookingOut)
def get_booking(id: uuid.UUID):
    with session_scope() as session:
        booking = session.get(Booking, id)
        if not booking:
            raise HTTPException(status_code=404, detail="booking not found")
        return _booking_to_out(booking)


@app.post("/bookings/{id}/cancel", response_model=BookingOut)
def cancel_booking(id: uuid.UUID):
    with session_scope() as session:
        booking = session.get(Booking, id)
        if not booking:
            raise HTTPException(status_code=404, detail="booking not found")
        if booking.status != BOOKING_STATUS_CONFIRMED:
            raise HTTPException(status_code=409, detail="booking is not CONFIRMED")

        try:
            flight_client.release_reservation(booking.id)
        except grpc.RpcError:
            pass

        booking.status = BOOKING_STATUS_CANCELLED
        session.add(booking)
        session.flush()
        session.refresh(booking)
        return _booking_to_out(booking)


@app.get("/bookings", response_model=list[BookingOut])
def list_bookings(user_id: str = Query(..., min_length=1, max_length=64)):
    with session_scope() as session:
        stmt = select(Booking).where(Booking.user_id == user_id).order_by(Booking.created_at.desc())
        bookings = list(session.execute(stmt).scalars().all())
        return [_booking_to_out(b) for b in bookings]
