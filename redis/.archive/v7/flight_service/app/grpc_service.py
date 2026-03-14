import uuid
from datetime import datetime, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import repository
from cache import cache_proto, invalidate_cache
from db import session_scope
from flight.v1 import flight_service_pb2 as pb2
from flight.v1 import flight_service_pb2_grpc as pb2_grpc


def dt_to_ts(dt: datetime) -> Timestamp:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts = Timestamp()
    ts.FromDatetime(dt.astimezone(timezone.utc))
    return ts


def ts_to_dt(ts: Timestamp) -> datetime:
    return ts.ToDatetime().replace(tzinfo=timezone.utc)


def flight_status_to_proto(status: str) -> int:
    return {
        "SCHEDULED": pb2.FLIGHT_STATUS_SCHEDULED,
        "DELAYED": pb2.FLIGHT_STATUS_DELAYED,
        "CANCELLED": pb2.FLIGHT_STATUS_CANCELLED,
        "DEPARTED": pb2.FLIGHT_STATUS_DEPARTED,
        "COMPLETED": pb2.FLIGHT_STATUS_COMPLETED,
    }.get(status, pb2.FLIGHT_STATUS_UNSPECIFIED)


def reservation_status_to_proto(status: str) -> int:
    return {
        "ACTIVE": pb2.RESERVATION_STATUS_ACTIVE,
        "RELEASED": pb2.RESERVATION_STATUS_RELEASED,
        "EXPIRED": pb2.RESERVATION_STATUS_EXPIRED,
    }.get(status, pb2.RESERVATION_STATUS_UNSPECIFIED)


def flight_to_proto(f) -> pb2.Flight:
    return pb2.Flight(
        id=str(f.id),
        flight_number=f.flight_number,
        airline_iata_code=f.airline.iata_code,
        airline_name=f.airline.name,
        route=pb2.Route(origin=f.origin_iata, destination=f.destination_iata),
        departure_time=dt_to_ts(f.departure_time),
        arrival_time=dt_to_ts(f.arrival_time),
        total_seats=f.total_seats,
        available_seats=f.available_seats,
        price_cents=f.price_cents,
        currency=f.currency,
        status=flight_status_to_proto(f.status),
    )


def reservation_to_proto(r) -> pb2.Reservation:
    return pb2.Reservation(
        id=str(r.id),
        flight_id=str(r.flight_id),
        booking_id=str(r.booking_id),
        seats=r.seats,
        status=reservation_status_to_proto(r.status),
        reserved_at=dt_to_ts(r.reserved_at),
        expires_at=dt_to_ts(r.expires_at),
    )


class FlightService(pb2_grpc.FlightServiceServicer):
    @cache_proto(
        key_func=lambda self, request, context: f"search:{request.origin}:{request.destination}:{request.departure_time_from.seconds}",
        ttl=300,
        proto_class=pb2.SearchFlightsResponse,
    )
    def SearchFlights(self, request: pb2.SearchFlightsRequest, context: grpc.ServicerContext) -> pb2.SearchFlightsResponse:
        if not request.origin or not request.destination:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "origin and destination are required")

        departure_from = ts_to_dt(request.departure_time_from) if request.HasField("departure_time_from") else None
        departure_to = ts_to_dt(request.departure_time_to) if request.HasField("departure_time_to") else None

        with session_scope() as session:
            flights = repository.search_flights(
                session=session,
                origin=request.origin,
                destination=request.destination,
                departure_from=departure_from,
                departure_to=departure_to,
                seats_required=request.seats_required,
            )
            return pb2.SearchFlightsResponse(flights=[flight_to_proto(f) for f in flights])

    @cache_proto(
        key_func=lambda self, request, context: f"flight:{request.flight_id}",
        ttl=300,
        proto_class=pb2.GetFlightResponse,
    )
    def GetFlight(self, request: pb2.GetFlightRequest, context: grpc.ServicerContext) -> pb2.GetFlightResponse:
        try:
            flight_id = uuid.UUID(request.flight_id)
        except Exception:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid flight_id")

        with session_scope() as session:
            try:
                flight = repository.get_flight(session, flight_id)
            except repository.NotFound:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
            return pb2.GetFlightResponse(flight=flight_to_proto(flight))

    def ReserveSeats(self, request: pb2.ReserveSeatsRequest, context: grpc.ServicerContext) -> pb2.ReserveSeatsResponse:
        try:
            flight_id = uuid.UUID(request.flight_id)
            booking_id = uuid.UUID(request.booking_id)
        except Exception:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid flight_id or booking_id")

        if request.seats <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "seats must be > 0")

        with session_scope() as session:
            try:
                reservation = repository.reserve_seats(
                    session=session, flight_id=flight_id, booking_id=booking_id, seats=request.seats
                )
            except repository.NotFound:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
            except repository.ResourceExhausted:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "not enough seats")
            except repository.FailedPrecondition as e:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(e))

            # Invalidate cache
            invalidate_cache(f"flight:{flight_id}")
            invalidate_cache("search:*")  # Invalidate all search results as availability changed

            return pb2.ReserveSeatsResponse(reservation=reservation_to_proto(reservation))

    def ReleaseReservation(
        self, request: pb2.ReleaseReservationRequest, context: grpc.ServicerContext
    ) -> pb2.ReleaseReservationResponse:
        try:
            booking_id = uuid.UUID(request.booking_id)
        except Exception:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid booking_id")

        with session_scope() as session:
            try:
                flight_id = repository.release_reservation(session=session, booking_id=booking_id)
            except repository.NotFound:
                context.abort(grpc.StatusCode.NOT_FOUND, "active reservation not found")

            # Invalidate cache
            invalidate_cache(f"flight:{flight_id}")
            invalidate_cache("search:*")

            return pb2.ReleaseReservationResponse()
