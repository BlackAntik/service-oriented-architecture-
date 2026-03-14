import uuid
from datetime import date, datetime, timezone, timedelta

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from config import FLIGHT_GRPC_ADDR, FLIGHT_SERVICE_API_KEY
from flight.v1 import flight_service_pb2 as pb2
from flight.v1 import flight_service_pb2_grpc as pb2_grpc


def _dt_to_ts(dt: datetime) -> Timestamp:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts = Timestamp()
    ts.FromDatetime(dt.astimezone(timezone.utc))
    return ts


class FlightClient:
    def __init__(self) -> None:
        self._channel = grpc.insecure_channel(FLIGHT_GRPC_ADDR)
        self._stub = pb2_grpc.FlightServiceStub(self._channel)
        self._metadata = (("x-api-key", FLIGHT_SERVICE_API_KEY),)

    def search_flights(self, origin: str, destination: str, d: date | None) -> list[pb2.Flight]:
        req = pb2.SearchFlightsRequest(origin=origin, destination=destination)
        if d is not None:
            start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            req.departure_time_from.CopyFrom(_dt_to_ts(start))
            req.departure_time_to.CopyFrom(_dt_to_ts(end))
        resp = self._stub.SearchFlights(req, metadata=self._metadata)
        return list(resp.flights)

    def get_flight(self, flight_id: uuid.UUID) -> pb2.Flight:
        resp = self._stub.GetFlight(pb2.GetFlightRequest(flight_id=str(flight_id)), metadata=self._metadata)
        return resp.flight

    def reserve_seats(self, flight_id: uuid.UUID, booking_id: uuid.UUID, seats: int) -> pb2.Reservation:
        resp = self._stub.ReserveSeats(
            pb2.ReserveSeatsRequest(flight_id=str(flight_id), booking_id=str(booking_id), seats=seats),
            metadata=self._metadata,
        )
        return resp.reservation

    def release_reservation(self, booking_id: uuid.UUID) -> None:
        self._stub.ReleaseReservation(
            pb2.ReleaseReservationRequest(booking_id=str(booking_id)), metadata=self._metadata
        )
