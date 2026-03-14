from concurrent import futures

import grpc

from config import GRPC_HOST, GRPC_PORT
from grpc_service import FlightService
from flight.v1 import flight_service_pb2_grpc as pb2_grpc


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_FlightServiceServicer_to_server(FlightService(), server)
    server.add_insecure_port(f"{GRPC_HOST}:{GRPC_PORT}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
