from concurrent import futures

import grpc

from config import GRPC_HOST, GRPC_PORT, FLIGHT_SERVICE_API_KEY
from grpc_service import FlightService
from flight.v1 import flight_service_pb2_grpc as pb2_grpc


class AuthInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        metadata = dict(handler_call_details.invocation_metadata)
        api_key = metadata.get("x-api-key")

        if api_key != FLIGHT_SERVICE_API_KEY:
            def abort(ignored_request, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or missing API key")

            return grpc.unary_unary_rpc_method_handler(abort)

        return continuation(handler_call_details)


def serve() -> None:
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=(AuthInterceptor(),),
    )
    pb2_grpc.add_FlightServiceServicer_to_server(FlightService(), server)
    server.add_insecure_port(f"{GRPC_HOST}:{GRPC_PORT}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
