package io.meridian.ledger;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Authenticates every gRPC call before it can reach a money-moving endpoint. */
public final class GrpcApiKeyInterceptor implements ServerInterceptor {
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private final byte[] expected;

    public GrpcApiKeyInterceptor(String apiKey) {
        this.expected = ("Bearer " + apiKey).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String authorization = headers.get(AUTHORIZATION);
        byte[] actual = authorization == null ? new byte[0] : authorization.getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, actual)) {
            call.close(Status.UNAUTHENTICATED.withDescription("a valid ledger credential is required"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }
}
