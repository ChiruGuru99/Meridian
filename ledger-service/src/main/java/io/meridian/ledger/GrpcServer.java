package io.meridian.ledger;

import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Runs the gRPC server inside the Spring lifecycle, with the standard gRPC health service. */
@Component
public class GrpcServer implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(GrpcServer.class);
    private final GrpcLedgerEndpoint endpoint;
    private final int port;
    private final HealthStatusManager health = new HealthStatusManager();
    private volatile Server server;

    public GrpcServer(GrpcLedgerEndpoint endpoint, @Value("${ledger.grpc.port:9091}") int port) {
        this.endpoint = endpoint;
        this.port = port;
    }

    @Override
    public void start() {
        try {
            server = NettyServerBuilder.forPort(port)
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .addService(endpoint)
                    .addService(health.getHealthService())
                    .build()
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        health.setStatus("", ServingStatus.SERVING);
        log.info("gRPC ledger listening on {}", server.getPort());
    }

    @Override
    public void stop() {
        health.enterTerminalState();
        server.shutdown();
        try {
            server.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return server != null && !server.isShutdown();
    }

    public int port() {
        return server.getPort();
    }
}
