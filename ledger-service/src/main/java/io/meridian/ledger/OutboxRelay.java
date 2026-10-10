package io.meridian.ledger;

import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Polls the outbox and publishes to Kafka (I5). A row is marked published only after the broker
 * acknowledged it (acks=all, idempotent producer), so a crash between send and mark re-sends:
 * delivery is at-least-once and consumers dedupe. Rows are claimed with SKIP LOCKED so several
 * replicas can relay concurrently without double-claiming.
 */
@Component
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        public Iterable<String> keys(Map<String, String> c) { return c.keySet(); }
        public String get(Map<String, String> c, String k) { return c == null ? null : c.get(k); }
    };

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final JsonMapper json;

    record Row(long id, String topic, String key, String payload, String headers) {}

    public OutboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate tx, JsonMapper json,
                       MeterRegistry metrics) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.tx = tx;
        this.json = json;
        metrics.gauge("ledger.outbox.pending", this, r -> r.pending());
    }

    @Scheduled(fixedDelayString = "${ledger.outbox.poll-ms:200}", initialDelayString = "${ledger.outbox.poll-ms:200}")
    public void relay() {
        try {
            while (publishBatch() > 0) {
                // drain until empty
            }
        } catch (RuntimeException e) {
            log.warn("outbox relay paused: {}", e.getMessage());
        }
    }

    /** Publishes one batch; returns the number of rows marked published. */
    int publishBatch() {
        Integer n = tx.execute(status -> {
            List<Row> rows = jdbc.sql("""
                            SELECT id, topic, message_key, payload::text, headers::text FROM outbox
                            WHERE published_at IS NULL ORDER BY id LIMIT 200 FOR UPDATE SKIP LOCKED""")
                    .query((rs, i) -> new Row(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5)))
                    .list();
            List<Long> sent = new ArrayList<>();
            for (Row row : rows) {
                try {
                    send(row);
                    sent.add(row.id());
                } catch (Exception e) {
                    log.warn("publish of outbox row {} failed, will retry: {}", row.id(), e.toString());
                    break; // keep per-key ordering: stop at the first failure
                }
            }
            if (!sent.isEmpty()) {
                jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)").param("ids", sent).update();
            }
            return sent.size();
        });
        return n == null ? 0 : n;
    }

    private void send(Row row) throws Exception {
        Map<String, String> headers = json.readValue(row.headers(), new TypeReference<Map<String, String>>() {});
        var record = new ProducerRecord<String, String>(row.topic(), row.key(), row.payload());
        headers.forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        // Re-enter the trace of the request that wrote the event, so the publish span is its child.
        Context ctx = W3CTraceContextPropagator.getInstance().extract(Context.root(), headers, GETTER);
        try (var ignored = ctx.makeCurrent()) {
            kafka.send(record).get(15, TimeUnit.SECONDS);
        }
    }

    long pending() {
        try {
            return jdbc.sql("SELECT count(*) FROM outbox WHERE published_at IS NULL").query(Long.class).single();
        } catch (RuntimeException e) {
            return -1;
        }
    }
}
