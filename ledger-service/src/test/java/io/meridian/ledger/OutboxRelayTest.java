package io.meridian.ledger;

import io.meridian.ledger.Model.PostCommand;
import io.meridian.ledger.Model.PostingLine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.redpanda.RedpandaContainer;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** I5 on the producer side: an event leaves the outbox only after the broker acknowledged it. */
class OutboxRelayTest {

    private static OutboxRelay relay(String bootstrap) {
        Map<String, Object> cfg = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 2_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2_000,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new OutboxRelay(TestEnv.app(), new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(cfg)),
                TestEnv.bean(TransactionTemplate.class), TestEnv.bean(JsonMapper.class), new SimpleMeterRegistry());
    }

    @Test
    void eventIsMarkedPublishedOnlyAfterTheBrokerAcknowledges() {
        String a = TestEnv.newAccount("A", 0L, "EUR"), b = TestEnv.newAccount("B", 0L, "EUR");
        TestEnv.fund(a, "EUR", 100);
        UUID txn = TestEnv.ledger().post(new PostCommand(TestEnv.key(), "TRANSFER", "outbox", List.of(
                new PostingLine(a, "EUR", 10), new PostingLine(b, "EUR", -10)), Map.of())).transaction().id();
        JdbcClient db = TestEnv.app();

        // Broker unreachable: nothing is marked published, the event stays queued.
        assertThat(relay("localhost:1").publishBatch()).isZero();
        assertThat(publishedAt(db, txn)).isNull();

        try (var redpanda = new RedpandaContainer("redpandadata/redpanda:v26.1.18")) {
            redpanda.start();
            OutboxRelay live = relay(redpanda.getBootstrapServers());
            while (live.publishBatch() > 0) {
                // drain everything every test has written
            }
            assertThat(publishedAt(db, txn)).isNotNull();

            try (var consumer = new KafkaConsumer<String, String>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, redpanda.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "test",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
                consumer.subscribe(List.of(LedgerService.TOPIC));
                boolean found = false;
                long deadline = System.currentTimeMillis() + 30_000;
                while (!found && System.currentTimeMillis() < deadline) {
                    for (var rec : consumer.poll(Duration.ofMillis(500))) {
                        if (rec.key().equals(txn.toString())) {
                            assertThat(rec.value()).contains(a).contains("ledger.transaction.posted");
                            found = true;
                        }
                    }
                }
                assertThat(found).as("event for %s delivered", txn).isTrue();
            }
        }
    }

    private static Object publishedAt(JdbcClient db, UUID txn) {
        return db.sql("SELECT published_at FROM outbox WHERE message_key = :k").param("k", txn.toString())
                .query((rs, n) -> rs.getTimestamp(1)).list().get(0);
    }
}
