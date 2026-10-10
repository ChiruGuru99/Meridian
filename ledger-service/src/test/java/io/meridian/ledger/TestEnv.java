package io.meridian.ledger;

import io.meridian.ledger.Model.AccountType;
import io.meridian.ledger.Model.CurrencyLimit;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.List;
import java.util.UUID;

/**
 * One real Postgres (Testcontainers) and one application context shared by every test class,
 * JUnit and jqwik alike. The ledger is append-only, so tests never clean up: they use fresh
 * account ids, and global invariant checks must hold across everything all tests wrote.
 */
public final class TestEnv {
    private static PostgreSQLContainer pg;
    private static ConfigurableApplicationContext ctx;

    private TestEnv() {}

    public static synchronized ConfigurableApplicationContext context() {
        if (ctx == null) {
            pg = new PostgreSQLContainer("postgres:17-alpine")
                    .withDatabaseName("ledger")
                    .withInitScript("test-roles.sql");
            pg.start();
            ctx = new SpringApplicationBuilder(LedgerApplication.class)
                    .web(WebApplicationType.NONE)
                    .run(
                            "--spring.datasource.url=" + pg.getJdbcUrl(),
                            "--spring.datasource.username=ledger_app",
                            "--spring.datasource.password=ledger_app",
                            "--spring.datasource.hikari.maximum-pool-size=20",
                            "--spring.flyway.user=" + pg.getUsername(),
                            "--spring.flyway.password=" + pg.getPassword(),
                            "--ledger.grpc.port=0",
                            "--ledger.outbox.poll-ms=3600000",
                            "--spring.kafka.bootstrap-servers=localhost:1");
        }
        return ctx;
    }

    public static <T> T bean(Class<T> type) {
        return context().getBean(type);
    }

    public static LedgerService ledger() {
        return bean(LedgerService.class);
    }

    /** JDBC as the application role (no UPDATE/DELETE grants on the ledger). */
    public static JdbcClient app() {
        return bean(JdbcClient.class);
    }

    /** JDBC as the schema owner / superuser, to prove the database itself refuses bad writes. */
    public static JdbcClient owner() {
        context();
        return JdbcClient.create(new DriverManagerDataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword()));
    }

    public static String newAccount(String name, Long overdraftLimit, String... currencies) {
        String id = "t_" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);
        List<CurrencyLimit> limits = java.util.Arrays.stream(currencies).map(c -> new CurrencyLimit(c, overdraftLimit)).toList();
        ledger().createAccount(id, name, AccountType.LIABILITY, limits);
        return id;
    }

    public static String key() {
        return "test:" + UUID.randomUUID();
    }

    /** Credits a customer account from the nostro (simulated inbound funds). */
    public static void fund(String accountId, String currency, long amount) {
        ledger().post(new Model.PostCommand(key(), "DEPOSIT", "test funding", List.of(
                new Model.PostingLine("SYS-NOSTRO", currency, amount),
                new Model.PostingLine(accountId, currency, -amount)), null));
    }

    public static long balance(String accountId, String currency) {
        return ledger().getAccount(accountId).balances().stream()
                .filter(b -> b.currency().equals(currency)).findFirst().orElseThrow().balance();
    }
}
