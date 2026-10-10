# Build context: repository root (the build needs ../proto).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY proto proto
COPY ledger-service/pom.xml ledger-service/pom.xml
COPY ledger-service/src ledger-service/src
RUN --mount=type=cache,target=/root/.m2 cd ledger-service && mvn -B -q -DskipTests package

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 app
WORKDIR /app
COPY --from=build /src/ledger-service/target/ledger-service-1.0.0.jar app.jar
COPY --from=build /src/ledger-service/target/opentelemetry-javaagent.jar otel-javaagent.jar
USER app
EXPOSE 8081 9091
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -javaagent:/app/otel-javaagent.jar"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
