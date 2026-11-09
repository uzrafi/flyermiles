# One image recipe for every service: docker build --build-arg MODULE=ledger-service .
# Expects the jar to be built first (./mvnw -DskipTests package), which keeps image builds fast.
FROM eclipse-temurin:21-jre

ARG MODULE
ARG OTEL_AGENT_VERSION=2.9.0

ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar /otel/opentelemetry-javaagent.jar
RUN chmod 644 /otel/opentelemetry-javaagent.jar && useradd --system --uid 1001 app

WORKDIR /app
COPY ${MODULE}/target/${MODULE}-*.jar /app/app.jar

USER 1001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError" \
    OTEL_JAVAAGENT_ENABLED=false \
    OTEL_METRICS_EXPORTER=otlp \
    OTEL_LOGS_EXPORTER=otlp \
    OTEL_TRACES_EXPORTER=otlp \
    OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
    OTEL_INSTRUMENTATION_MICROMETER_ENABLED=false

ENTRYPOINT ["java", "-javaagent:/otel/opentelemetry-javaagent.jar", "-jar", "/app/app.jar"]
