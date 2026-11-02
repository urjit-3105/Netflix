# ==============================================================================
# Multi-Stage Production Dockerfile for Netflix Resilience Microservice
# ==============================================================================

# Stage 1: Builder
FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /workspace/app

# Cache dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Build application
COPY src src
RUN mvn clean package -DskipTests

# Stage 2: Production Runtime
FROM eclipse-temurin:17-jre-jammy
LABEL maintainer="Netflix Resilience Team"
LABEL description="High-Throughput Netflix Distributed Resilience Microservice"

WORKDIR /app

# Run as dedicated unprivileged user for security
RUN groupadd -r spring && useradd -r -g spring spring
USER spring:spring

# Copy built JAR from builder stage
COPY --from=builder --chown=spring:spring /workspace/app/target/*.jar app.jar

# JVM Performance & Container Optimizations
ENV JAVA_OPTS="-XX:+UseG1GC -XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0 -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
