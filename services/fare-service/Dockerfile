# fare-service image. The shared infrastructure/docker/Dockerfile.service only knows how to
# `go build`, so the Java service carries its own two-stage build.

# --- build: Maven + Temurin 21 JDK -------------------------------------------------------
# Tests are not run here; `./mvnw -B verify` (locally and in the fare-service CI job) is the
# test gate. The image build only compiles and packages the jar.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src

# Resolve dependencies in their own layer so source edits do not re-download them.
COPY pom.xml ./
RUN mvn -B --no-transfer-progress dependency:go-offline

COPY src ./src
RUN mvn -B --no-transfer-progress -DskipTests package \
    && mkdir -p /out \
    && cp target/fare-service-*.jar /out/fare-service.jar

# --- run: distroless Java 21, non-root -----------------------------------------------------
# Same family as the Go services' distroless/static:nonroot base: no shell, no package manager,
# and the process runs as the unprivileged `nonroot` user.
FROM gcr.io/distroless/java21-debian12:nonroot
WORKDIR /app
COPY --from=build /out/fare-service.jar /app/fare-service.jar
USER nonroot:nonroot
EXPOSE 8087
ENTRYPOINT ["/usr/bin/java", "-jar", "/app/fare-service.jar"]
