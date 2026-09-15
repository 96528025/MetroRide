FROM golang:1.22-alpine AS build
WORKDIR /src
COPY tests/routingfixture/main.go .
RUN CGO_ENABLED=0 go build -o /route-fixture main.go
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /route-fixture /route-fixture
ENTRYPOINT ["/route-fixture"]
