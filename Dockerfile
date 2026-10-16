FROM golang:1.25-alpine AS build
WORKDIR /src

# Download dependencies first so they cache independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# .dockerignore strips .git, so build info is passed in rather than derived.
ARG TAG=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

# CGO_ENABLED=0 for the distroless static base below: every dependency is pure
# Go, pgx included. GOEXPERIMENT=jsonv2: mautrix's federation/pdu (hashing,
# signing, event IDs) is built only with it.
RUN CGO_ENABLED=0 GOEXPERIMENT=jsonv2 go build -trimpath \
      -ldflags="-s -w \
        -X main.tag=${TAG} \
        -X main.commit=${COMMIT} \
        -X main.buildTime=${BUILD_TIME}" \
      -o /out/goevent-creator ./cmd/goevent-creator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/goevent-creator /goevent-creator
ENTRYPOINT ["/goevent-creator"]
CMD ["-config", "/data/goevent-creator.yaml"]
