# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X 'hros-event-worker/cmd.Version=1.0.0' -X 'hros-event-worker/cmd.BuildDate=$(date -u +%Y-%m-%d)'" \
    -o /bin/hros-event-worker main.go

# Stage 2: Final Minimal Image
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/hros-event-worker /usr/local/bin/hros-event-worker

USER nobody:nobody

ENTRYPOINT ["/usr/local/bin/hros-event-worker"]
CMD ["run", "--help"]
