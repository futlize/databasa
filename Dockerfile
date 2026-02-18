FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/databasa ./cmd/databasa-server

FROM gcr.io/distroless/base-debian12

WORKDIR /app
COPY --from=builder /out/databasa /usr/local/bin/databasa
COPY databasa.toml /app/databasa.toml

VOLUME ["/app/data"]
EXPOSE 50051

ENTRYPOINT ["/usr/local/bin/databasa", "-config", "/app/databasa.toml"]
