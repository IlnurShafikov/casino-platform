# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY shared/ shared/
COPY game-service/ game-service/

RUN go work init ./shared ./game-service

WORKDIR /src/game-service

ENV CGO_ENABLED=0

RUN go build -o /out/server ./cmd

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/server /app/server

EXPOSE 8084

ENTRYPOINT ["/app/server"]