# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY shared/ shared/
COPY auth-service/ auth-service/

RUN go work init ./shared ./auth-service

WORKDIR /src/auth-service

ENV CGO_ENABLED=0

RUN go build -o /out/server ./cmd

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/server /app/server

EXPOSE 8083

ENTRYPOINT ["/app/server"]