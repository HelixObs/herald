FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /gateway ./cmd

FROM alpine:3.20
RUN apk add --no-cache wget
COPY --from=builder /gateway /gateway
ENTRYPOINT ["/gateway"]
