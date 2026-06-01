FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOWORK=off CGO_ENABLED=0 go build -o /brreg-financial-service ./cmd/brreg-financial-service

FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget
COPY --from=builder /brreg-financial-service /brreg-financial-service
ENTRYPOINT ["/brreg-financial-service"]
