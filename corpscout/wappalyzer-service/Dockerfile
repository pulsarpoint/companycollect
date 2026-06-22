FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /wappalyzer-service .

FROM gcr.io/distroless/static-debian12
COPY --from=build /wappalyzer-service /wappalyzer-service
ENV WAPPALYZER_ADDR=:9876
EXPOSE 9876
ENTRYPOINT ["/wappalyzer-service"]
