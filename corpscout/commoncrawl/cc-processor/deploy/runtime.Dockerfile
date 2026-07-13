FROM --platform=$BUILDPLATFORM golang:1.26.1-bookworm AS build

ARG TARGETARCH

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
       gcc-x86-64-linux-gnu \
       g++-x86-64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY cc-raw ./cc-raw
COPY cc-enrich-worker ./cc-enrich-worker

RUN mkdir /out \
    && cd /src/cc-enrich-worker \
    && CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ \
       CGO_ENABLED=1 GOOS=linux GOARCH="${TARGETARCH}" \
       go build -buildvcs=false -trimpath -o /out/cc-enrich-worker ./cmd/cc-enrich-worker

FROM scratch AS artifacts
COPY --from=build /out/ /
