FROM golang:1.20 AS build

WORKDIR /app

ENV GOPROXY=direct

RUN apt update -y && apt install -y default-jre && \
    curl --create-dirs -O --output-dir /usr/local/lib https://www.antlr.org/download/antlr-4.13.0-complete.jar &&  \
    echo '#!/bin/sh' > /usr/local/bin/antlr && \
    echo 'java -jar /usr/local/lib/antlr-4.13.0-complete.jar "$@"' >> /usr/local/bin/antlr && \
    chmod +x /usr/local/bin/antlr

COPY go.mod go.sum ./

RUN go mod download

COPY cmd cmd
COPY pkg pkg

RUN go generate ./...
RUN go build -v github.com/kuzznya/go-redis-search-replica/cmd/replica

FROM debian:bullseye-slim

WORKDIR /app

COPY --from=build /app/replica .

ENTRYPOINT ["/app/replica"]
