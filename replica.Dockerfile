FROM golang:1.20 AS build

WORKDIR /app

ENV GOPROXY=direct

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
