FROM golang:1.20 AS build

WORKDIR /module

ENV GOPROXY=direct

COPY go.mod go.sum ./

RUN go mod download

COPY redismodule redismodule
COPY pkg pkg
COPY cmd cmd

RUN go build -v -buildmode=c-shared github.com/kuzznya/go-redis-search-replica/cmd/redismodule/ftsindex
RUN chmod +x ftsindex

FROM redis:7.0

ENV LD_LIBRARY_PATH /usr/lib/redis/modules

COPY --from=build /module/ftsindex ${LD_LIBRARY_PATH}/

ENTRYPOINT ["redis-server"]
CMD ["--loadmodule", "/usr/lib/redis/modules/ftsindex", "--loglevel", "debug"]
