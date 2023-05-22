package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/kuzznya/go-redis-search-replica/pkg/exec"
	"github.com/kuzznya/go-redis-search-replica/pkg/rdb"
	"github.com/kuzznya/go-redis-search-replica/pkg/resp"
	"github.com/kuzznya/go-redis-search-replica/pkg/search"
	"github.com/kuzznya/go-redis-search-replica/pkg/server"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	var logLevel string
	flag.StringVar(&logLevel, "log", "", "--log warn - set log level to warn")
	var masterUrl string
	flag.StringVar(&masterUrl, "replicaof", "",
		"--replicaof localhost:6379 - set master url to localhost:6379")
	var port int
	flag.IntVar(&port, "port", -1, "--port 6379 - set replica listening port to 6379")
	flag.Parse()
	if logLevel == "" {
		logLevel = os.Getenv("LOG_LEVEL")
	}
	if logLevel == "" {
		logLevel = "info"
	}
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.WithError(err).Panicln("Failed to parse log level")
	}
	log.SetLevel(level)

	if masterUrl == "" {
		masterUrl = os.Getenv("REPLICAOF")
	}
	if masterUrl == "" {
		masterUrl = "localhost:6379"
	}

	if port == -1 && os.Getenv("PORT") != "" {
		port, err = strconv.Atoi(os.Getenv("PORT"))
		if err != nil {
			log.WithError(err).Panicln("Failed to parse port from environment variable PORT")
		}
	}
	if port == -1 {
		port = 16379
	}

	s := storage.New()
	engine := search.NewEngine(s)
	e := exec.New(s, engine)

	dialTimeout := 30 * time.Second
	conn, err := createMasterConn(masterUrl, dialTimeout)
	if err != nil {
		log.WithError(err).Panicln("Failed to connect to Redis")
	}
	err = conn.SetKeepAlive(true)
	if err != nil {
		log.WithError(err).Panicln("Failed to make connection keep-alive")
	}

	c := redis.NewClient(&redis.Options{
		MaxRetries: 1,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return conn, nil
		},
		PoolSize:        1,
		ConnMaxIdleTime: -1, // disable idle timeout check
	})

	masterId, offset := initSync(c)
	log.Infof("Redis PSYNC response received - masterId: %s, offset: %d", masterId, offset)

	err = conn.SetReadDeadline(time.Now().Add(1 * time.Hour))
	if err != nil {
		log.WithError(err).Panicln("Failed to set read deadline for connection")
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	go func() {
		for {
			// NB: We report offset - 1 so that replica is never in full sync from the master POV,
			// so master never tries to failover to this node
			ackOffset := offset - 1
			if ackOffset < 0 {
				ackOffset = 0
			}
			replconfAck(writer, conn, ackOffset)
			time.Sleep(1 * time.Second)
		}
	}()

	rdbReadStart := time.Now()

	rdbLen := readRdb(reader, e)
	log.Infof("RDB content received successfully (%d bytes) in %s", rdbLen, time.Now().Sub(rdbReadStart).String())

	go server.StartServer(engine, port) // TODO: 03/05/2023 check if it is better to start server in the beginning or here

	parser := resp.NewParser(reader)
	for {
		err = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		if err != nil {
			log.WithError(err).Panicln("Failed to set read deadline for connection")
		}

		cmd, read, err := parser.ParseCmd()
		if err != nil {
			if err == io.EOF {
				log.Panicln("Socket was closed")
			}
			log.WithError(err).Panicln("Error while reading replication data")
		}

		if cmd != nil {
			log.Infof("Cmd: %s", cmd.Name())
			log.Debugf("Cmd args: %+v", cmd)
			err := e.Exec(cmd)
			if err != nil {
				log.WithError(err).Panicln("Failed to execute command")
			}
		}

		offset += read
	}
}

func createMasterConn(masterUrl string, dialTimeout time.Duration) (*net.TCPConn, error) {
	conn, err := net.DialTimeout("tcp", masterUrl, dialTimeout)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		return tcpConn, nil
	} else {
		return nil, errors.New("Connection is not TCPConn")
	}
}

func initSync(c *redis.Client) (masterId string, offset uint64) {
	execReplconf(c)

	psync := redis.NewCmd(context.TODO(), "PSYNC", "0", "0")
	err := c.Process(context.TODO(), psync)
	if err != nil {
		log.WithError(err).Panicln("Failed to run PSYNC command")
	}
	res := psync.Val()

	log.Infof("Redis response: %s", res)

	if strResp, ok := res.(string); ok {
		parts := strings.Split(strResp, " ")
		if len(parts) != 3 {
			log.Panicf("Redis PSYNC response '%s' cannot be splitted in 3 parts", strResp)
		}
		if strings.ToUpper(parts[0]) != "FULLRESYNC" {
			log.Panicf("Redis PSYNC response '%s' is not FULLRESYNC", strResp)
		}
		masterId = parts[1]
		offset, err = strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			log.Panicf("Redis PSYNC response '%s' invalid: failed to parse offset", strResp)
		}
	} else {
		log.Panicln("Redis PSYNC response is not string")
	}
	return
}

func execReplconf(c *redis.Client) {
	replconf := redis.NewCmd(context.TODO(), "REPLCONF", "ip-address", "127.0.0.2", "listening-port", "6379")
	err := c.Process(context.TODO(), replconf)
	if err != nil {
		log.WithError(err).Panicln("Failed to run REPLCONF command")
	}
	if strings.ToLower(replconf.Val().(string)) != "ok" {
		log.Panicln("Unknown response to REPLCONF: %s", replconf.Val())
	}
}

func replconfAck(writer *bufio.Writer, conn *net.TCPConn, offset uint64) {
	ack := fmt.Sprintf("REPLCONF ACK %d\n", offset)
	log.Tracef("Ack: %s", ack)

	_, err := writer.Write([]byte(ack))
	if err != nil {
		log.WithError(err).Panicln("Failed to REPLCONF ACK master")
	}

	err = conn.SetWriteDeadline(time.Now().Add(1 * time.Second))

	if err != nil {
		log.WithError(err).Panicln("Failed to set write deadline for connection")
	}

	err = writer.Flush()
	if err != nil {
		log.WithError(err).Panicln("Failed to flush write buffer")
	}
}

func readRdb(reader *bufio.Reader, e exec.Executor) uint64 {
	var line []byte
	var err error
	for {
		line, _, err = reader.ReadLine()
		if err != nil {
			log.WithError(err).Panicln("Failed to read RDB")
		}
		if len(line) > 0 {
			break
		}
	}
	if line[0] != '$' {
		log.Panicf("RDB content should start with $<len>, but received '%s'", line)
	}
	rdbLen, err := strconv.ParseUint(string(line[1:]), 10, 64)
	if err != nil {
		log.WithError(err).Panicln("Failed to parse RDB size")
	}

	err = rdb.Parse(reader, e)
	if err != nil {
		log.WithError(err).Panicf("Failed to parse RDB: %+v", err)
	}

	return rdbLen
}
