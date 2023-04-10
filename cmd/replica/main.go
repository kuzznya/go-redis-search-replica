package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/kuzznya/go-redis-search-replica/pkg/exec"
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"github.com/kuzznya/go-redis-search-replica/pkg/rdb"
	"github.com/kuzznya/go-redis-search-replica/pkg/resp"
	"github.com/kuzznya/go-redis-search-replica/pkg/search"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const indexAsync = false

func main() {
	log.SetLevel(log.InfoLevel)

	idx := index.NewFTSIndex([]string{"*"}, []string{"headline", "short_description"})

	newDocs := make(chan *storage.Document)
	deletedDocs := make(chan *storage.Document)
	go func() {
		for {
			doc := <-newDocs
			idx.Add(doc)
		}
	}()

	go func() {
		for {
			_ = <-deletedDocs
			// TODO 20.03.2023 implement GC
		}
	}()

	indexUpdate := func(d *storage.Document) {
		if indexAsync {
			newDocs <- d
		} else {
			idx.Add(d)
		}
	}
	gcFunc := func(d *storage.Document) {
		deletedDocs <- d
	}

	s := storage.New(indexUpdate, gcFunc)
	e := exec.New(s)

	dialTimeout := 30 * time.Second
	conn, err := createConn(dialTimeout)
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
		PoolSize:    1,
		IdleTimeout: -1, // disable idle timeout check
	})

	execReplconf(c)

	psync := redis.NewCmd(context.TODO(), "PSYNC", "0", "0")
	err = c.Process(context.TODO(), psync)
	if err != nil {
		log.WithError(err).Panicln("Failed to run PSYNC command")
	}
	res := psync.Val()

	log.Infof("Redis response: %s", res)

	var masterId string
	var offset int
	if strResp, ok := res.(string); ok {
		parts := strings.Split(strResp, " ")
		if len(parts) != 3 {
			log.Panicf("Redis PSYNC response '%s' cannot be splitted in 3 parts", strResp)
		}
		if strings.ToUpper(parts[0]) != "FULLRESYNC" {
			log.Panicf("Redis PSYNC response '%s' is not FULLRESYNC", strResp)
		}
		masterId = parts[1]
		offset, err = strconv.Atoi(parts[2])
		if err != nil {
			log.Panicf("Redis PSYNC response '%s' invalid: failed to parse offset", strResp)
		}
	} else {
		log.Panicln("Redis PSYNC response is not string")
	}

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

	var line []byte
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
	rdbLen, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		log.WithError(err).Panicln("Failed to parse RDB size")
	}

	err = rdb.Parse(reader, e)
	if err != nil {
		log.WithError(err).Panicf("Failed to parse RDB: %+v", err)
	}

	log.Infof("RDB content received successfully (%d bytes)", rdbLen)

	for len(newDocs) > 0 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(5 * time.Millisecond)

	//idx.PrintIndex()

	start := time.Now().UnixMicro()
	result := search.Intersect(idx.Read("spider"), idx.Read("man"))
	result = search.TopN(5, result)
	for {
		occ, score, ok := result.Next()
		//_, _, ok := result.Next()
		if !ok {
			break
		}
		if occ.Doc == nil {
			continue
		}
		log.Infof("Document %s, score %.6f", occ.Doc.Key, score)
	}
	log.Infof("Execution time: %d", time.Now().UnixMicro()-start)

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

func createConn(dialTimeout time.Duration) (*net.TCPConn, error) {
	conn, err := net.DialTimeout("tcp", "localhost:6379", dialTimeout)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		return tcpConn, nil
	} else {
		return nil, errors.New("Connection is not TCPConn")
	}
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

func replconfAck(writer *bufio.Writer, conn *net.TCPConn, offset int) {
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
