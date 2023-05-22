package server

import (
	"fmt"
	"github.com/kuzznya/go-redis-search-replica/pkg/search"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

const host = "0.0.0.0"

func StartServer(engine search.Engine, port int) {
	addr := fmt.Sprintf("%s:%d", host, port)
	log.Infof("Starting server on %s", addr)
	err := redcon.ListenAndServe(
		addr,
		server{engine: engine}.handle,
		func(c redcon.Conn) bool { return true },
		func(c redcon.Conn, err error) {
			if err != nil {
				log.WithError(err).Warnln("Connection closed")
			} else {
				log.Debugln("Connection closed")
			}
		},
	)
	if err != nil {
		log.WithError(err).Panicln("Failed to run server")
	}
}

type server struct {
	engine search.Engine
}

var memprof *os.File
var cpuprof *os.File

func (s server) handle(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		err := recover()
		if err != nil {
			conn.WriteError(fmt.Sprintf("%s", err))
			if e, ok := err.(error); ok {
				log.WithError(e).Errorln("Failed to process the command")
			}
		}
	}()

	if len(cmd.Args) == 0 {
		return
	}

	data := cmd.Args

	args := make([]string, len(data))
	for i, arg := range data {
		args[i] = string(arg)
	}

	cmdName := strings.ToLower(args[0])
	switch cmdName {
	case "ft.search":
		s.handleFtSearch(conn, args[1:])
		return
	case "quit":
		conn.WriteString("OK")
		_ = conn.Close()
		return
	case "command":
		handleCommandDocs(conn, args[1:])
		return
	case "pprof":
		handlePprof(conn, args[1:])
		return
	}
	conn.WriteError("Unknown command")
}

func (s server) handleFtSearch(conn redcon.Conn, args []string) {
	if len(args) < 2 {
		conn.WriteError("Wrong number of arguments provided")
		return
	}
	index := args[0]
	query := args[1]

	pos := 1
	next := func() (string, bool) {
		if pos+1 < len(args) {
			pos++
			return args[pos], true
		}
		return "", false
	}

	var limit *search.Limit

	for {
		arg, ok := next()
		if !ok {
			break
		}
		arg = strings.ToLower(arg)
		switch arg {
		case "limit":
			offsetStr, ok := next()
			if !ok {
				conn.WriteError("LIMIT requires two numeric arguments")
				return
			}
			offset, err := strconv.Atoi(offsetStr)
			if err != nil {
				conn.WriteError("LIMIT requires two numeric arguments")
				return
			}
			numStr, ok := next()
			if !ok {
				conn.WriteError("LIMIT requires two numeric arguments")
				return
			}
			num, err := strconv.Atoi(numStr)
			if err != nil {
				conn.WriteError("LIMIT requires two numeric arguments")
				return
			}
			limit = &search.Limit{Offset: offset, Num: num}
		default:
			conn.WriteError(fmt.Sprintf("Unknown argument '%s'", arg))
			return
		}
	}

	start := time.Now()
	iter, err := s.engine.Search(index, query, limit)
	if err != nil {
		panic(err)
	}

	log.Debugf("Query finished in %s", time.Now().Sub(start))

	docs := make([]*storage.Document, 0)
	for {
		occ, _, ok := iter.Next()
		if !ok {
			break
		}
		docs = append(docs, occ.Doc)
	}

	conn.WriteArray(len(docs)*2 + 1)
	conn.WriteInt(len(docs))
	for _, doc := range docs {
		conn.WriteAny(doc)
	}
}

func handleCommandDocs(conn redcon.Conn, args []string) {
	if len(args) > 0 && strings.ToLower(args[0]) == "docs" {
		if len(args) > 1 {
			commandDocs(conn, args[1:])
		} else {
			commandDocs(conn, nil)
		}
	} else {
		conn.WriteError("Unknown command")
	}
}

func handlePprof(conn redcon.Conn, args []string) {
	if len(args) != 1 {
		conn.WriteError("Either 'pprof start' or 'pprof end' is supported")
	}
	switch strings.ToLower(args[0]) {
	case "start":
		if memprof != nil {
			conn.WriteError("Already in progress")
			return
		}
		memprof, _ = os.Create("mem.pprof")
		cpuprof, _ = os.Create("cpu.pprof")
		_ = pprof.StartCPUProfile(cpuprof)
		conn.WriteString("OK")
	case "end":
		if memprof == nil {
			conn.WriteError("No pprof in progress")
			return
		}
		_ = pprof.WriteHeapProfile(memprof)
		_ = memprof.Close()
		memprof = nil
		pprof.StopCPUProfile()
		_ = cpuprof.Close()
		cpuprof = nil
		conn.WriteString("OK")
	default:
		conn.WriteError("Either 'pprof start' or 'pprof end' is supported")
	}
}

func commandDocs(conn redcon.Conn, cmds []string) {
	if cmds != nil && (len(cmds) > 1 || strings.ToLower(cmds[1]) != "ft.search") {
		conn.WriteError("Unknown subcommand(s)")
		return
	}
	ftSearchDocs := map[string]any{
		"summary": "Search the index with a textual query",
		"arguments": []any{
			commandArg("index", "key", nil),
			commandArg("...options...", "string", nil),
		},
	}
	conn.WriteAny([]any{"FT.SEARCH", ftSearchDocs})
}

func commandArg(name string, argType string, flags []string, args ...map[string]any) map[string]any {
	a := map[string]any{
		"name": name,
		"type": argType,
	}
	if flags != nil && len(flags) > 0 {
		a["flags"] = flags
	}
	if len(args) > 0 {
		a["arguments"] = args
	}
	return a
}
