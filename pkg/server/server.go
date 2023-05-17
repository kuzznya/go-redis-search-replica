package server

import (
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/kuzznya/go-redis-search-replica/pkg/parser"
	"github.com/kuzznya/go-redis-search-replica/pkg/search"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

const host = "0.0.0.0"
const port = "16379"

func StartServer(engine search.Engine) {
	addr := host + ":" + port
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

	text := make([]string, len(data))
	for i, arg := range data {
		text[i] = string(arg)
	}

	cmdName := strings.ToLower(text[0])
	switch cmdName {
	case "quit":
		conn.WriteString("OK")
		_ = conn.Close()
		return
	case "command":
		if len(text) > 1 && strings.ToLower(text[1]) == "docs" {
			if len(text) > 2 {
				commandDocs(conn, text[2:])
			} else {
				commandDocs(conn, nil)
			}
		} else {
			conn.WriteError("Unknown command")
		}
		return

	case "pprof":
		if len(text) == 1 {
			conn.WriteError("Either 'pprof start' or 'pprof end' is supported")
		}
		switch strings.ToLower(text[1]) {
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
		return
	}

	cmdText := strings.Join(text, " ")
	log.Infof("Received cmd: %s", cmdText)

	lexerErrors := сustomErrorListener{}
	parserErrors := сustomErrorListener{}

	lexer := parser.NewFTLexer(antlr.NewInputStream(cmdText))
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(&lexerErrors)

	stream := antlr.NewCommonTokenStream(lexer, 0)

	p := parser.NewFTParser(stream)
	p.RemoveErrorListeners()
	p.AddErrorListener(&parserErrors)

	root := p.Root()

	ftCreate := newFtCreateListener(s.engine, conn)
	antlr.NewParseTreeWalker().Walk(ftCreate, root)

	ftSearch := newFtSearchListener(s.engine, conn)
	antlr.NewParseTreeWalker().Walk(ftSearch, root)
}

type ftCreateListener struct {
	*parser.BaseFTParserListener
	engine search.Engine
	conn   redcon.Conn
}

func newFtCreateListener(engine search.Engine, conn redcon.Conn) *ftCreateListener {
	return &ftCreateListener{engine: engine, conn: conn}
}

func (l *ftCreateListener) ExitFt_create(ctx *parser.Ft_createContext) {
	name := ctx.Index().GetText()

	var prefixes []string
	if ctx.Prefix_part() != nil {
		prefixCount, err := strconv.Atoi(ctx.Prefix_part().Integral().GetText())
		if err != nil {
			panic(errors.Wrap(err, "failed to parse prefix count"))
		}
		if len(ctx.Prefix_part().AllPrefix()) != prefixCount {
			panic(errors.Errorf("invalid prefix count (expected %d, actual %d)", prefixCount, len(ctx.Prefix_part().AllPrefix())))
		}

		prefixes = make([]string, prefixCount)
		for i, prefix := range ctx.Prefix_part().AllPrefix() {
			prefixes[i] = prefix.GetText()
		}
	} else {
		prefixes = []string{"*"}
	}

	fields := make([]string, len(ctx.AllField_spec()))
	for i, fieldSpec := range ctx.AllField_spec() {
		fieldName := fieldSpec.Field_name().GetText()
		fieldType := fieldSpec.Field_type().GetText()
		if strings.ToLower(fieldType) != "text" {
			log.Errorf("Unknown field type %s", fieldType)
		}
		fields[i] = fieldName
	}

	l.engine.CreateIndex(name, prefixes, fields)

	log.Infof("Index %s created (prefixes %s, fields %v)", name, prefixes, fields)

	l.conn.WriteString("OK")
}

type ftSearchListener struct {
	*parser.BaseFTParserListener
	engine search.Engine
	conn   redcon.Conn
}

func newFtSearchListener(engine search.Engine, conn redcon.Conn) *ftSearchListener {
	return &ftSearchListener{engine: engine, conn: conn}
}

func (l *ftSearchListener) ExitFt_search(ctx *parser.Ft_searchContext) {
	index := ctx.Index().GetText()

	limitPart := ctx.Limit_part()

	start := time.Now()
	iter, err := l.engine.Search(index, ctx.Query(), limitPart)
	if err != nil {
		panic(err)
	}

	log.Infof("Query finished in %s", time.Now().Sub(start))

	docs := make([]*storage.Document, 0)
	for {
		occ, _, ok := iter.Next()
		if !ok {
			break
		}
		docs = append(docs, occ.Doc)
	}

	l.conn.WriteArray(len(docs)*2 + 1)
	l.conn.WriteInt(len(docs))
	for _, doc := range docs {
		l.conn.WriteAny(doc)
	}
}

func commandDocs(conn redcon.Conn, cmds []string) {
	// TODO: 05/05/2023 filter by cmds
	ftCreateDocs := map[string]any{
		"summary": "Create an index with the given specification",
		"arguments": []any{
			commandArg("index", "key", nil),
			commandArg("...options...", "string", nil),
		},
	}
	ftSearchDocs := map[string]any{
		"summary": "Search the index with a textual query",
		"arguments": []any{
			commandArg("index", "key", nil),
			commandArg("...options...", "string", nil),
		},
	}
	conn.WriteAny([]any{"FT.CREATE", ftCreateDocs, "FT.SEARCH", ftSearchDocs})
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

type сustomSyntaxError struct {
	line, column int
	msg          string
}

func (c сustomSyntaxError) Error() string {
	return fmt.Sprintf("Error at position %d: %s", c.column, c.msg)
}

type сustomErrorListener struct {
	*antlr.DefaultErrorListener // Embed default which ensures we fit the interface
}

func (c *сustomErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	err := сustomSyntaxError{
		line:   line,
		column: column,
		msg:    msg,
	}
	panic(err)
}
