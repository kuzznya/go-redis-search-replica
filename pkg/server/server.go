package server

import (
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/kuzznya/go-redis-search-replica/pkg/parser"
	"github.com/kuzznya/go-redis-search-replica/pkg/search"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"
	"strconv"
	"strings"
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

func (s server) handle(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		_ = conn.Close()
	}()

	data := cmd.Args
	text := make([]string, len(data))
	for i, arg := range data {
		text[i] = string(arg)
	}
	cmdText := strings.Join(text, " ")
	log.Infof("Received cmd: %s", cmdText)
	lexer := parser.NewFTLexer(antlr.NewInputStream(cmdText))
	stream := antlr.NewCommonTokenStream(lexer, 0)
	p := parser.NewFTParser(stream)
	p.AddErrorListener(antlr.NewDiagnosticErrorListener(true))
	root := p.Root()

	ftCreate := newFtCreateListener(s.engine, conn)
	antlr.NewParseTreeWalker().Walk(ftCreate, root)
	if ftCreate.err != nil {
		conn.WriteError(ftCreate.err.Error())
		return
	}

	ftSearch := newFtSearchListener(s.engine, conn)
	antlr.NewParseTreeWalker().Walk(ftSearch, root)
	if ftSearch.err != nil {
		conn.WriteError(ftSearch.err.Error())
		return
	}
}

type ftCreateListener struct {
	*parser.BaseFTParserListener
	engine search.Engine
	conn   redcon.Conn
	err    error
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
			l.err = errors.Wrap(err, "failed to parse prefix count")
			return
		}
		if len(ctx.Prefix_part().AllPrefix()) != prefixCount {
			l.err = errors.Errorf("invalid prefix count (expected %d, actual %d)", prefixCount, len(ctx.Prefix_part().AllPrefix()))
			return
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
	err    error
}

func newFtSearchListener(engine search.Engine, conn redcon.Conn) *ftSearchListener {
	return &ftSearchListener{engine: engine, conn: conn}
}

func (l *ftSearchListener) ExitFt_search(ctx *parser.Ft_searchContext) {
	index := ctx.Index().GetText()
	iter, err := l.engine.Search(index, ctx.Query())
	if err != nil {
		l.err = err
		return
	}

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
		l.conn.WriteString(doc.Key)
		l.conn.WriteArray(len(doc.Hash) * 2)
		for k, v := range doc.Hash {
			l.conn.WriteString(k)
			l.conn.WriteString(string(v))
		}
	}
}
