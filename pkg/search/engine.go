package search

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"github.com/blevesearch/go-porterstemmer"
	"github.com/emirpasic/gods/stacks"
	"github.com/emirpasic/gods/stacks/arraystack"
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"github.com/kuzznya/go-redis-search-replica/pkg/parser"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"math"
	"sync"
	"time"
)

const indexAsync = false

type Engine struct {
	s       storage.Storage
	indexes map[string]*index.FTSIndex
	mu      *sync.RWMutex
}

func NewEngine(s storage.Storage) Engine {
	e := Engine{s: s, indexes: make(map[string]*index.FTSIndex), mu: &sync.RWMutex{}}

	newDocs := make(chan *storage.Document)

	s.OnSave(func(d *storage.Document) {
		if indexAsync {
			newDocs <- d
		} else {
			e.Add(d)
		}
	})

	deletedDocs := make(chan *storage.Document)

	s.OnDelete(func(d *storage.Document) {
		deletedDocs <- d
	})

	go func() {
		for {
			_ = <-deletedDocs
			// TODO 20.03.2023 implement GC
		}
	}()

	return e
}

func (e Engine) CreateIndex(name string, prefixes []string, fields []string) {
	idx := index.NewFTSIndex(prefixes, fields)

	e.mu.Lock()
	e.indexes[name] = idx
	e.mu.Unlock()

	log.Infof("Created index %s", name)
	start := time.Now()

	go func() {
		docs := e.s.GetAll(prefixes)
		idx.Load(docs)
		log.Infof("Index %s creation finished in %s", name, time.Now().Sub(start))
	}()
}

func (e Engine) DeleteIndex(name string) {
	e.mu.RLock()
	if i, ok := e.indexes[name]; ok {
		i.MarkDeleted()
	} else {
		return
	}
	e.mu.RUnlock()
	e.mu.Lock()
	delete(e.indexes, name)
	e.mu.Unlock()
}

func (e Engine) Add(d *storage.Document) {
	for _, idx := range e.indexes {
		idx.Add(d)
	}
}

type Limit struct {
	Offset int
	Num    int
}

func (e Engine) Search(idxName string, query string, limit *Limit) (iter index.TermIterator, err error) {
	e.mu.RLock()
	idx, found := e.indexes[idxName]
	e.mu.RUnlock()
	if !found {
		return nil, errors.Errorf("Index %s not found", idxName)
	}

	ftSearch := newQueryListener(idx)

	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if e, ok := r.(error); ok {
			err = e
		} else {
			err = errors.Errorf("%s", e)
		}
	}()

	lexerErrors := сustomErrorListener{}
	parserErrors := сustomErrorListener{}

	lexer := parser.NewQueryLexer(antlr.NewInputStream(query))
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(&lexerErrors)

	stream := antlr.NewCommonTokenStream(lexer, 0)

	p := parser.NewQueryParser(stream)
	p.RemoveErrorListeners()
	p.AddErrorListener(&parserErrors)

	queryCtx := p.Query()

	antlr.NewParseTreeWalker().Walk(ftSearch, queryCtx)

	iter, ok := ftSearch.pop()
	if !ok {
		return nil, errors.New("failed to parse query")
	}

	if limit != nil {
		iter = TopN(limit.Offset, limit.Num, iter)
	} else {
		iter = TopN(0, math.MaxInt, iter)
	}

	return iter, nil
}

type queryListener struct {
	*parser.BaseQueryParserListener
	idx   *index.FTSIndex
	stack stacks.Stack
}

func newQueryListener(idx *index.FTSIndex) *queryListener {
	return &queryListener{idx: idx, stack: arraystack.New()}
}

func (l *queryListener) ExitWord(ctx *parser.WordContext) {
	word := ctx.GetText()
	term := porterstemmer.StemString(word)
	l.stack.Push(l.idx.Read(term))
}

func (l *queryListener) ExitExact_match(ctx *parser.Exact_matchContext) {
	panic(errors.New("not implemented"))
}

func (l *queryListener) ExitField_query_part(ctx *parser.Field_query_partContext) {
	panic(errors.New("not implemented"))
}

func (l *queryListener) ExitQuery_part(ctx *parser.Query_partContext) {
	if ctx.Non_union_query_part() != nil {
		return
	}

	if ctx.OR() != nil {
		ok := l.union()
		if !ok {
			panic(errors.Errorf("Failed to parse query part: %s", ctx.GetText()))
		}
		return
	}

	ok := l.intersect()
	if !ok {
		panic(errors.Errorf("Failed to parse query part: %s", ctx.GetText()))
	}
	return
}

func (l *queryListener) union() bool {
	iter2, ok := l.pop()
	if !ok {
		return false
	}

	iter1, ok := l.pop()
	if !ok {
		return false
	}

	l.stack.Push(Union(iter1, iter2))
	return true
}

func (l *queryListener) intersect() bool {
	iter2, ok := l.pop()
	if !ok {
		return false
	}

	iter1, ok := l.pop()
	if !ok {
		return false
	}

	iter1 = Intersect(iter1, iter2)

	l.stack.Push(iter1)
	return true
}

func (l *queryListener) pop() (index.TermIterator, bool) {
	v, ok := l.stack.Pop()
	if !ok {
		return nil, false
	}
	if v == nil {
		return nil, false
	}
	return v.(index.TermIterator), true
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
