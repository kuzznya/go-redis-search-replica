package search

import (
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/blevesearch/go-porterstemmer"
	"github.com/emirpasic/gods/stacks"
	"github.com/emirpasic/gods/stacks/arraystack"
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"github.com/kuzznya/go-redis-search-replica/pkg/parser"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"math"
	"strconv"
	"sync"
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

	go func() {
		docs := e.s.GetAll(prefixes)
		idx.Load(docs)
		log.Infof("Index %s creation finished", name)
	}()
}

func (e Engine) Add(d *storage.Document) {
	for _, idx := range e.indexes {
		idx.Add(d)
	}
}

func (e Engine) Search(idxName string, ctx parser.IQueryContext, limit parser.ILimit_partContext) (iter index.TermIterator, err error) {
	e.mu.RLock()
	idx, found := e.indexes[idxName]
	e.mu.RUnlock()
	if !found {
		return nil, errors.Errorf("Index %s not found", idxName)
	}

	ftSearch := newFtSearchListener(idx)

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

	antlr.NewParseTreeWalker().Walk(ftSearch, ctx) // TODO: 05/05/2023 handle panics maybe

	iter, ok := ftSearch.pop()
	if !ok {
		return nil, errors.New("failed to parse query")
	}

	if limit != nil {
		offsetStr := limit.Offset().GetText()
		offset, err := strconv.Atoi(offsetStr)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to parse offset: %s", offsetStr)
		}

		numStr := limit.Num().GetText()
		num, err := strconv.Atoi(numStr)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to parse limit: %s", offsetStr)
		}

		iter = TopN(offset, num, iter)
	} else {
		iter = TopN(0, math.MaxInt, iter)
	}

	return iter, nil
}

type ftSearchListener struct {
	*parser.BaseFTParserListener
	idx   *index.FTSIndex
	stack stacks.Stack
}

func newFtSearchListener(idx *index.FTSIndex) *ftSearchListener {
	return &ftSearchListener{idx: idx, stack: arraystack.New()}
}

func (l *ftSearchListener) ExitQuery(ctx *parser.QueryContext) {
	// TODO: 03/05/2023 ensure it can be no-op
}

func (l *ftSearchListener) ExitWord(ctx *parser.WordContext) {
	word := ctx.GetText()
	term := porterstemmer.StemString(word)
	l.stack.Push(l.idx.Read(term))
}

func (l *ftSearchListener) ExitExact_match(ctx *parser.Exact_matchContext) {
	panic("not implemented")
}

func (l *ftSearchListener) ExitField_query_part(ctx *parser.Field_query_partContext) {
	panic("not implemented")
}

func (l *ftSearchListener) ExitQuery_part(ctx *parser.Query_partContext) {
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

func (l *ftSearchListener) union() bool {
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

func (l *ftSearchListener) intersect() bool {
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

func (l *ftSearchListener) pop() (index.TermIterator, bool) {
	v, ok := l.stack.Pop()
	if !ok {
		return nil, false
	}
	if v == nil {
		return nil, false
	}
	return v.(index.TermIterator), true
}
