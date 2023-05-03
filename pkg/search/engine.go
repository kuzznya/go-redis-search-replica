package search

import (
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/blevesearch/go-porterstemmer"
	"github.com/emirpasic/gods/queues"
	"github.com/emirpasic/gods/queues/arrayqueue"
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"github.com/kuzznya/go-redis-search-replica/pkg/parser"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	"github.com/pkg/errors"
	"strconv"
	"sync"
)

type Engine struct {
	indexes map[string]*index.FTSIndex
	mu      *sync.RWMutex
}

func NewEngine() Engine {
	return Engine{indexes: make(map[string]*index.FTSIndex), mu: &sync.RWMutex{}}
}

func (e Engine) CreateIndex(name string, prefixes []string, fields []string) {
	idx := index.NewFTSIndex(prefixes, fields)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.indexes[name] = idx
}

func (e Engine) Add(d *storage.Document) {
	for _, idx := range e.indexes {
		idx.Add(d)
	}
}

func (e Engine) Search(idxName string, ctx parser.IQueryContext) (index.TermIterator, error) {
	e.mu.RLock()
	idx, found := e.indexes[idxName]
	e.mu.RUnlock()
	if !found {
		return nil, errors.Errorf("Index %s not found", idxName)
	}

	ftSearch := newFtSearchListener(idx)

	antlr.NewParseTreeWalker().Walk(ftSearch, ctx)

	if ftSearch.err != nil {
		return nil, errors.Wrap(ftSearch.err, "failed to parse query")
	}

	v, ok := ftSearch.queue.Dequeue()
	if !ok {
		return nil, errors.New("failed to parse query")
	}

	return v.(index.TermIterator), nil
}

type ftSearchListener struct {
	*parser.BaseFTParserListener
	idx   *index.FTSIndex
	queue queues.Queue
	err   error
}

func newFtSearchListener(idx *index.FTSIndex) *ftSearchListener {
	return &ftSearchListener{idx: idx, queue: arrayqueue.New()}
}

func (l *ftSearchListener) ExitQuery(ctx *parser.QueryContext) {
	// TODO: 03/05/2023 ensure it can be no-op
}

func (l *ftSearchListener) ExitWord(ctx *parser.WordContext) {
	word := ctx.GetText()
	term := porterstemmer.StemString(word)
	l.queue.Enqueue(l.idx.Read(term))
}

func (l *ftSearchListener) ExitExact_match(ctx *parser.Exact_matchContext) {
	panic("not implemented")
}

func (l *ftSearchListener) ExitField_query_part(ctx *parser.Field_query_partContext) {
	panic("not implemented")
}

func (l *ftSearchListener) ExitParenthesized_query_part(ctx *parser.Parenthesized_query_partContext) {
	// TODO: 03/05/2023 ensure this should be no-op
}

func (l *ftSearchListener) ExitQuery_part(ctx *parser.Query_partContext) {
	if l.err != nil {
		return
	}

	if ctx.OR() != nil {
		ok := l.union()
		if !ok {
			l.err = errors.Errorf("Failed to parse query part: %s", ctx.GetText())
			return
		}
		return
	}

	ok := l.intersect()
	if !ok {
		l.err = errors.Errorf("Failed to parse query part: %s", ctx.GetText())
		return
	}
	return
}

func (l *ftSearchListener) ExitLimit_part(ctx *parser.Limit_partContext) {
	iter, ok := l.dequeue()
	if !ok {
		l.err = errors.Errorf("Failed to parse query part, no query found for limiting: %s", ctx.GetText())
		return
	}

	// TODO: 03/05/2023 change to long offset and limit
	offsetStr := ctx.Offset().GetText()
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		l.err = errors.Errorf("Failed to parse offset: %s", offsetStr)
		return
	}

	limitStr := ctx.Num().GetText()
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		l.err = errors.Errorf("Failed to parse limit: %s", limit)
		return
	}

	l.queue.Enqueue(TopN(offset, limit, iter))
}

func (l *ftSearchListener) union() bool {
	iter1, ok := l.dequeue()
	if !ok {
		return false
	}

	iter2, ok := l.dequeue()
	if !ok {
		return false
	}

	l.queue.Enqueue(Union(iter1, iter2))
	return true
}

func (l *ftSearchListener) intersect() bool {
	iter, ok := l.dequeue()
	if !ok {
		return false
	}

	for {
		iter2, ok := l.dequeue()
		if !ok {
			break
		}

		iter = Intersect(iter, iter2)
	}

	l.queue.Enqueue(iter)
	return true
}

func (l *ftSearchListener) dequeue() (index.TermIterator, bool) {
	v, ok := l.queue.Dequeue()
	if !ok {
		return nil, false
	}
	return v.(index.TermIterator), true
}
