package search

import (
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"sort"
)

type iterBufValue struct {
	occ   index.DocTermOccurrence
	score float32
}

type TopNIterator struct {
	values []iterBufValue
}

func (t *TopNIterator) Next() (occurrence index.DocTermOccurrence, score float32, ok bool) {
	if len(t.values) == 0 {
		ok = false
		return
	}
	occurrence = t.values[0].occ
	score = t.values[0].score
	ok = true
	t.values = t.values[1:]
	return
}

func TopN(offset int, limit int, iter index.TermIterator) index.TermIterator {
	for i := offset; i > 0; i-- {
		_, _, ok := iter.Next()
		if !ok {
			return index.Empty()
		}
	}

	values := make([]iterBufValue, 0)
	for {
		occ, score, ok := iter.Next()
		if !ok {
			break
		}
		values = append(values, iterBufValue{occ: occ, score: score})
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].score >= values[j].score
	})

	if len(values) < limit {
		limit = len(values)
	}
	values = values[:limit]

	return &TopNIterator{values: values}
}
