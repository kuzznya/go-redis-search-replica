package search

import (
	"github.com/blevesearch/go-porterstemmer"
	"github.com/blevesearch/segment"
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	log "github.com/sirupsen/logrus"
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

func TopN(limit int, iter index.TermIterator) index.TermIterator {
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

func Search(query string) {
	terms := make([]string, 0)
	seg := segment.NewSegmenterDirect([]byte(query))
	pos := 0
	for seg.Segment() {
		token := seg.Text()

		if seg.Type() == segment.None {
			continue
		}

		term := porterstemmer.StemString(token)
		terms = append(terms, term)

		pos++
	}
	if err := seg.Err(); err != nil {
		log.WithError(err).Panicln("Failed to segment value")
	}

}
