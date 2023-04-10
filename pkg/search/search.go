package search

import (
	"github.com/blevesearch/go-porterstemmer"
	"github.com/blevesearch/segment"
	log "github.com/sirupsen/logrus"
	"github.com/willf/bitset"
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"sync"
)

type IntersectIterator struct {
	iters   []index.TermIterator
	buf     []iterBufValue
	pos     int
	drained bool
}

type iterBufValue struct {
	occ   index.DocTermOccurrence
	score float32
}

func (ii *IntersectIterator) Next() (occurrence index.DocTermOccurrence, score float32, ok bool) {
	if ii.drained {
		ok = false
		return
	}

	if ii.buf != nil {
		occurrence = ii.buf[ii.pos].occ
		score = ii.buf[ii.pos].score
		ok = true

		ii.pos++

		if ii.pos == len(ii.buf) {
			ii.pos = 0
			ii.buf = nil
		}

		return
	}

	wg := sync.WaitGroup{}
	results := make([]iterBufValue, len(ii.iters))
	takeOld := bitset.BitSet{}
	drained := false
	for {
		for i, iter := range ii.iters {
			if takeOld.Test(uint(i)) {
				continue
			}

			i := i
			iter := iter

			wg.Add(1)
			go func() {
				defer wg.Done()
				occ, score, ok := iter.Next()
				if !ok {
					drained = true
					return
				}
				results[i] = iterBufValue{occ: occ, score: score}
			}()
		}
		wg.Wait()

		if drained {
			ii.drained = true
			ok = false
			return
		}

		first := results[0]
		equal := true
		maxKey := first.occ.Doc.Key
		for _, res := range results[1:] {
			if res.occ.Doc.Key > maxKey {
				maxKey = res.occ.Doc.Key
			}

			if equal && res.occ.Doc.Key != first.occ.Doc.Key {
				equal = false
			}
		}

		if equal {
			ii.buf = results[1:]
			return first.occ, first.score, true
		}

		takeOld = bitset.BitSet{}
		for i, res := range results {
			if res.occ.Doc.Key == maxKey {
				takeOld.Set(uint(i))
			}
		}
	}

	//for _, iter := range ii.iters {
	//	occ, score, ok := iter.Next()
	//	if !ok {
	//		continue
	//	}
	//
	//	if oldOcc, found := ii.buf[occ.Doc.Key]; found {
	//		if oldOcc != nil {
	//			ii.buf[occ.Doc.Key] = nil // mark as already reported
	//			return oldOcc.occ, oldOcc.score, true
	//		}
	//		return occ, score, true
	//	}
	//
	//	ii.buf[occ.Doc.Key] = &iterBufValue{occ: occ, score: score}
	//}
	//ok = false
	//return
}

func Intersect(iters ...index.TermIterator) index.TermIterator {
	return &IntersectIterator{iters: iters, buf: nil}
}

type UnionIterator struct {
	iters []index.TermIterator
	buf   []iterBufValue
	pos   int
}

func (u UnionIterator) Next() (occurrence index.DocTermOccurrence, score float32, ok bool) {
	if u.buf != nil {
		occurrence = u.buf[u.pos].occ
		score = u.buf[u.pos].score
		ok = true

		u.pos++

		if u.pos == len(u.buf) {
			u.pos = 0
			u.buf = nil
		}

		return
	}

	results := make([]iterBufValue, len(u.iters))
	hasValue := bitset.New(uint(len(u.iters)))
	wg := sync.WaitGroup{}

	for i, iter := range u.iters {
		iter := iter
		i := i

		wg.Add(1)
		go func() {
			defer wg.Done()
			occ, score, ok := iter.Next()
			if !ok {
				return
			}
			results[i] = iterBufValue{occ: occ, score: score}
			hasValue.Set(uint(i))
		}()
	}
	wg.Wait()

	validResults := make([]iterBufValue, 0)
	for i, r := range results {
		if !hasValue.Test(uint(i)) {
			continue
		}
		validResults = append(validResults, r)
	}

	if len(validResults) == 0 {
		ok = false
		return
	}

	first := validResults[0]
	if len(validResults) > 1 {
		u.buf = validResults[1:]
	}
	return first.occ, first.score, true

	//if u.curIdx >= len(u.iters) {
	//	return
	//}
	//for i := u.curIdx; i < len(u.iters); i++ {
	//	occurrence, score, ok = u.iters[i].Next()
	//	if ok {
	//		u.curIdx = i
	//		break
	//	}
	//}
	//u.curIdx = len(u.iters)
	//return
}

func Union(iters ...index.TermIterator) index.TermIterator {
	return &UnionIterator{iters: iters, buf: nil, pos: 0}
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
