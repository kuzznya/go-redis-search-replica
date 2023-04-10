package search

import (
	"github.com/willf/bitset"
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"sync"
)

type IntersectIterator struct {
	iters   []index.TermIterator
	drained bool
	async   bool
}

func (ii *IntersectIterator) Next() (occurrence index.DocTermOccurrence, score float32, ok bool) {
	if ii.async {
		return ii.asyncNext()
	}

	if ii.drained {
		ok = false
		return
	}

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

			occ, score, ok := iter.Next()
			if !ok {
				drained = true
				break
			}
			results[i] = iterBufValue{occ: occ, score: score}
		}

		if drained {
			ii.drained = true
			ok = false
			return
		}

		first := results[0]
		equal := true
		fields := first.occ.Fields
		occurrences := first.occ.Occurrences
		score := first.score
		maxKey := first.occ.Doc.Key
		for _, res := range results[1:] {
			if res.occ.Doc.Key > maxKey {
				maxKey = res.occ.Doc.Key
			}

			if equal {
				fields.InPlaceUnion(&res.occ.Fields)
				occurrences = append(occurrences, res.occ.Occurrences...)
				score += res.score

				equal = res.occ.Doc.Key == first.occ.Doc.Key
			}
		}

		if equal {
			result := index.DocTermOccurrence{Doc: first.occ.Doc, TF: 0, Fields: fields, Occurrences: occurrences}
			return result, score, true
		}

		takeOld = bitset.BitSet{}
		for i, res := range results {
			if res.occ.Doc.Key == maxKey {
				takeOld.Set(uint(i))
			}
		}
	}
}

func (ii *IntersectIterator) asyncNext() (occurrence index.DocTermOccurrence, score float32, ok bool) {
	if ii.drained {
		ok = false
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
		fields := first.occ.Fields
		occurrences := first.occ.Occurrences
		score := first.score
		maxKey := first.occ.Doc.Key
		for _, res := range results[1:] {
			if res.occ.Doc.Key > maxKey {
				maxKey = res.occ.Doc.Key
			}

			if equal {
				fields.InPlaceUnion(&res.occ.Fields)
				occurrences = append(occurrences, res.occ.Occurrences...)
				score += res.score
			}

			if equal && res.occ.Doc.Key != first.occ.Doc.Key {
				equal = false
			}
		}

		if equal {
			result := index.DocTermOccurrence{Doc: first.occ.Doc, TF: 0, Fields: fields, Occurrences: occurrences}
			return result, score, true
		}

		takeOld = bitset.BitSet{}
		for i, res := range results {
			if res.occ.Doc.Key == maxKey {
				takeOld.Set(uint(i))
			}
		}
	}
}

func Intersect(iters ...index.TermIterator) index.TermIterator {
	return &IntersectIterator{iters: iters}
}
