package search

import (
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"sync"
)

type IntersectIterator struct {
	iter1   index.TermIterator
	iter2   index.TermIterator
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

	var buf1 iterBufValue
	var buf1HasValue bool
	var buf2 iterBufValue
	var buf2HasValue bool

	for {
		if !buf1HasValue {
			occ1, score1, ok1 := ii.iter1.Next()
			if !ok1 {
				ii.drained = true
				ok = false
				return
			}
			buf1.occ = occ1
			buf1.score = score1
			buf1HasValue = true
		}

		if !buf2HasValue {
			occ2, score2, ok2 := ii.iter2.Next()
			if !ok2 {
				ii.drained = true
				ok = false
				return
			}
			buf2.occ = occ2
			buf2.score = score2
			buf2HasValue = true
		}

		if buf1.occ.Doc.Key == buf2.occ.Doc.Key {
			fields := buf1.occ.Fields
			fields.InPlaceUnion(&buf2.occ.Fields)
			occurrences := append(buf1.occ.Occurrences, buf2.occ.Occurrences...)
			result := index.DocTermOccurrence{Doc: buf1.occ.Doc, TF: 0, Fields: fields, Occurrences: occurrences}
			return result, buf1.score + buf2.score, true // TODO: 06/05/2023 add penalty for distance
		}
		// skip buffer for the iterator with greater key as the other iterator can return the same key later
		if buf1.occ.Doc.Key > buf2.occ.Doc.Key {
			buf2HasValue = false
		} else {
			buf1HasValue = false
		}
	}
}

func (ii *IntersectIterator) asyncNext() (occurrence index.DocTermOccurrence, score float32, ok bool) {
	if ii.drained {
		ok = false
		return
	}

	var buf1 iterBufValue
	var buf1HasValue bool
	var buf2 iterBufValue
	var buf2HasValue bool

	wg := sync.WaitGroup{}

	for {
		if !buf1HasValue {
			wg.Add(1)

			go func() {
				defer wg.Done()

				occ1, score1, ok1 := ii.iter1.Next()
				if !ok1 {
					ii.drained = true
					return
				}
				buf1.occ = occ1
				buf1.score = score1
				buf1HasValue = true
			}()
		}

		if !buf2HasValue {
			wg.Add(1)

			go func() {
				defer wg.Done()

				occ2, score2, ok2 := ii.iter2.Next()
				if !ok2 {
					ii.drained = true
					return
				}
				buf2.occ = occ2
				buf2.score = score2
				buf2HasValue = true
			}()
		}

		wg.Wait()
		if ii.drained {
			ok = false
			return
		}

		if buf1.occ.Doc == buf2.occ.Doc {
			fields := buf1.occ.Fields
			fields.InPlaceUnion(&buf2.occ.Fields)
			occurrences := append(buf1.occ.Occurrences, buf2.occ.Occurrences...)
			result := index.DocTermOccurrence{Doc: buf1.occ.Doc, TF: 0, Fields: fields, Occurrences: occurrences}
			return result, buf1.score + buf2.score, true // TODO: 06/05/2023 add penalty for distance
		}
		// skip buffer for the iterator with greater key as the other iterator can return the same key later
		if buf1.occ.Doc.Key > buf2.occ.Doc.Key {
			buf2HasValue = false
		} else {
			buf1HasValue = false
		}
	}
}

func Intersect(iter1 index.TermIterator, iter2 index.TermIterator) index.TermIterator {
	return &IntersectIterator{iter1: iter1, iter2: iter2}
}
