package search

import (
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"math"
)

type IntersectIterator struct {
	iter1   index.TermIterator
	iter2   index.TermIterator
	drained bool
}

func (ii *IntersectIterator) Next() (occurrence index.DocTermOccurrence, score float32, ok bool) {
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
			score = (buf1.score + buf2.score) / distancePenalty(buf1.occ.Occurrences, buf2.occ.Occurrences)
			return result, score, true
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
	if _, ok := iter1.(index.StopWordIterator); ok {
		return iter2
	}
	if _, ok := iter2.(index.StopWordIterator); ok {
		return iter1
	}
	return &IntersectIterator{iter1: iter1, iter2: iter2}
}

func distancePenalty(occs1 []index.FieldTermOccurrence, occs2 []index.FieldTermOccurrence) float32 {
	if len(occs1) == 0 || len(occs2) == 0 {
		return 1
	}

	i1 := 0
	i2 := 0

	minDist := math.MaxInt
	for {
		if i1 == len(occs1) || i2 == len(occs2) {
			break
		}
		pos1 := occs1[i1].Pos
		pos2 := occs2[i2].Pos
		minDist = min(abs(pos1-pos2), minDist)
		if pos2 > pos1 {
			i1++
		} else {
			i2++
		}
	}
	minDistFloat := float64(minDist)
	return float32(math.Sqrt(minDistFloat * minDistFloat))
}

func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}
