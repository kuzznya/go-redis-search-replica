package search

import (
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
	"sync"
)

type UnionIterator struct {
	iter1       index.TermIterator
	iter2       index.TermIterator
	buf         iterBufValue
	bufHasValue bool
	drained     bool
	async       bool
}

func (u *UnionIterator) Next() (occurrence index.DocTermOccurrence, score float32, ok bool) {
	if u.async {
		return u.asyncNext()
	}

	if u.drained == true {
		ok = false
		return
	}

	if u.bufHasValue {
		u.bufHasValue = false
		return u.buf.occ, u.buf.score, true
	}

	occ1, score1, ok1 := u.iter1.Next()
	occ2, score2, ok2 := u.iter2.Next()

	if !ok1 && !ok2 {
		u.drained = true
		ok = false
		return
	}
	if !ok1 {
		return occ2, score2, true
	}
	if !ok2 {
		return occ1, score1, true
	}

	if occ1.Doc == occ2.Doc {
		fields := occ1.Fields
		fields.InPlaceUnion(&occ2.Fields)
		occurrences := append(occ1.Occurrences, occ2.Occurrences...)
		result := index.DocTermOccurrence{Doc: occ1.Doc, TF: 0, Fields: fields, Occurrences: occurrences}
		return result, score1 + score2, true // TODO: 06/05/2023 add penalty for distance ?
	}
	// skip buffer for the iterator with greater key as the other iterator can return the same key later
	if occ1.Doc.Key > occ2.Doc.Key {
		u.buf = iterBufValue{occ: occ1, score: score1}
		u.bufHasValue = true
		return occ2, score2, true
	} else {
		u.buf = iterBufValue{occ: occ2, score: score2}
		u.bufHasValue = true
		return occ1, score1, true
	}
}

func (u *UnionIterator) asyncNext() (occurrence index.DocTermOccurrence, score float32, ok bool) {
	if u.drained == true {
		ok = false
		return
	}

	if u.bufHasValue {
		u.bufHasValue = false
		return u.buf.occ, u.buf.score, true
	}

	wg := sync.WaitGroup{}

	var occ1, occ2 index.DocTermOccurrence
	var score1, score2 float32
	var ok1, ok2 bool

	wg.Add(2)
	go func() {
		defer wg.Done()
		occ1, score1, ok1 = u.iter1.Next()
	}()
	go func() {
		defer wg.Done()
		occ2, score2, ok2 = u.iter2.Next()
	}()
	wg.Wait()

	if !ok1 && !ok2 {
		u.drained = true
		ok = false
		return
	}
	if !ok1 {
		return occ2, score2, true
	}
	if !ok2 {
		return occ1, score1, true
	}

	if occ1.Doc.Key == occ2.Doc.Key {
		fields := occ1.Fields
		fields.InPlaceUnion(&occ2.Fields)
		occurrences := append(occ1.Occurrences, occ2.Occurrences...)
		result := index.DocTermOccurrence{Doc: occ1.Doc, TF: 0, Fields: fields, Occurrences: occurrences}
		return result, score1 + score2, true // TODO: 06/05/2023 add penalty for distance ?
	}
	// skip buffer for the iterator with greater key as the other iterator can return the same key later
	if occ1.Doc.Key > occ2.Doc.Key {
		u.buf = iterBufValue{occ: occ1, score: score1}
		u.bufHasValue = true
		return occ2, score2, true
	} else {
		u.buf = iterBufValue{occ: occ2, score: score2}
		u.bufHasValue = true
		return occ1, score1, true
	}
}

func Union(iter1 index.TermIterator, iter2 index.TermIterator) index.TermIterator {
	return &UnionIterator{iter1: iter1, iter2: iter2}
}
