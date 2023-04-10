package search

import (
	log "github.com/sirupsen/logrus"
	"github.com/willf/bitset"
	"github.com/kuzznya/go-redis-search-replica/pkg/index"
)

type UnionIterator struct {
	iters       []index.TermIterator
	buf         []iterBufValue
	bufHasValue bitset.BitSet
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

	results := make(map[string]iterBufValue)

	var minKey string

	for i, iter := range u.iters {
		iter := iter

		var occ index.DocTermOccurrence
		var curScore float32
		var curOk bool

		if u.bufHasValue.Test(uint(i)) {
			occ = u.buf[i].occ
			curScore = u.buf[i].score
		} else {
			occ, curScore, curOk = iter.Next()
			if !curOk {
				continue
			}
			u.buf[i] = iterBufValue{occ: occ, score: curScore}
			u.bufHasValue.Set(uint(i))
		}

		if minKey == "" || occ.Doc.Key < minKey {
			minKey = occ.Doc.Key
		}

		if v, found := results[occ.Doc.Key]; found {
			v.occ.Fields.InPlaceUnion(&occ.Fields)
			v.occ.Occurrences = append(v.occ.Occurrences, occ.Occurrences...)
		} else {
			value := iterBufValue{occ: occ, score: curScore}
			results[occ.Doc.Key] = value
			u.buf[i] = value
		}
	}

	if len(results) == 0 {
		ok = false
		u.drained = true
		return
	}

	result := results[minKey]
	for i, v := range u.buf {
		if u.bufHasValue.Test(uint(i)) && v.occ.Doc.Key == minKey {
			u.bufHasValue.Clear(uint(i))
		}
	}

	if result.occ.Doc == nil {
		log.Panicf("Result for key %s is null", minKey)
	}

	return result.occ, result.score, true
}

func (u *UnionIterator) asyncNext() (occurrence index.DocTermOccurrence, score float32, ok bool) {
	panic("TODO implement me")
	//
	//if u.buf != nil {
	//	occurrence = u.buf[u.pos].occ
	//	score = u.buf[u.pos].score
	//	ok = true
	//
	//	u.pos++
	//
	//	if u.pos == len(u.buf) {
	//		u.pos = 0
	//		u.buf = nil
	//	}
	//
	//	return
	//}
	//
	//results := make([]iterBufValue, len(u.iters))
	//hasValue := bitset.New(uint(len(u.iters)))
	//wg := sync.WaitGroup{}
	//
	//for i, iter := range u.iters {
	//	iter := iter
	//	i := i
	//
	//	wg.Add(1)
	//	go func() {
	//		defer wg.Done()
	//		occ, score, ok := iter.Next()
	//		if !ok {
	//			return
	//		}
	//		results[i] = iterBufValue{occ: occ, score: score}
	//		hasValue.Set(uint(i))
	//	}()
	//}
	//wg.Wait()
	//
	//validResults := make([]iterBufValue, 0)
	//for i, r := range results {
	//	if !hasValue.Test(uint(i)) {
	//		continue
	//	}
	//	validResults = append(validResults, r)
	//}
	//
	//if len(validResults) == 0 {
	//	ok = false
	//	return
	//}
	//
	//first := validResults[0]
	//if len(validResults) > 1 {
	//	u.buf = validResults[1:]
	//}
	//return first.occ, first.score, true
}

func Union(iters ...index.TermIterator) index.TermIterator {
	return &UnionIterator{iters: iters, buf: make([]iterBufValue, len(iters)), bufHasValue: bitset.BitSet{}}
}
