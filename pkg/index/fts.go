package index

import (
	"fmt"
	"github.com/blevesearch/go-porterstemmer"
	"github.com/blevesearch/segment"
	"github.com/emirpasic/gods/sets"
	"github.com/emirpasic/gods/sets/hashset"
	log "github.com/sirupsen/logrus"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type FTSIndex struct {
	prefixes  []string // TODO 13.03.2023 maybe use Trie here
	fields    []string // sorted array
	fieldSet  sets.Set // fields duplicated to set for O(1) checks
	trie      Trier
	df        map[string]uint // TODO 20.03.2023 store this in trie ?
	docsCount int32
	mu        sync.RWMutex
}

type docTermOccurrence struct {
	doc         *storage.Document
	tf          float32
	fields      []uint // fields: array of bit masks of FTSIndex.fields that contain occurrences, 1st 8 bits represent mask for first 8 FTSIndex.fields, the lowest bit is the 1st field
	occurrences []fieldTermOccurrence
}

type fieldTermOccurrence struct {
	fieldIdx int
	offset   int
	len      int
	pos      int
}

func NewFTSIndex(prefixes []string, fields []string) *FTSIndex {
	sort.Strings(fields)
	fieldSet := hashset.New()
	for _, field := range fields {
		fieldSet.Add(field)
	}
	return &FTSIndex{
		prefixes:  prefixes,
		fields:    fields,
		fieldSet:  fieldSet,
		trie:      NewRuneTrie(),
		df:        map[string]uint{},
		docsCount: 0,
	}
}

func (i *FTSIndex) Add(doc *storage.Document) {
	if !matchesPrefix(i.prefixes, doc.Key) {
		return
	}

	log.Debugf("Adding document %s to index", doc.Key)

	// O(1) access to occurrence for current document, using trie here seems inefficient due to O(k) access and result as array of occurrences in all documents
	occurrences := make(map[string]*docTermOccurrence)

	termCount := 0

	for k, v := range doc.Hash {
		fieldIdx := sort.SearchStrings(i.fields, k)
		if fieldIdx > len(i.fields) || k != i.fields[fieldIdx] {
			continue
		}

		start := 0
		pos := 0
		seg := segment.NewSegmenterDirect(v)
		for seg.Segment() {
			token := seg.Text()
			end := start + len(token)

			// TODO 13.03.2023 remove this, needed only to detect unusual types
			i.logUnusualTokenType(token, seg.Type())

			if seg.Type() == segment.None {
				start = end
				continue
			}

			i.processToken(doc, occurrences, fieldIdx, token, start, pos)

			start = end
			pos++
		}
		if err := seg.Err(); err != nil {
			log.WithError(err).Panicln("Failed to segment value")
		}

		termCount += pos
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	atomic.AddInt32(&i.docsCount, 1)
	for term, occurrence := range occurrences {
		occurrence.tf = float32(len(occurrence.occurrences)) / float32(termCount)
		i.trie.Add(term, *occurrence)
		i.df[term]++
	}
}

func (i *FTSIndex) processToken(doc *storage.Document, occurrences map[string]*docTermOccurrence, fieldIdx int, token string, start int, pos int) {
	term := porterstemmer.StemString(token)

	occurrence, found := occurrences[term]
	if !found {
		occurrence = &docTermOccurrence{doc: doc, fields: []uint{0}, occurrences: []fieldTermOccurrence{}}
		occurrences[term] = occurrence
	}

	requiredMaskLen := fieldIdx/8 + 1
	for i := len(occurrence.fields); i < requiredMaskLen; i++ {
		occurrence.fields = append(occurrence.fields, 0)
	}

	occurrence.fields[fieldIdx/8] &= 1 << (fieldIdx % 8)

	fieldOccurrence := fieldTermOccurrence{fieldIdx: fieldIdx, offset: start, len: len(token), pos: pos}
	occurrence.occurrences = append(occurrence.occurrences, fieldOccurrence)
}

func (i *FTSIndex) logUnusualTokenType(token string, tokenType int) {
	switch tokenType {
	case segment.Ideo:
		log.Warnf("%s type: Ideo\n", token)
	case segment.Kana:
		log.Warnf("%s type: Kana\n", token)
	case segment.Number:
		log.Warnf("%s type: Number\n", token)
	}
}

//type termIterator interface {
//	Next() (fieldTermOccurrence, bool)
//}
//
//type termIter struct {
//}
//
//func (i *FTSIndex) termIter(term string) *docTermOccurrence {
//	idf := i.idf[term]
//	i.trie.Get(term)
//}

func (i *FTSIndex) PrintIndex() {
	i.mu.RLock()
	defer i.mu.RUnlock()

	_ = i.trie.Walk(func(key string, occurrences []docTermOccurrence) error {
		fmt.Printf("Term: %s, IDF = %.3f\n", key, i.idf(key))
		for _, o := range occurrences {
			fmt.Printf("\tDocument %s occurrences (%d, tf = %.3f):\n", o.doc.Key, len(o.occurrences), o.tf)
			for _, fo := range o.occurrences {
				field := i.fields[fo.fieldIdx]
				value := o.doc.Hash[field]
				word := string(value[fo.offset : fo.offset+fo.len])
				fmt.Printf("\t\t@%s (offset %d, len %d, pos %d): %s\n",
					field, fo.offset, fo.len, fo.pos, word)
			}
		}
		return nil
	})
}

func (i *FTSIndex) idf(term string) float32 {
	df := i.df[term]
	idf := math.Log2(1 + float64(i.docsCount)/float64(df))
	fmt.Printf("DF = %d, total docs = %d\n", df, i.docsCount)
	return float32(idf)
}

func matchesPrefix(prefixes []string, key string) bool {
	for _, prefix := range prefixes {
		if prefix == "*" || strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
