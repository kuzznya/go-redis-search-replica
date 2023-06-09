package index

import (
	"fmt"
	"github.com/bits-and-blooms/bitset"
	"github.com/blevesearch/go-porterstemmer"
	"github.com/blevesearch/segment"
	"github.com/emirpasic/gods/queues"
	"github.com/emirpasic/gods/queues/arrayqueue"
	"github.com/emirpasic/gods/sets/hashset"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	log "github.com/sirupsen/logrus"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var stopWords = []string{"a", "an", "and", "are", "as", "at",
	"be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such",
	"that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"}

type FTSIndex struct {
	deleted     bool
	prefixes    []string
	fields      []string // sorted array
	trie        Trier
	df          map[string]uint
	docsCount   int32
	creating    bool
	pendingDocs queues.Queue // TODO: 07/05/2023 handle document deletion, probably by marking document in the queue as deleted
	mu          sync.RWMutex
}

type DocTermOccurrence struct {
	Doc         *storage.Document
	TF          float32
	Fields      bitset.BitSet
	Occurrences []FieldTermOccurrence
}

type FieldTermOccurrence struct {
	FieldIdx int
	Offset   int
	Len      int
	Pos      int
}

type TermIterator interface {
	Next() (occurrence DocTermOccurrence, score float32, ok bool)
}

type EmptyIterator struct{}

func (iter EmptyIterator) Next() (occurrence DocTermOccurrence, score float32, ok bool) {
	ok = false
	return
}

type StopWordIterator struct {
	EmptyIterator
}

func Empty() TermIterator {
	return EmptyIterator{}
}

func NewFTSIndex(prefixes []string, fields []string) *FTSIndex {
	sort.Strings(fields)
	fieldSet := hashset.New()
	for _, field := range fields {
		fieldSet.Add(field)
	}
	return &FTSIndex{
		prefixes:    prefixes,
		fields:      fields,
		trie:        NewRuneTrie(),
		df:          map[string]uint{},
		creating:    true,
		pendingDocs: arrayqueue.New(),
		docsCount:   0,
	}
}

func (i *FTSIndex) Load(docs []*storage.Document) {
	for _, doc := range docs {
		if i.deleted {
			return
		}
		if !matchesPrefix(i.prefixes, doc.Key) {
			continue
		}
		i.processDoc(doc)
	}
	i.drainQueue()

	i.creating = false

	// draining the queue here because new docs could be added before flipping i.creating but after first drain
	i.drainQueue()
}

func (i *FTSIndex) drainQueue() {
	for {
		if i.deleted {
			return
		}

		doc, ok := i.pendingDocs.Dequeue()
		if !ok {
			break
		}
		i.processDoc(doc.(*storage.Document))
	}
}

func (i *FTSIndex) MarkDeleted() {
	i.deleted = true
}

func (i *FTSIndex) Add(doc *storage.Document) {
	if !matchesPrefix(i.prefixes, doc.Key) {
		return
	}

	// defer document indexing if not all existing docs are processed yet
	if i.creating {
		log.Debugf("Index is processing existing data, adding document %s to the queue", doc.Key)
		i.pendingDocs.Enqueue(doc)
		return
	}
}

func (i *FTSIndex) processDoc(doc *storage.Document) {
	log.Debugf("Adding document %s to index", doc.Key)

	// O(1) access to occurrence for current document, using trie here seems inefficient due to O(k) access and result as array of Occurrences in all documents
	occurrences := make(map[string]*DocTermOccurrence)

	termCount := 0

	// token index counted across all fields
	pos := 0

	for k, v := range doc.Hash {
		fieldIdx := sort.SearchStrings(i.fields, k)
		if fieldIdx >= len(i.fields) || k != i.fields[fieldIdx] {
			continue
		}

		start := 0
		seg := segment.NewSegmenterDirect(v)
		for seg.Segment() {
			token := seg.Text()
			end := start + len(token)

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
		occurrence.TF = float32(len(occurrence.Occurrences)) / float32(termCount)
		i.trie.Add(term, *occurrence)
		df, ok := i.df[term]
		if !ok {
			i.df[term] = 1
		} else {
			i.df[term] = df + 1
		}
	}
}

func (i *FTSIndex) processToken(doc *storage.Document, occurrences map[string]*DocTermOccurrence, fieldIdx int, token string, start int, pos int) {
	token = strings.ToLower(token)

	if isStopWord(token) {
		return
	}

	termRunes := porterstemmer.StemWithoutLowerCasing([]rune(token))
	term := string(termRunes)

	occurrence, found := occurrences[term]
	if !found {
		occurrence = &DocTermOccurrence{Doc: doc, Fields: *bitset.New(uint(fieldIdx)), Occurrences: []FieldTermOccurrence{}}
		occurrences[term] = occurrence
	}

	occurrence.Fields.Set(uint(fieldIdx))

	fieldOccurrence := FieldTermOccurrence{FieldIdx: fieldIdx, Offset: start, Len: len(token), Pos: pos}
	occurrence.Occurrences = append(occurrence.Occurrences, fieldOccurrence)
}

type readIterator struct {
	i           *FTSIndex
	term        string
	idf         float32
	occurrences []DocTermOccurrence
	pos         int
}

func (r *readIterator) Next() (occurrence DocTermOccurrence, score float32, ok bool) {
	for {
		if r.pos == len(r.occurrences) {
			ok = false
			return
		}
		occurrence = r.occurrences[r.pos]
		r.pos++
		if occurrence.Doc.Deleted {
			continue
		}
		return occurrence, occurrence.TF * r.idf, true
	}

}

func (i *FTSIndex) Read(term string) TermIterator {
	term = strings.ToLower(term)
	if isStopWord(term) {
		return StopWordIterator{}
	}

	termRunes := porterstemmer.StemWithoutLowerCasing([]rune(term))
	term = string(termRunes)

	i.mu.RLock()
	defer i.mu.RUnlock()
	occurrences := i.trie.Get(term)
	if occurrences == nil {
		return Empty()
	}
	idf := i.idf(term)
	return &readIterator{i: i, term: term, idf: idf, occurrences: occurrences, pos: 0}
}

func (i *FTSIndex) PrintIndex() {
	i.mu.RLock()
	defer i.mu.RUnlock()

	_ = i.trie.Walk(func(key string, occurrences []DocTermOccurrence) error {
		fmt.Printf("Term: %s, IDF = %.3f\n", key, i.idf(key))
		for _, o := range occurrences {
			fmt.Printf("\tDocument %s Occurrences (%d, TF = %.3f):\n", o.Doc.Key, len(o.Occurrences), o.TF)
			for _, fo := range o.Occurrences {
				field := i.fields[fo.FieldIdx]
				value := o.Doc.Hash[field]
				word := string(value[fo.Offset : fo.Offset+fo.Len])
				fmt.Printf("\t\t@%s (offset %d, len %d, pos %d): %s\n",
					field, fo.Offset, fo.Len, fo.Pos, word)
			}
		}
		return nil
	})
}

func (i *FTSIndex) idf(term string) float32 {
	df := i.df[term]
	idf := math.Log2(1 + float64(i.docsCount)/float64(df))
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

func isStopWord(term string) bool {
	i := sort.SearchStrings(stopWords, term)
	return i >= 0 && i < len(stopWords) && stopWords[i] == term
}
