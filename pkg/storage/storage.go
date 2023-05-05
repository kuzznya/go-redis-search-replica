package storage

import (
	"github.com/tidwall/redcon"
	"strings"
	"sync"
)

type Storage struct {
	m        map[string]*Document
	onSave   DocumentCallback
	onDelete DocumentCallback
	mu       *sync.RWMutex
}

type Document struct {
	Key     string
	Hash    Hash
	Deleted bool
}

func (d Document) MarshalRESP() []byte {
	data := make([]byte, 0)
	data = redcon.AppendString(data, d.Key)
	data = redcon.AppendAny(data, d.Hash)
	return data
}

type Hash map[string][]byte

type DocumentCallback func(d *Document)

func New() Storage {
	noOp := func(*Document) {}
	return Storage{m: map[string]*Document{}, onSave: noOp, onDelete: noOp, mu: &sync.RWMutex{}}
}

func (s Storage) OnSave(action DocumentCallback) {
	s.onSave = action
}

func (s Storage) OnDelete(action DocumentCallback) {
	s.onDelete = action
}

func (s Storage) Save(key string, hash Hash) {
	s.mu.Lock()
	doc, found := s.m[key]
	newDoc := &Document{Key: key, Hash: hash, Deleted: false}
	s.m[key] = newDoc
	s.mu.Unlock()
	if found {
		doc.Deleted = true
		s.onDelete(doc)
	}
	s.onSave(newDoc)
}

func (s Storage) Get(key string) (Document, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, found := s.m[key]; found {
		return *val, true
	}
	return Document{}, false
}

func (s Storage) Delete(key string) {
	s.mu.Lock()
	doc, found := s.m[key]
	delete(s.m, key)
	s.mu.Unlock()
	if found {
		doc.Deleted = true
		s.onDelete(doc)
	}
}

func (s Storage) Rename(key string, newKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, found := s.m[key]
	if found {
		delete(s.m, key)
		s.m[newKey] = doc
	}
}

func (s Storage) GetAll(prefixes []string) []*Document {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := make([]*Document, 0)
	for _, v := range s.m {
		if !matchesPrefix(prefixes, v.Key) {
			continue
		}
		data = append(data, v)
	}
	return data
}

func matchesPrefix(prefixes []string, key string) bool {
	for _, prefix := range prefixes {
		if prefix == "*" || strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
