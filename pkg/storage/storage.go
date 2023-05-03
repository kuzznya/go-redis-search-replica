package storage

import "sync"

type Storage struct {
	m      map[string]Document
	onSave DocumentCallback
	gcFunc DocumentCallback
	mu     *sync.RWMutex
}

type Document struct {
	Key     string // TODO check if it should be *string
	Hash    Hash
	Deleted bool
}

type Hash map[string][]byte

type DocumentCallback func(d *Document)

func New(onSave DocumentCallback, gcFunc DocumentCallback) Storage {
	return Storage{m: map[string]Document{}, onSave: onSave, gcFunc: gcFunc, mu: &sync.RWMutex{}}
}

func (s Storage) Save(key string, hash Hash) {
	s.mu.Lock()
	doc, found := s.m[key]
	newDoc := Document{Key: key, Hash: hash, Deleted: false}
	s.m[key] = newDoc
	s.mu.Unlock()
	if found {
		doc.Deleted = true
		s.gcFunc(&doc)
	}
	s.onSave(&newDoc)
}

func (s Storage) Get(key string) (Document, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, found := s.m[key]; found {
		return val, true
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
		s.gcFunc(&doc) // TODO 20.03.2023 check if we get the same reference that is stored in the map
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

func (s Storage) Walk(func(key string, doc Document)) {

}
