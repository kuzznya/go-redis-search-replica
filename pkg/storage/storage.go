package storage

type Storage struct {
	m      map[string]Object
	gcFunc GCFunc
}

type Object struct {
	Key     string // TODO check if it should be *string
	Hash    Hash
	Deleted bool
}

type Hash map[string]string

type GCFunc func(o Object)

func New(gcFunc GCFunc) Storage {
	return Storage{m: map[string]Object{}, gcFunc: gcFunc}
}

func (s Storage) Save(key string, hash Hash) {
	o, found := s.m[key]
	s.m[key] = Object{Key: key, Hash: hash, Deleted: false}
	if found {
		o.Deleted = true
		s.gcFunc(o)
	}
}

func (s Storage) Get(key string) (Object, bool) {
	if val, found := s.m[key]; found {
		return val, true
	}
	return Object{}, false
}

func (s Storage) Delete(key string) {
	o, found := s.m[key]
	delete(s.m, key)
	if found {
		o.Deleted = true
		s.gcFunc(o)
	}
}

func (s Storage) Rename(key string, newKey string) {
	o, found := s.m[key]
	if found {
		delete(s.m, key)
		s.m[newKey] = o
	}
}
