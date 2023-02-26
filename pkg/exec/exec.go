package exec

import (
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
)

type Executor struct {
	s storage.Storage
}

func New(s storage.Storage) Executor {
	return Executor{s: s}
}

func (e Executor) Exec(cmd Command) error {
	return cmd.exec(e.s)
}
