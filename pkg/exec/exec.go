package exec

import (
	"github.com/kuzznya/go-redis-search-replica/pkg/search"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
)

type Executor struct {
	s      storage.Storage
	engine search.Engine
}

func New(s storage.Storage, e search.Engine) Executor {
	return Executor{s: s, engine: e}
}

func (e Executor) Exec(cmd Command) error {
	return cmd.exec(e.s, e.engine)
}
