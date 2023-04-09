package index

import "github.com/kuzznya/go-redis-search-replica/pkg/storage"

type Index interface {
	Update(h storage.Hash)
	Delete(h storage.Hash)
}
