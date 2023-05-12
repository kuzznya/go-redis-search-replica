package exec

import (
	"github.com/kuzznya/go-redis-search-replica/pkg/idxmodel"
	"github.com/kuzznya/go-redis-search-replica/pkg/search"
	"github.com/kuzznya/go-redis-search-replica/pkg/storage"
	"github.com/pkg/errors"
	"strconv"
)

const (
	Set      = "SET"
	Hset     = "HSET"
	Hmset    = "HMSET"
	Hsetnx   = "HSETNX"
	Hincrby  = "HINCRBY"
	Hdel     = "HDEL"
	Del      = "DEL"
	Rename   = "RENAME"
	Renamenx = "RENAMENX"
	FtCreate = "FT.CREATE"
)

type Command interface {
	Name() string
	exec(s storage.Storage, engine search.Engine) error
}

type SetCmd struct {
	Key string
}

func (c SetCmd) Name() string {
	return Set
}

func (c SetCmd) exec(s storage.Storage, _ search.Engine) error {
	s.Delete(c.Key)
	return nil
}

type HSetCmd struct {
	Key  string
	Args []HSetArg
}

type HSetArg struct {
	Field string
	Value []byte
}

func (c HSetCmd) Name() string {
	return Hset
}

func (c HSetCmd) exec(s storage.Storage, _ search.Engine) error {
	o, found := s.Get(c.Key)
	h := o.Hash
	if !found {
		h = storage.Hash{}
	}
	for _, arg := range c.Args {
		h[arg.Field] = arg.Value
	}
	s.Save(c.Key, h)
	return nil
}

type HsetnxCmd struct {
	Key   string
	Field string
	Value []byte
}

func (c HsetnxCmd) Name() string {
	return Hsetnx
}

func (c HsetnxCmd) exec(s storage.Storage, _ search.Engine) error {
	o, found := s.Get(c.Key)
	h := o.Hash
	if !found {
		h = storage.Hash{}
	} else if _, found := h[c.Field]; found {
		return nil
	}
	h[c.Field] = c.Value
	s.Save(c.Key, h)
	return nil
}

type HincrbyCmd struct {
	Key   string
	Field string
	Value int64
}

func (c HincrbyCmd) Name() string {
	return Hincrby
}

func (c HincrbyCmd) exec(s storage.Storage, _ search.Engine) error {
	o, found := s.Get(c.Key)
	h := o.Hash
	if !found {
		h = storage.Hash{}
	}

	val := int64(0)
	if prev, found := h[c.Field]; found {
		parsed, err := strconv.ParseInt(string(prev), 10, 64)
		if err != nil {
			return errors.New("hash value is not an integer")
		}
		val = parsed
	}
	val += c.Value

	h[c.Field] = []byte(strconv.FormatInt(val, 10))

	s.Save(c.Key, h)

	return nil
}

type HDelCmd struct {
	Key    string
	Fields []string
}

func (c HDelCmd) Name() string {
	return Hdel
}

func (c HDelCmd) exec(s storage.Storage, _ search.Engine) error {
	o, found := s.Get(c.Key)
	h := o.Hash
	if !found {
		return nil
	}
	for _, f := range c.Fields {
		delete(h, f)
	}
	if len(h) == 0 {
		s.Delete(c.Key)
	} else {
		s.Save(c.Key, h)
	}
	return nil
}

type DelCmd struct {
	Keys []string
}

func (c DelCmd) Name() string {
	return Del
}

func (c DelCmd) exec(s storage.Storage, _ search.Engine) error {
	for _, k := range c.Keys {
		s.Delete(k)
	}
	return nil
}

type RenameCmd struct {
	Key    string
	NewKey string
}

func (c RenameCmd) Name() string {
	return Rename
}

func (c RenameCmd) exec(s storage.Storage, _ search.Engine) error {
	s.Rename(c.Key, c.NewKey)
	return nil
}

type RenamenxCmd struct {
	Key    string
	NewKey string
}

func (c RenamenxCmd) Name() string {
	return Renamenx
}

func (c RenamenxCmd) exec(s storage.Storage, _ search.Engine) error {
	s.Rename(c.Key, c.NewKey)
	return nil
}

type FtCreateCmd struct {
	Index idxmodel.Index
}

func (c FtCreateCmd) Name() string {
	return FtCreate
}

func (c FtCreateCmd) exec(_ storage.Storage, engine search.Engine) error {
	prefixes := c.Index.Prefixes
	if prefixes == nil || len(prefixes) == 0 {
		prefixes = []string{"*"}
	}
	fields := make([]string, len(c.Index.Schema))
	for i, f := range c.Index.Schema {
		fields[i] = f.Name
	}
	engine.CreateIndex(c.Index.Name, prefixes, fields)
	return nil
}
