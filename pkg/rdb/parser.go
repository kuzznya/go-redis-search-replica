package rdb

import (
	"bufio"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/kuzznya/go-redis-search-replica/pkg/exec"
	"github.com/kuzznya/go-redis-search-replica/pkg/rdb/core"
	"github.com/kuzznya/go-redis-search-replica/pkg/rdb/model"
)

func Parse(r *bufio.Reader, e exec.Executor) error {
	decoder := core.NewDecoder(r)
	var procErr error
	err := decoder.Parse(func(o model.RedisObject) bool {
		log.Debugf("Key %s (type %s)", o.GetKey(), o.GetType())

		if o.GetType() != "hash" {
			return true
		}

		hash := o.(*model.HashObject)

		args := make([]exec.HSetArg, len(hash.Hash))
		i := 0
		for k, v := range hash.Hash {
			args[i] = exec.HSetArg{Field: k, Value: v}
			i++
		}
		err := e.Exec(exec.HSetCmd{Key: o.GetKey(), Args: args})
		if err != nil {
			procErr = err
			return false
		}

		log.Debugf("Hash %s: %s", hash.Key, hash.Hash)

		return true
	})
	if procErr != nil {
		return errors.Wrap(procErr, "failed to process RDB")
	}
	return errors.Wrap(err, "failed to parse RDB")
}
