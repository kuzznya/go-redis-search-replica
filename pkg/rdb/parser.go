package rdb

import (
	"bufio"
	"encoding/json"
	"github.com/kuzznya/go-redis-search-replica/pkg/exec"
	"github.com/kuzznya/go-redis-search-replica/pkg/idxmodel"
	"github.com/kuzznya/rdb/core"
	"github.com/kuzznya/rdb/model"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const ftsIndexType = "fts-index"

func Parse(r *bufio.Reader, e exec.Executor) error {
	decoder := core.NewDecoder(r).WithSpecialType(ftsIndexType, parseFtsIndex)
	var procErr error
	err := decoder.Parse(func(o model.RedisObject) bool {
		log.Debugf("Key %s (type %s)", o.GetKey(), o.GetType())

		if o.GetType() == ftsIndexType {
			mtObj := o.(*model.ModuleTypeObject)
			idx := mtObj.Value.(*idxmodel.Index)
			log.Infof("Index: %+v", idx)
		}

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

func parseFtsIndex(h core.ModuleTypeHandler, encVersion int) (interface{}, error) {
	opcode, _, err := h.ReadLength()
	if err != nil {
		return nil, err
	}

	if opcode != core.OpcodeString {
		return nil, errors.New("unsupported module type value code")
	}

	data, err := h.ReadString()
	if err != nil {
		return nil, err
	}

	idx := idxmodel.Index{}
	err = json.Unmarshal(data, &idx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse index")
	}

	opcode, _, err = h.ReadLength()
	if err != nil {
		return nil, errors.Wrap(err, "expected module type value EOF")
	}
	if opcode != core.OpcodeEOF {
		return nil, errors.New("expected module type value EOF")
	}

	return &idx, nil
}
