package resp

import (
	"github.com/kuzznya/go-redis-search-replica/pkg/exec"
	"github.com/kuzznya/go-redis-search-replica/pkg/resp/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"reflect"
	"strconv"
	"strings"
)

func NewParser(rd io.Reader) *Parser {
	c := &countingReader{r: rd}
	return &Parser{rd: proto.NewReader(c), c: c}
}

type Parser struct {
	rd *proto.Reader
	c  *countingReader
}

// ParseCmd returns the data, count of bytes read and error (if any)
func (p *Parser) ParseCmd() (exec.Command, uint64, error) {
	data, err := p.rd.ReadReply(sliceParser)
	offset := p.c.offset
	p.c.offset = 0

	if err != nil {
		return nil, offset, err
	}

	parts, ok := data.([]any)
	if !ok {
		return nil, offset, nil // TODO check if nil as err is ok
	}

	name, ok := parts[0].(string)
	if !ok {
		return nil, offset, nil
	}

	name = strings.ToUpper(name)
	switch name {
	case exec.Set:
		cmd, err := parseSet(parts[1:])
		return cmd, offset, err
	case exec.Hmset:
		fallthrough
	case exec.Hset:
		cmd, err := parseHset(parts[1:])
		return cmd, offset, err
	case exec.Hsetnx:
		cmd, err := parseHsetnx(parts[1:])
		return cmd, offset, err
	case exec.Hincrby:
		cmd, err := parseHincrby(parts[1:])
		return cmd, offset, err
	case exec.Hdel:
		cmd, err := parseHdel(parts[1:])
		return cmd, offset, err
	case exec.Del:
		cmd, err := parseDel(parts[1:])
		return cmd, offset, err
	case exec.Rename:
		cmd, err := parseRename(parts[1:], false)
		return cmd, offset, err
	case exec.Renamenx:
		cmd, err := parseRename(parts[1:], true)
		return cmd, offset, err
	}

	log.Tracef("Skipping cmd %+v", parts)

	return nil, offset, nil
}

func parseSet(args []any) (exec.Command, error) {
	if len(args) == 0 {
		log.Warnln("Not enough args for SET, skipping")
		return nil, nil
	}

	key, ok := args[0].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse SET: unknown key type %s", reflect.TypeOf(args[0]).Name())
	}

	return exec.SetCmd{Key: key}, nil
}

func parseHset(args []any) (exec.Command, error) {
	if len(args) < 2 {
		log.Warnln("Not enough args for HSET, skipping")
		return nil, nil
	}

	key, ok := args[0].(string)
	if !ok {
		return nil, errors.New("Failed to parse HSET: unknown key type")
	}

	hsetArgs := make([]exec.HSetArg, (len(args)-1)/2)
	for i := 1; i < len(args)-1; i += 2 {
		field, ok := args[i].(string)
		if !ok {
			return nil, errors.New("Failed to parse HSET")
		}
		hsetArgs[(i-1)/2].Field = field

		strValue, ok := args[i+1].(string)
		if !ok {
			return nil, errors.Errorf("Failed to parse HSET: unknown value type %s", reflect.TypeOf(args[i+1]).Name())
		}

		hsetArgs[(i-1)/2].Value = []byte(strValue)
	}

	return exec.HSetCmd{Key: key, Args: hsetArgs}, nil
}

func parseHsetnx(args []any) (exec.Command, error) {
	if len(args) < 3 {
		log.Warnln("Not enough args for HSETNX, skipping")
		return nil, nil
	}

	key, ok := args[0].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse HINCRBY: unknown key type %s", reflect.TypeOf(args[0]).Name())
	}

	field, ok := args[1].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse HINCRBY: unknown field name type %s", reflect.TypeOf(args[1]).Name())
	}

	value, ok := args[2].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse HSETNX: unknown value type %s", reflect.TypeOf(args[2]).Name())
	}

	return exec.HsetnxCmd{Key: key, Field: field, Value: []byte(value)}, nil
}

func parseHincrby(args []any) (exec.Command, error) {
	if len(args) < 3 {
		log.Warnln("Not enough args for HINCRBY, skipping")
		return nil, nil
	}

	key, ok := args[0].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse HINCRBY: unknown key type %s", reflect.TypeOf(args[0]).Name())
	}

	field, ok := args[1].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse HINCRBY: unknown field name type %s", reflect.TypeOf(args[1]).Name())
	}

	incrStr, ok := args[2].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse HINCRBY: unknown increment type %s", reflect.TypeOf(args[2]).Name())
	}

	incr, err := strconv.ParseInt(incrStr, 10, 64)
	if err != nil {
		return nil, errors.Errorf("Failed to parse HINCRBY: %s", err.Error())
	}

	return exec.HincrbyCmd{Key: key, Field: field, Value: incr}, nil
}

func parseHdel(args []any) (exec.Command, error) {
	if len(args) < 2 {
		log.Warnln("Not enough args for HDEL, skipping")
		return nil, nil
	}

	key, ok := args[0].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse HDEL: unknown key type %s", reflect.TypeOf(args[0]).Name())
	}

	fields := make([]string, len(args)-1)
	for i, field := range args[1:] {
		fields[i], ok = field.(string)
		if !ok {
			return nil, errors.Errorf("Failed to parse HDEL: unknown field name type %s", reflect.TypeOf(field).Name())
		}
	}
	return exec.HDelCmd{Key: key, Fields: fields}, nil
}

func parseDel(args []any) (exec.Command, error) {
	if len(args) == 0 {
		log.Warnln("Not enough args for DEL, skipping")
		return nil, nil
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		key, ok := arg.(string)
		if !ok {
			return nil, errors.Errorf("Failed to parse DEL: unknown key type %s", reflect.TypeOf(args[0]).Name())
		}
		keys[i] = key
	}

	return exec.DelCmd{Keys: keys}, nil
}

func parseRename(args []any, nx bool) (exec.Command, error) {
	if len(args) < 2 {
		log.Warnln("Not enough args for RENAME/RENAMENX, skipping")
		return nil, nil
	}

	key, ok := args[0].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse RENAME/RENAMENX: unknown key type %s", reflect.TypeOf(args[0]).Name())
	}

	newKey, ok := args[1].(string)
	if !ok {
		return nil, errors.Errorf("Failed to parse RENAME/RENAMENX: unknown key type %s", reflect.TypeOf(args[0]).Name())
	}

	if nx {
		return exec.RenamenxCmd{Key: key, NewKey: newKey}, nil
	} else {
		return exec.RenameCmd{Key: key, NewKey: newKey}, nil
	}
}

// sliceParser implements proto.MultiBulkParse.
func sliceParser(rd *proto.Reader, n int64) (interface{}, error) {
	vals := make([]interface{}, n)
	for i := 0; i < len(vals); i++ {
		v, err := rd.ReadReply(sliceParser)
		if err != nil {
			if err == proto.Nil {
				vals[i] = nil
				continue
			}
			if err, ok := err.(proto.RedisError); ok {
				vals[i] = err
				continue
			}
			return nil, err
		}
		vals[i] = v
	}
	return vals, nil
}

type countingReader struct {
	r      io.Reader
	offset uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if err != nil {
		return n, err
	}
	c.offset += uint64(n)
	return n, err
}
