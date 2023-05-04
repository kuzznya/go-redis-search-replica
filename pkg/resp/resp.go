package resp

import (
	"github.com/kuzznya/go-redis-search-replica/pkg/exec"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"
	"io"
	"strconv"
	"strings"
)

func NewParser(rd io.Reader) *Parser {
	c := &countingReader{r: rd}
	return &Parser{rd: redcon.NewReader(c), c: c}
}

type Parser struct {
	rd *redcon.Reader
	c  *countingReader
}

// ParseCmd returns the data, count of bytes read and error (if any)
func (p *Parser) ParseCmd() (exec.Command, uint64, error) {
	data, err := p.rd.ReadCommand()
	offset := p.c.offset
	p.c.offset = 0

	if err != nil {
		return nil, offset, err
	}

	parts := data.Args

	name := string(parts[0])

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

func parseSet(args [][]byte) (exec.Command, error) {
	if len(args) == 0 {
		log.Warnln("Not enough args for SET, skipping")
		return nil, nil
	}

	key := string(args[0])
	return exec.SetCmd{Key: key}, nil
}

func parseHset(args [][]byte) (exec.Command, error) {
	if len(args) < 2 {
		log.Warnln("Not enough args for HSET, skipping")
		return nil, nil
	}

	key := string(args[0])

	hsetArgs := make([]exec.HSetArg, (len(args)-1)/2)
	for i := 1; i < len(args)-1; i += 2 {
		field := string(args[i])
		hsetArgs[(i-1)/2].Field = field
		hsetArgs[(i-1)/2].Value = args[i+1]
	}

	return exec.HSetCmd{Key: key, Args: hsetArgs}, nil
}

func parseHsetnx(args [][]byte) (exec.Command, error) {
	if len(args) < 3 {
		log.Warnln("Not enough args for HSETNX, skipping")
		return nil, nil
	}

	key := string(args[0])
	field := string(args[1])
	value := args[2]

	return exec.HsetnxCmd{Key: key, Field: field, Value: value}, nil
}

func parseHincrby(args [][]byte) (exec.Command, error) {
	if len(args) < 3 {
		log.Warnln("Not enough args for HINCRBY, skipping")
		return nil, nil
	}

	key := string(args[0])
	field := string(args[1])
	incrStr := string(args[2])

	incr, err := strconv.ParseInt(incrStr, 10, 64)
	if err != nil {
		return nil, errors.Errorf("Failed to parse HINCRBY: %s", err.Error())
	}

	return exec.HincrbyCmd{Key: key, Field: field, Value: incr}, nil
}

func parseHdel(args [][]byte) (exec.Command, error) {
	if len(args) < 2 {
		log.Warnln("Not enough args for HDEL, skipping")
		return nil, nil
	}

	key := string(args[0])

	fields := make([]string, len(args)-1)
	for i, field := range args[1:] {
		fields[i] = string(field)
	}
	return exec.HDelCmd{Key: key, Fields: fields}, nil
}

func parseDel(args [][]byte) (exec.Command, error) {
	if len(args) == 0 {
		log.Warnln("Not enough args for DEL, skipping")
		return nil, nil
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		key := string(arg)
		keys[i] = key
	}

	return exec.DelCmd{Keys: keys}, nil
}

func parseRename(args [][]byte, nx bool) (exec.Command, error) {
	if len(args) < 2 {
		log.Warnln("Not enough args for RENAME/RENAMENX, skipping")
		return nil, nil
	}

	key := string(args[0])
	newKey := string(args[1])

	if nx {
		return exec.RenamenxCmd{Key: key, NewKey: newKey}, nil
	} else {
		return exec.RenameCmd{Key: key, NewKey: newKey}, nil
	}
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
