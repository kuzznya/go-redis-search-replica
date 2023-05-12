package idxmodel

import (
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

type Index struct {
	Name     string
	Prefixes []string
	Schema   []Field
}

type Field struct {
	Name string
	Type string
}

func Parse(next func() (string, bool), checkNext func(expected string) bool) (*Index, error) {
	name, ok := next()
	if !ok {
		return nil, errors.New("ERR index name not provided")
	}

	var prefixes []string
	if checkNext("prefix") {
		numPrefixesStr, ok := next()
		if !ok {
			return nil, errors.New("ERR failed to parse index prefixes")
		}

		numPrefixes, err := strconv.Atoi(numPrefixesStr)
		if err != nil || numPrefixes < 0 {
			return nil, errors.New("ERR failed to parse number of prefixes")
		}

		prefixes = make([]string, numPrefixes)
		for i := 0; i < numPrefixes; i++ {
			prefix, ok := next()
			if !ok {
				return nil, errors.Errorf("ERR number of prefixes less than defined num %d", numPrefixes)
			}
			prefixes[i] = prefix
		}
	}

	if !checkNext("schema") {
		return nil, errors.New("ERR schema not provided")
	}

	schema := make([]Field, 0)
	for {
		field, ok := next()
		if !ok {
			break
		}

		fieldType, ok := next()
		if !ok {
			return nil, errors.New("ERR field type not defined")
		}
		if strings.ToLower(fieldType) != "text" {
			return nil, errors.New("ERR unknown field type")
		}

		schema = append(schema, Field{Name: field, Type: strings.ToLower(fieldType)})
	}

	return &Index{Name: name, Prefixes: prefixes, Schema: schema}, nil
}
