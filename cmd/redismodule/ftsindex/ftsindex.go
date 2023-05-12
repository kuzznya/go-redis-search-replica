package main

import (
	"encoding/json"
	"github.com/kuzznya/go-redis-search-replica/pkg/idxmodel"
	"github.com/kuzznya/go-redis-search-replica/redismodule"
	"strings"
	"unsafe"
)

const ModuleName = "fts-index"

var IdxType redismodule.ModuleType

type Index struct {
	Name     string
	Prefixes []string
	Schema   []Field
}

type Field struct {
	Name string
	Type string
}

func main() {
	panic("Not intended for run")
}

func init() {
	redismodule.Mod = RediSearchMod()
}

func RediSearchMod() *redismodule.Module {
	mod := redismodule.NewMod()
	mod.Name = "redisearch"
	mod.Version = 1
	mod.SemVer = "0.0.1"
	mod.Author = "kuzznya"
	mod.Website = "https://github.com/kuzznya/go-redis-search-replica"
	mod.Desc = "Full-text search implementation for Redis in Go"
	mod.DataTypes = []redismodule.DataType{createIndexType()}
	mod.Commands = []redismodule.Command{createCmdFtCreate()}
	mod.AfterInit = func(ctx redismodule.Ctx, args []redismodule.String) error {
		IdxType = redismodule.GetModuleDataType(ModuleName)
		return nil
	}
	return mod
}

func createIndexType() redismodule.DataType {
	return redismodule.DataType{
		Name:   ModuleName,
		EncVer: 1,
		Desc:   "Full-text index",

		Free: func(ptr unsafe.Pointer) {
			redismodule.LogDebug("Free began")
			idx := (*Index)(ptr)
			redismodule.LogDebug("Free %s - no-op as it is Go pointer", idx.Name)
		},

		RdbLoad: func(rdb redismodule.IO, encver int) unsafe.Pointer {
			data := rdb.LoadString()
			idx := Index{}
			err := json.Unmarshal([]byte(data.String()), &idx)
			if err != nil {
				redismodule.LogError("Failed to load index from RDB")
				return nil
			}
			redismodule.LogDebug("Loaded index %s", idx.Name)
			return unsafe.Pointer(&idx)
		},

		RdbSave: func(rdb redismodule.IO, value unsafe.Pointer) {
			idx := (*Index)(value)
			data, err := json.Marshal(idx)
			if err != nil {
				redismodule.LogError("Failed to save index to RDB")
				return
			}

			size := len(data)
			rdb.SaveStringBuffer(data, size)
		},
	}
}

func createCmdFtCreate() redismodule.Command {
	return redismodule.Command{
		Name:     "ft.create",
		Flags:    redismodule.BuildCommandFlag(redismodule.CF_WRITE, redismodule.CF_FAST, redismodule.CF_DENY_OOM),
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: ftCreate,
	}
}

func ftCreate(cmd redismodule.CmdContext) int {
	ctx, args := cmd.Ctx, cmd.Args
	ctx.AutoMemory()

	if len(args) < 2 {
		ctx.ReplyWithError("ERR index name not provided")
		return redismodule.ERR
	}

	name := args[1]
	key, ok := openKeyRW(ctx, name)
	if !ok {
		return redismodule.ERR
	}

	pos := 0
	next := func() (string, bool) {
		pos++
		if len(args) <= pos {
			return "", false
		}
		return args[pos].String(), true
	}
	checkNext := func(expected string) bool {
		if len(args) <= pos+1 {
			return false
		}
		arg := strings.ToLower(args[pos+1].String())
		if arg == expected {
			pos++
			return true
		}
		ctx.LogDebug("Unexpected arg: %s, expected: %s", arg, expected)
		return false
	}

	idx, err := idxmodel.Parse(next, checkNext)
	if err != nil {
		ctx.ReplyWithError(err.Error())
		return redismodule.ERR
	}

	status := key.ModuleTypeSetValue(IdxType, unsafe.Pointer(idx))
	if status == redismodule.ERR {
		ctx.ReplyWithError("ERR failed to set module type value")
		return redismodule.ERR
	}

	key.CloseKey()

	ctx.ReplicateVerbatim()

	return ctx.ReplyWithOK()
}

func openKeyRW(ctx redismodule.Ctx, k redismodule.String) (redismodule.Key, bool) {
	key := ctx.OpenKey(k, redismodule.WRITE|redismodule.READ)
	if !key.IsEmpty() && key.ModuleTypeGetType() != IdxType {
		ctx.ReplyWithError(redismodule.ERRORMSG_WRONGTYPE)
		return key, false
	}
	return key, true
}

func mustOpenKeyData(ctx redismodule.Ctx, k redismodule.String, mode int) (*Index, bool) {
	key := ctx.OpenKey(k, mode)
	if key.IsEmpty() {
		ctx.ReplyWithError("ERR Index not exists")
		return nil, false
	}
	if key.ModuleTypeGetType() != IdxType {
		ctx.ReplyWithError(redismodule.ERRORMSG_WRONGTYPE)
		return nil, false
	}
	return (*Index)(key.ModuleTypeGetValue()), true
}
