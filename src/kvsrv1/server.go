package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu     sync.Mutex
	store  map[string]ValueVersion
	logger *Logger
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.logger = NewLogger()
	kv.store = make(map[string]ValueVersion)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	vv, ok := kv.store[args.Key]

	kv.logger.Info("[SERVER] GET in - key: %s. val: %s. ver: %d", args.Key, vv.value, vv.version)

	if !ok {
		reply.Err = rpc.ErrNoKey
		kv.logger.Info("[SERVER] GET out - key: %s. err: %s", args.Key, rpc.ErrNoKey)
	} else {
		reply.Value = vv.value
		reply.Version = vv.version
		reply.Err = rpc.OK
		kv.logger.Info("[SERVER] GET out - key: %s. val: %s. ver: %d", args.Key, vv.value, vv.version)
	}

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	vv, ok := kv.store[args.Key]

	if ok {
		if args.Version == vv.version {
			kv.store[args.Key] = ValueVersion{
				value:   args.Value,
				version: args.Version + 1,
			}
			reply.Err = rpc.OK
			kv.logger.Info("[SERVER] PUT out - key: %s. val: %s. ver: %d", args.Key, args.Value, args.Version)
		} else {
			reply.Err = rpc.ErrVersion
			kv.logger.Info("[SERVER] PUT out - key: %s. err: %s", args.Key, rpc.ErrVersion)
		}
	} else if args.Version == 0 {
		kv.store[args.Key] = ValueVersion{
			value:   args.Value,
			version: 1,
		}
		reply.Err = rpc.OK
		kv.logger.Info("[SERVER] PUT out - key: %s. val: %s. ver: %d", args.Key, args.Value, args.Version)
	} else {
		reply.Err = rpc.ErrNoKey
		kv.logger.Info("[SERVER] PUT out - key: %s. err: %s", args.Key, rpc.ErrNoKey)
	}

}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
