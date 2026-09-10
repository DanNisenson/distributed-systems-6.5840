package kvsrv

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

var count = 0
var mu sync.Mutex

type Clerk struct {
	clnt   *tester.Clnt
	server string

	id     int
	logger Logger
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {

	ck := &Clerk{clnt: clnt, server: server}

	mu.Lock()
	count += 1
	ck.id = count
	mu.Unlock()

	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{}
	args.Key = key
	reply := rpc.GetReply{}

	ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)

	if !ok {
		// ck.logger.Error("client_id", ck.id, "status", "get_failed")
		return ck.Get(key)
	} else {
		// ck.logger.Info("client_id", ck.id, "status", "get_success", "reply", reply)
		return reply.Value, reply.Version, reply.Err
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	err := ck.put(key, value, version)
	ck.logger.Info("[Client] PUT done. id: %d. status: %s", ck.id, err)
	return err
}

func (ck *Clerk) put(key, value string, version rpc.Tversion, retry ...bool) rpc.Err {
	args := rpc.PutArgs{}
	args.Key = key
	args.Value = value
	args.Version = version
	reply := rpc.PutReply{}

	ck.logger.Error("[Client] PUT attempt. id: %d", ck.id)

	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

	if !ok {
		ck.logger.Error("[Client] PUT failed. Retry. id: %d", ck.id)
		return ck.put(key, value, version, true)
	} else {
		ck.logger.Error("[Client] PUT success. id: %d. retryLen: %d", ck.id, len(retry))
		if len(retry) > 0 && retry[0] && reply.Err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}
		return reply.Err
	}
}
