package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	n := len(ck.servers)
	args := GetArgs{}
	args.Key = key
	args.UUID = nrand()
	DPrintf("Client %v Get %v", args.UUID, key)
	for {
		for i := 0; i < n; i++ {
			reply := GetReply{}
			DPrintf("client端发送内容Get-> K%d args: %+v", i, args)
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err.noError() && reply.Success {

				DPrintf("Get: %+v: value: %+v成功", args, reply.Value)
				return reply.Value

			} else {
				DPrintf("Get失败 K%d args: %+v ok: %+v reply: %+v", i, args, ok, reply)
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	n := len(ck.servers)
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.UUID = nrand()
	DPrintf("Client %v PutAppend %v %v %v", args.UUID, key, value, op)
	for {

		for i := 0; i < n; i++ {
			reply := PutAppendReply{}
			DPrintf("client端发送内容Put-> K%d args: %+v", i, args)
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err.noError() && reply.Success {

					DPrintf("PutApeend: %v %+v: %+v成功", op, key, value)
					return
				}
			} else {
				DPrintf("PutAppend: 失败 K%d %v: %v", i, key, value)
			}
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
