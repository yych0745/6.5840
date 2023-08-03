package shardkv

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const debug = true
const debugLog = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	pc, _, _, _ := runtime.Caller(2)
	name := runtime.FuncForPC(pc).Name()
	if debug {
		time := time.Since(raft.DebugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d ", time)
		format = prefix + " " + name + " " + format
		log.Printf(format, a...)
	}
	return
}

type OType string

func (t OType) IsGet() bool {
	return t == "Get"
}

func (t OType) IsAppend() bool {
	return t == "Append"
}

func (t OType) IsPut() bool {
	return t == "Put"
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UUID  int64
	Key   string
	Value string
	T     OType
}

type shardState int

const (
	serving shardState = iota
	pending
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state shardState
	dead  int32

	config shardctrler.Config

	DB       map[string]string
	His      map[int64]struct{}
	DelteHis map[int64]struct{}
	Log      []Op
	mck      *shardctrler.Clerk
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("K%d gid: %d database %v", kv.me, kv.gid, kv.DB)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer kv.mu.Unlock()
	// Your code here.
	kv.mu.Lock()

	// 如果当前是pending状态，那么就等
	for kv.state == pending {
	}
	if args.Gid != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

	_, ok := kv.His[args.UUID]
	DPrintf("K%d 收到内容%+v ok: %v", kv.me, args, ok)
	if ok {
		reply.Value = kv.DB[args.Key]
		reply.Err = OK
		return
	}
	op := Op{}
	op.Key = args.Key
	op.T = "Get"
	op.UUID = args.UUID
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "NoLeader"
		DPrintf("K%d Get 失败1", kv.me)
		return
	}

	for cnt := 0; cnt < 100; cnt++ {

		kv.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)

		kv.mu.Lock()
		tterm, isLeader := kv.rf.GetState()
		if debugLog {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Get index: %v len(log): %v args: %+v", kv.me, isLeader, term, tterm, index, len(kv.Log), args)
		} else {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Get index: %v len: %v args: %+v", kv.me, isLeader, term, tterm, index, len(kv.His), args)
		}
		if !isLeader || term != tterm {
			reply.Err = "NoLeader"
			DPrintf("K%d Get 失败2", kv.me)
			return
		}
		if _, ok := kv.His[args.UUID]; ok {
			if debugLog {
				DPrintf("K%d Get Server添加成功index: %d len(kv.Log): %d kv.Log: %+v date: %+v", kv.me, index, len(kv.Log), kv.Log, kv.DB)
			} else {
				DPrintf("K%d Get Server添加成功index: %d len: %d date: %+v", kv.me, index, len(kv.His), kv.DB)
			}

			for kv.state == pending {
			}
			if args.Gid != kv.gid {
				reply.Err = ErrWrongGroup
				return
			}

			reply.Value = kv.DB[args.Key]
			reply.Err = OK
			return
		}
	}
	DPrintf("K%d Get %v 超时", args.UUID)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer kv.mu.Unlock()
	var index int
	var isLeader bool
	var term int

	kv.mu.Lock()

	// 如果当前是pending状态，那么就等
	for kv.state == pending {
	}

	// 判断是否是当前的gid

	DPrintf("K%d 收到内容%+v", kv.me, args)
	_, ok := kv.His[args.UUID]
	DPrintf("K%d 收到内容%+v ok: %v", kv.me, args, ok)
	if ok {
		reply.Err = OK
		DPrintf("K%d put args: %+v 结果1: %+v", kv.me, args, kv)
		return
	}
	op := Op{}
	op.Key = args.Key
	op.T = OType(args.Op)
	op.Value = args.Value
	op.UUID = args.UUID

	index, term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = "NoLeader"
		DPrintf("K%d Put 失败1", kv.me)
		return
	}
	for cnt := 0; cnt < 100; cnt++ {

		kv.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)

		kv.mu.Lock()

		tterm, isLeader := kv.rf.GetState()
		if debugLog {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待PutAppend index: %v len(log): %v args: %+v", kv.me, isLeader, term, tterm, index, len(kv.Log), args)
		} else {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待PutAppend index: %v len: %v args: %+v", kv.me, isLeader, term, tterm, index, len(kv.His), args)
		}
		if !isLeader || term != tterm {
			reply.Err = "NoLeader"
			DPrintf("K%d Put 失败2", kv.me)
			return
		}
		if _, ok := kv.His[args.UUID]; ok {
			if debugLog {
				DPrintf("K%d PutAppend Server添加成功index: %d len(kv.Log): %d kv.Log: %+v date: %+v", kv.me, index, len(kv.Log), kv.Log, kv.DB)
			} else {
				DPrintf("K%d PutAppend Server添加成功index: %d len: %d date: %+v", kv.me, index, len(kv.His), kv.DB)
			}

			for kv.state == pending {
			}
			if args.Gid != kv.gid {
				reply.Err = ErrWrongGroup
				return
			}

			reply.Err = OK
			DPrintf("K%d args: %+v put 结果2: %+v", kv.me, args, kv)
			return
		}
	}
	DPrintf("K%d PutAppend %v 超时", args.UUID)

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) RefreshData() {
	ms := time.Duration(1)
	for kv.killed() == false {
		select {
		case v := <-kv.applyCh:
			DPrintf("K%d server端接收到日志%+v", kv.me, v)
			p, ok := v.Command.(Op)
			if !ok {
				if v.SnapshotValid {
					DPrintf("RefreshData: 收到snapshot: %v", v.Snapshot)
					// kv.ReadSnapshot(v.Snapshot)
					continue
				} else {
					panic(fmt.Sprintf("K%d command %v to Op{} 转换错误", kv.me, v))
				}
			}
			kv.mu.Lock()

			// 如果是之前的内容，那么就不采纳
			// if _, ok := kv.DelteHis[p.UUID]; ok {
			// 	delete(kv.DelteHis, p.UUID)
			// 	kv.mu.Unlock()
			// 	continue
			// }
			if _, ok := kv.His[p.UUID]; ok {
				DPrintf("K%d 之前存在%+v", kv.me, p.UUID)
				kv.mu.Unlock()
				continue
			}

			if debugLog {
				kv.Log = append(kv.Log, p)
			}
			if p.T.IsAppend() {
				key := p.Key
				kv.DB[key] += p.Value
			} else if p.T.IsPut() {
				kv.DB[p.Key] = p.Value
			}

			kv.His[p.UUID] = struct{}{}

			if debugLog {
				DPrintf("K%d 增加: %+v 增加hist: %+v 长度: %d", kv.me, p, p.UUID, len(kv.Log))
			} else {
				DPrintf("K%d 增加: %+v 增加hist: %+v 长度: %d", kv.me, p, p.UUID, len(kv.His))
			}

			kv.mu.Unlock()
		default:
			// DPrintf("K%d sleep", kv.me)
			time.Sleep(ms * time.Millisecond)
		}
		// kv.Snapshot()
	}
}

func (kv *ShardKV) transform() {
}

func (kv *ShardKV) RefreshConfig() {
	ms := time.Duration(100)
	for kv.killed() == false {
		_, leader := kv.rf.GetState()
		if leader {
			kv.mu.Lock()
			newConfig := kv.mck.Query(-1)
			if newConfig.Num == kv.config.Num {
				kv.state = serving
			} else {
				kv.state = pending
				kv.config = newConfig
				kv.transform()
				kv.state = serving
			}
			kv.mu.Unlock()
		}
		time.Sleep(ms * time.Microsecond)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.DB = make(map[string]string)
	kv.His = make(map[int64]struct{})
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config.Num = -1
	go kv.RefreshConfig()
	go kv.RefreshData()
	return kv
}
