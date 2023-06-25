package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		time := time.Since(raft.DebugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d ", time)
		format = prefix + format
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
	T     OType
	Key   string
	Value string
	UUID  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dataset map[string]string
	his     map[int64]struct{}
	Log     []Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	defer kv.mu.Unlock()
	// Your code here.
	reply.Success = false
	kv.mu.Lock()
	_, ok := kv.his[args.UUID]
	DPrintf("K%d 收到内容%+v", kv.me, args, ok)
	// for _, ok := kv.his[args.UUID]; !ok; _, ok = kv.his[args.UUID] {
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

	// kv.mu.Unlock()
	// time.Sleep(time.Duration(100) * time.Millisecond)
	// kv.mu.Lock()
	for cnt := 0; cnt < 100; cnt++ {

		kv.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)

		kv.mu.Lock()
		tterm, isLeader := kv.rf.GetState()
		DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Get index: %v len(log): %v args: %+v", kv.me, isLeader, term, tterm, index, len(kv.Log), args)
		if !isLeader || term != tterm {
			reply.Err = "NoLeader"
			DPrintf("K%d Get 失败2", kv.me)
			return
		}
		if _, ok := kv.his[args.UUID]; ok {
			DPrintf("K%d Server添加成功index: %d len(kv.Log): %d kv.Log: %+v date: %+v", kv.me, index, len(kv.Log), kv.Log, kv.dataset)
			reply.Value = kv.dataset[args.Key]
			reply.Success = true
			return
		}
	}
	DPrintf("K%d Get %v 超时", args.UUID)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer kv.mu.Unlock()
	var index int
	var isLeader bool
	var term int

	kv.mu.Lock()
	DPrintf("K%d 收到内容%+v", kv.me, args)
	// for _, ok := kv.his[args.UUID]; !ok; _, ok = kv.his[args.UUID] {
	reply.Success = false
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
		DPrintf("K%d isLeader: %v term: %d tterm: %d 等待PutAppend index: %v len(log): %v args: %+v", kv.me, isLeader, term, tterm, index, len(kv.Log), args)
		if !isLeader || term != tterm {
			reply.Err = "NoLeader"
			DPrintf("K%d Put 失败2", kv.me)
			return
		}
		if _, ok := kv.his[args.UUID]; ok {
			DPrintf("K%d Server添加成功index: %d len(kv.Log): %d kv.Log: %+v date: %+v", kv.me, index, len(kv.Log), kv.Log, kv.dataset)
			reply.Success = true
			return
		}
	}
	// kv.mu.Unlock()
	// time.Sleep(time.Duration(100) * time.Millisecond)
	// kv.mu.Lock()
	DPrintf("K%d PutAppend %v 超时", args.UUID)
	// }
}

func (kv *KVServer) RefreshData() {
	ms := time.Duration(1)
	for kv.killed() == false {
		select {
		case v := <-kv.applyCh:
			DPrintf("K%d server端接收到日志%+v 锁外", kv.me, v)
			kv.mu.Lock()
			DPrintf("K%d server端接收到日志%+v 锁内", kv.me, v)
			p, ok := v.Command.(Op)
			if !ok {
				panic("K%d command to Op{} 转换错误")
			}

			// 如果是之前的内容，那么就不采纳
			if _, ok := kv.his[p.UUID]; ok {
				DPrintf("K%d 之前存在%+v", kv.me, p.UUID)
				kv.mu.Unlock()
				continue
			}

			kv.Log = append(kv.Log, p)
			if p.T.IsAppend() {
				key := p.Key
				kv.dataset[key] += p.Value
			} else if p.T.IsPut() {
				kv.dataset[p.Key] = p.Value
			}

			kv.his[p.UUID] = struct{}{}
			DPrintf("K%d 增加: %+v 增加hist: %+v 长度: %d", kv.me, p, p.UUID, len(kv.Log))

			kv.mu.Unlock()
		default:
			// DPrintf("K%d sleep", kv.me)
			time.Sleep(ms * time.Millisecond)
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.Log = make([]Op, 1)
	kv.dataset = make(map[string]string)
	kv.his = make(map[int64]struct{})
	go kv.RefreshData()
	return kv
}
