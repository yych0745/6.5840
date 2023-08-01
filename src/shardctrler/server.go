package shardctrler

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

var debug = true
var debugLog = false

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

func (t OType) IsJoin() bool {
	return t == "Join"
}

func (t OType) IsLeave() bool {
	return t == "Leave"
}

func (t OType) IsQuery() bool {
	return t == "Query"
}

func (t OType) IsMove() bool {
	return t == "Move"
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	T       OType
	Servers map[int][]string
	Gids    []int
	UUID    int64
	Shard   int
	Gid     int
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	His     map[int64]struct{}
	Log     []Op
	HisQue  Queue
	dead    int32
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	defer sc.mu.Unlock()
	sc.mu.Lock()
	_, ok := sc.His[args.UUID]
	if ok {
		reply.WrongLeader = false
		return
	}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "不是leader"
	}
	op := Op{}
	op.T = "Join"
	op.UUID = args.UUID
	op.Servers = args.Servers
	index, term, isLeader := sc.rf.Start(op)
	for cnt := 0; cnt < 100; cnt++ {

		sc.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)

		sc.mu.Lock()
		tterm, isLeader := sc.rf.GetState()
		if debugLog {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Join index: %v len(log): %v args: %+v", sc.me, isLeader, term, tterm, index, len(sc.Log), args)
		} else {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Join index: %v len: %v args: %+v", sc.me, isLeader, term, tterm, index, len(sc.His), args)
		}
		if !isLeader || term != tterm {
			reply.Err = "NoLeader"
			DPrintf("K%d Join 失败2", sc.me)
			return
		}
		if _, ok := sc.His[args.UUID]; ok {
			if debugLog {
				DPrintf("K%d Join Server添加成功index: %d ", sc.me, index)
			} else {
				DPrintf("K%d Join Server添加成功index: %d len: %d ", sc.me, index, len(sc.His))
			}
			DPrintf("K%d Join Config %v", sc.me, sc.configs)
			reply.WrongLeader = false
			reply.Config = sc.configs[len(sc.configs)-1]
			return
		}
	}
	DPrintf("K%d Join %v 超时", sc.me, args.UUID)
	reply.Err = Err(fmt.Sprintf("K%d Join timeout!", sc.me))
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	defer sc.mu.Unlock()
	sc.mu.Lock()
	_, ok := sc.His[args.UUID]
	if ok {
		reply.WrongLeader = false
		return
	}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "不是leader"
	}
	op := Op{}
	op.T = "Leave"
	op.UUID = args.UUID
	op.Gids = args.GIDs
	index, term, isLeader := sc.rf.Start(op)
	for cnt := 0; cnt < 100; cnt++ {

		sc.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)

		sc.mu.Lock()
		tterm, isLeader := sc.rf.GetState()
		if debugLog {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Leave index: %v len(log): %v args: %+v", sc.me, isLeader, term, tterm, index, len(sc.Log), args)
		} else {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Leave index: %v len: %v args: %+v", sc.me, isLeader, term, tterm, index, len(sc.His), args)
		}
		if !isLeader || term != tterm {
			reply.Err = "NoLeader"
			DPrintf("K%d Leave 失败2", sc.me)
			return
		}
		if _, ok := sc.His[args.UUID]; ok {
			if debugLog {
				DPrintf("K%d Leave Server添加成功index: %d ", sc.me, index)
			} else {
				DPrintf("K%d Leave Server添加成功index: %d len: %d ", sc.me, index, len(sc.His))
			}
			DPrintf("Leave 结果%v", sc.configs)
			reply.WrongLeader = false
			return
		}
	}
	DPrintf("K%d Leave %v 超时", args.UUID)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	defer sc.mu.Unlock()
	sc.mu.Lock()
	_, ok := sc.His[args.UUID]
	if ok {
		reply.WrongLeader = false
		return
	}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "不是leader"
	}
	op := Op{}
	op.T = "Move"
	op.UUID = args.UUID
	op.Gid = args.GID
	op.Shard = args.Shard
	index, term, isLeader := sc.rf.Start(op)
	for cnt := 0; cnt < 100; cnt++ {

		sc.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)

		sc.mu.Lock()
		tterm, isLeader := sc.rf.GetState()
		if debugLog {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Move index: %v len(log): %v args: %+v", sc.me, isLeader, term, tterm, index, len(sc.Log), args)
		} else {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Move index: %v len: %v args: %+v", sc.me, isLeader, term, tterm, index, len(sc.His), args)
		}
		if !isLeader || term != tterm {
			reply.Err = "NoLeader"
			DPrintf("K%d Get 失败2", sc.me)
			return
		}
		if _, ok := sc.His[args.UUID]; ok {
			if debugLog {
				DPrintf("K%d Move Server添加成功index: %d ", sc.me, index)
			} else {
				DPrintf("K%d Move Server添加成功index: %d len: %d ", sc.me, index, len(sc.His))
			}
			reply.WrongLeader = false
			return
		}
	}
	DPrintf("K%d Move 超时 %v", sc.me, args.UUID)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	defer sc.mu.Unlock()
	sc.mu.Lock()
	_, ok := sc.His[args.UUID]
	if ok {
		reply.WrongLeader = false
		return
	}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "不是leader"
	}
	op := Op{}
	op.T = "Query"
	op.UUID = args.UUID
	index, term, isLeader := sc.rf.Start(op)
	for cnt := 0; cnt < 100; cnt++ {

		sc.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)

		sc.mu.Lock()
		tterm, isLeader := sc.rf.GetState()
		if debugLog {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Query index: %v len(log): %v args: %+v", sc.me, isLeader, term, tterm, index, len(sc.Log), args)
		} else {
			DPrintf("K%d isLeader: %v term: %d tterm: %d 等待Query index: %v len: %v args: %+v", sc.me, isLeader, term, tterm, index, len(sc.His), args)
		}
		if !isLeader || term != tterm {
			reply.Err = "NoLeader"
			DPrintf("K%d Get 失败2", sc.me)
			return
		}
		if _, ok := sc.His[args.UUID]; ok {
			if debugLog {
				DPrintf("K%d Query Server添加成功index: %d ", sc.me, index)
			} else {
				DPrintf("K%d Query Server添加成功index: %d len: %d ", sc.me, index, len(sc.His))
			}
			if args.Num == -1 {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			DPrintf("Query config: %v", reply.Config)
			reply.WrongLeader = false
			return
		}
	}
	DPrintf("K%d Query %v 超时", sc.me, args.UUID)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	// debugLog = false
	// debug = false
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func GetGIDWithMinimumShards(s2g map[int]int) int {
	// make iteration deterministic
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with minimum shards
	index, min := 0, NShards+1
	for _, gid := range keys {
		if gid != 0 && s2g[gid] < min {
			index, min = gid, s2g[gid]
		}
	}
	return index
}

func GetGIDWithMaximumShards(s2g map[int]int) int {
	// always choose gid 0 if there is any
	if shards, ok := s2g[0]; ok && shards > 0 {
		DPrintf("GetGIDWithMaxi %+v Index: %d", s2g, 0)
		return 0
	}
	// make iteration deterministic
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with maximum shards
	index, max := 0, -1
	for _, gid := range keys {
		if s2g[gid] > max {
			index, max = gid, s2g[gid]
		}
	}
	DPrintf("GetGIDWithMaxi %+v Index: %d", s2g, index)
	return index
}

func rebalance(newConfig *Config, opIndex int) {
	//rebalance
	// 优先将没有对应gid的shard分配给新加入的
	for i, v := range newConfig.Shards {
		if v == 0 {
			newConfig.Shards[i] = opIndex
		}
	}

	// max := 0
	// min := 256
	var min_gid, max_gid int
	counts := make(map[int]int)
	// 初始化gid的数量
	for gid, _ := range newConfig.Groups {
		// 存储每个gid对应哪些shards 0: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
		counts[gid] = 0
	}
	// 记录每个gid中含有shard的数量
	for _, gid := range newConfig.Shards {
		// 对应的gid++
		if _, ok := counts[gid]; !ok {
			DPrintf("rebalance时 gid: %d 在shards中存在，但是groups中不存在", gid)
		}
		counts[gid]++
	}
	// 初始化min，max
	// for gid, nums := range counts {
	// 	if max == nums && max_gid < gid || max < nums {
	// 		max = nums
	// 		max_gid = gid
	// 	}
	// 	if min == nums && min_gid < gid || min > nums {
	// 		min = nums
	// 		min_gid = gid
	// 	}
	// }
	min_gid = GetGIDWithMinimumShards(counts)
	max_gid = GetGIDWithMaximumShards(counts)

	for counts[max_gid]-counts[min_gid] > 1 {
		// 调整
		for i, v := range newConfig.Shards {
			if v == max_gid {
				newConfig.Shards[i] = min_gid
				counts[min_gid]++
				counts[max_gid]--
				break
			}
		}
		// 再判断
		min_gid = GetGIDWithMinimumShards(counts)
		max_gid = GetGIDWithMaximumShards(counts)
		DPrintf("GetGIDWithMini %+v Index: %d", counts, min_gid)

		DPrintf("rebalancing max_gid: %d min_gid: %d counts: %+v shards: %+v", max_gid, min_gid, counts, newConfig.Shards)
	}
	DPrintf("rebalanced max_gid: %d min_gid: %d opIndex: %d counts: %+v shards: %+v", max_gid, min_gid, opIndex, counts, newConfig.Shards)
}

func (sc *ShardCtrler) ProcessL(op Op) {
	if op.T.IsJoin() {
		servers := make(map[int][]string)
		for k, v := range op.Servers {
			servers[k] = append(servers[k], v...)
		}
		config := sc.configs[len(sc.configs)-1]
		newConfig := Config{}
		newConfig.Shards = config.Shards
		newConfig.Num = config.Num + 1
		newConfig.Groups = make(map[int][]string)
		for k, v := range config.Groups {
			newConfig.Groups[k] = append(newConfig.Groups[k], v...)
		}

		// 拿来将为0的随机分配给join进来的gid
		var opIndex int
		for k, v := range op.Servers {
			newConfig.Groups[k] = append(newConfig.Groups[k], v...)
			opIndex = k
		}
		DPrintf("Join op: %v", op)

		rebalance(&newConfig, opIndex)

		sc.configs = append(sc.configs, newConfig)

	} else if op.T.IsLeave() {
		// 拿到需要删除的gids
		leaveGid := make(map[int]struct{})
		for _, gid := range op.Gids {
			leaveGid[gid] = struct{}{}
		}
		// 复制新的Config
		servers := make(map[int][]string)
		for k, v := range op.Servers {
			servers[k] = append(servers[k], v...)
		}
		config := sc.configs[len(sc.configs)-1]
		newConfig := Config{}
		newConfig.Shards = config.Shards
		newConfig.Num = config.Num + 1
		newConfig.Groups = make(map[int][]string)
		for gid, v := range config.Groups {
			if _, ok := leaveGid[gid]; ok {
				continue
			}
			newConfig.Groups[gid] = append(newConfig.Groups[gid], v...)
		}

		// 清除所有对应的内容
		for i, gid := range newConfig.Shards {
			if _, ok := leaveGid[gid]; ok {
				newConfig.Shards[i] = 0
			}
		}
		DPrintf("leave map: %+v, shard: %+v", newConfig.Groups, newConfig.Shards)
		// 拿到多余的gid
		opIndex := 0
		for _, gid := range newConfig.Shards {
			if gid != 0 {
				opIndex = gid
				break
			}
		}
		rebalance(&newConfig, opIndex)
		sc.configs = append(sc.configs, newConfig)
	} else if op.T.IsMove() {
		// move
		config := sc.configs[len(sc.configs)-1]
		newConfig := Config{}
		newConfig.Shards = config.Shards
		newConfig.Num = config.Num + 1
		newConfig.Groups = make(map[int][]string)
		for gid, v := range config.Groups {
			newConfig.Groups[gid] = append(newConfig.Groups[gid], v...)
		}
		newConfig.Shards[op.Shard] = op.Gid
		sc.configs = append(sc.configs, newConfig)
	}
}

func (sc *ShardCtrler) RefreshData() {
	ms := time.Duration(1)
	for sc.killed() == false {
		select {
		case v := <-sc.applyCh:
			DPrintf("K%d server端接收到日志%+v", sc.me, v)
			p, ok := v.Command.(Op)
			if !ok {
				if v.SnapshotValid {
					DPrintf("RefreshData: 收到snapshot(未实现load Snapshot): %v", v.Snapshot)
					// sc.ReadSnapshot(v.Snapshot)
					continue
				} else {
					panic(fmt.Sprintf("K%d command %v to Op{} 转换错误", sc.me, v))
				}
			}
			sc.mu.Lock()

			// 如果是之前的内容，那么就不采纳
			// if _, ok := sc.DelteHis[p.UUID]; ok {
			// 	delete(sc.DelteHis, p.UUID)
			// 	sc.mu.Unlock()
			// 	continue
			// }
			if _, ok := sc.His[p.UUID]; ok {
				DPrintf("K%d 之前存在%+v", sc.me, p.UUID)
				sc.mu.Unlock()
				continue
			}

			if debugLog {
				sc.Log = append(sc.Log, p)
			}

			sc.ProcessL(p)

			sc.His[p.UUID] = struct{}{}
			sc.HisQue.push_back(p.UUID)

			if debugLog {
				DPrintf("K%d 增加: %+v 增加hist: %+v 长度: %d", sc.me, p, p.UUID, len(sc.Log))
			} else {
				DPrintf("K%d 增加: %+v 增加hist: %+v 长度: %d", sc.me, p, p.UUID, len(sc.His))
			}

			sc.mu.Unlock()
		default:
			// DPrintf("K%d sleep", sc.me)
			time.Sleep(ms * time.Millisecond)
		}
		// sc.Snapshot()
	}
}

// func (sc *ShardCtrler) Snapshot() {
// 	if kv.maxraftstate == -1 {
// 		return
// 	}
// 	kv.mu.Lock()
// 	index, size := kv.rf.RaftStateSize()
// 	if size > kv.maxraftstate {
// 		w := new(bytes.Buffer)
// 		e := labgob.NewEncoder(w)
// 		// index := len(kv.His)
// 		e.Encode(kv.Dataset)
// 		// e.Encode(kv.His)
// 		// e.Encode(kv.DelteHis)
// 		e.Encode(kv.HisQue)
// 		DPrintf("K%d Snapshot: index: %d 打包 %v", kv.me, index-2, w.Bytes())
// 		kv.rf.Snapshot(index-2, w.Bytes())
// 	}
// 	kv.mu.Unlock()
// }

// func (kv *KVServer) ReadSnapshot(data []byte) {
// 	defer kv.mu.Unlock()
// 	kv.mu.Lock()
// 	if kv.maxraftstate == -1 {
// 		DPrintf("ReadSnapshot: 返回1")
// 		return
// 	}
// 	// data := kv.rf.ReadSnapshot()
// 	if len(data) == 0 {
// 		DPrintf("ReadSnapshot: 返回2")
// 		return
// 	}
// 	// var hisQue Queue
// 	// var his map[int64]struct{}
// 	var dataset map[string]string
// 	var hisQue Queue
// 	// var deleteHis map[int64]struct{}
// 	DPrintf("进入snapshot")
// 	r := bytes.NewBuffer(data)
// 	d := labgob.NewDecoder(r)
// 	if d.Decode(&dataset) != nil || d.Decode(&hisQue) != nil {
// 		DPrintf("snapshot %v", data)
// 		panic(errors.New("snapshot解析错误"))
// 	} else {
// 		for _, v := range hisQue.V {
// 			kv.His[v] = struct{}{}
// 		}
// 		DPrintf("K%d snapshot: 读取内容 His: %v,HisQue: %v -> %v, Dataset: %v -> %v", kv.me, kv.His, kv.HisQue, hisQue, kv.Dataset, dataset)
// 		kv.HisQue = hisQue
// 		kv.Dataset = dataset
// 		// kv.His = his
// 		// kv.DelteHis = deleteHis
// 	}
// }

// needed by shardsc.tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	if debugLog {
		sc.Log = make([]Op, 1)
	}

	sc.His = make(map[int64]struct{})
	sc.HisQue.init()
	// sc.ReadSnapshot(sc.rf.ReadSnapshot())
	go sc.RefreshData()

	return sc
}
