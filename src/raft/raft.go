package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	leaderId      int
	Term          int
	startElection bool
	electionChan  chan RequestVoteReply
	commitChan    chan int
	heartChan     chan AppendEntrieReply
	applyMsgChan  chan ApplyMsg
	// endChan       chan End
	VotedFor int
	rlock    sync.Mutex
	Log      Log
	logIndex int
	logRepu  []bool

	commitIndex int //index of highest log entry known to becommitted (initialized to 0, increasesmonotonically)
	lastApplied int

	nextIndex  []int
	matchIndex []int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) String() string {
	return fmt.Sprintf("S%d 的term: %d votedFor:%d nextIndex: %v matchIndex: %v logIndex: %v log:%v", rf.me, rf.Term, rf.VotedFor, rf.nextIndex, rf.matchIndex, rf.logIndex, rf.Log)
}

type End struct {
	end      bool
	exitLead bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func lastIndex(l []LogEntry) int {
	return len(l) - 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// defer rf.rlock.Unlock()
	rf.rlock.Lock()
	var term int
	var isleader bool
	term = rf.Term
	if rf.leaderId == rf.me {
		isleader = true
		Debug(dInfo, "S%d think itself is leader ,term is %d\n", rf.me, rf.Term)
	}
	rf.rlock.Unlock()
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Term)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log Log
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic(errors.New("错误"))
		//   error...
	} else {
		rf.Term = term
		rf.VotedFor = votedFor
		rf.Log = log
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.rlock.Lock()
	Debug(dInfo, "S%d 更新logIndex %d->%d rf.log %v->%v", rf.me, rf.logIndex, index, rf.Log, rf.Log.cut(index+1, rf.Log.len()))
	rf.logIndex = index
	// rf.Log = rf.Log[index+1:]
	rf.rlock.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	PreElcection bool
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VoteGranted  bool
	PreElcection bool
}

type AppendEntrieArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (r *AppendEntrieArgs) String() string {
	return fmt.Sprintf("Term: %d LeaderId: %d PrevLogIndex: %d PrevLogTerm: %d Entries %+v LeaderCommit: %d",
		r.Term, r.LeaderId, r.PrevLogIndex, r.PrevLogTerm, r.Entries, r.LeaderCommit)
}

type AppendEntrieReply struct {
	Term    int
	Success bool
	Id      int

	NotHeartbeat bool
	ReplyIndex   int
	ReplyTerm    int
}

func (r *AppendEntrieReply) String() string {
	return fmt.Sprintf("Term: %d Success: %v Id: %d NotHeartBeat: %v ReplyIndex: %+v ReplyTerm: %v",
		r.Term, r.Success, r.Id, r.NotHeartbeat, r.ReplyIndex, r.ReplyTerm)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	var term int
	isLeader := false

	// Your code here (2B).
	rf.rlock.Lock()
	term = rf.Term
	if rf.leaderId == rf.me {
		isLeader = true
	}
	if !isLeader {
		rf.rlock.Unlock()
		return index, term, isLeader
	}
	entry := LogEntry{Term: rf.Term, Command: command}
	rf.Log.append(entry)
	rf.persist()
	// Debug(dTrace, "S%d 增加了日志 %v 当前日志为: %v", rf.me, entry, rf.log)
	index = rf.Log.len() - 1
	rf.rlock.Unlock()

	Debug(dLog, "S%d 接收到的命令为%v 返回值 index: %d, term %d, isLeader %v", rf.me, command, index, term, isLeader)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// go rf.commit()
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.rlock.Lock()
		if rf.leaderId != rf.me {
			rf.rlock.Unlock()

			rf.follow()
			// 如果从follow中退出，那就进入election
			if rf.election() {
				rf.rlock.Lock()
				Debug(dVote, "S%d 选举成功 term为: %d\n", rf.me, rf.Term)
				rf.leaderId = rf.me
				rf.rlock.Unlock()
			}
		} else {

			Debug(dLeader, "S%d 成为leader\n", rf.me)
			for i := range rf.peers {
				rf.nextIndex[i] = rf.Log.len()
			}
			rf.matchIndex = make([]int, len(rf.peers))
			rf.leadL()
		}
	}
}

func (rf *Raft) commit1() {
	for rf.killed() == false {
		rf.rlock.Lock()
		Debug(dTrace, "S%d 的lastApplied为 %d commitIndex为 %d log长度为 %d", rf.me, rf.lastApplied, rf.commitIndex, rf.Log.len())
		for rf.lastApplied < rf.commitIndex {
			index := rf.lastApplied + 1
			msg := ApplyMsg{CommandIndex: index, Command: rf.Log.command(index), CommandValid: true}
			Debug(dTrace, "S%d 提交了日志 %+v", rf.me, msg)
			rf.applyMsgChan <- msg
			rf.lastApplied = index
		}
		if rf.me == rf.leaderId {

			rf.matchIndex[rf.me] = rf.Log.len() - 1
			index := rf.commitIndex
			for ; index <= rf.Log.len()-1; index++ {
				sum := 0
				for _, v := range rf.matchIndex {
					if v > index {
						sum++
					}
				}
				if sum*2 < len(rf.peers) {
					break
				}
			}
			Debug(dLeader, "S%d 的matchIndex为: %+v", rf.me, rf.matchIndex)
			if rf.Log.term(max(index, 0)) == rf.Term {
				rf.commitIndex = max(index, 0)
				Debug(dLeader, "S%d 更新commitIndex为：%d", rf.me, rf.commitIndex)
			}
		}
		rf.rlock.Unlock()
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) leadL() {
	// go rf.groupLogReplication()
	for {
		// Debug(dLeader, "S%d :i am leader\n", rf.me)
		if rf.killed() {
			rf.rlock.Unlock()
			return
		}
		if rf.leaderId != rf.me {
			rf.rlock.Unlock()
			return
		}
		args := AppendEntrieArgs{}
		args.Term = rf.Term
		args.LeaderId = rf.me
		rf.matchIndex[rf.me] = rf.Log.len() - 1
		index := rf.commitIndex
		for ; index <= rf.Log.len()-1; index++ {
			sum := 0
			for _, v := range rf.matchIndex {
				if v > index {
					sum++
				}
			}
			if sum*2 < len(rf.peers) {
				break
			}
		}
		Debug(dLeader, "%+v", rf)
		if index < rf.Log.len() && (rf.Log.term(max(index, 0)) == rf.Term || rf.Log.term(rf.commitIndex) == rf.Term) {
			if index > rf.commitIndex {
				rf.commitIndex = max(index, 0)
				rf.wakeCommit()
				Debug(dLeader, "S%d 更新commitIndex为：%d", rf.me, rf.commitIndex)
			}
		}
		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] < rf.Log.len()-1 {
				Debug(dLeader, "S%d 准备开启给S%d发送日志的协程", rf.me, i)
				if !rf.logRepu[i] {
					Debug(dLeader, "S%d 开启给S%d发送日志的协程", rf.me, i)
					go rf.logReplication1(i, peer)
					rf.logRepu[i] = true
				}
			}
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex >= rf.Log.len() {
				Debug(dWarn, "S%d 的日志长度大于leader S%d的本身日志长度 %+v", i, rf.me, rf.nextIndex)
				args.PrevLogIndex = rf.Log.len() - 1
			}
			args.PrevLogTerm = rf.Log.term(args.PrevLogIndex)
			args.LeaderCommit = min(rf.commitIndex, rf.matchIndex[i])
			Debug(dLog2, "S%d 发送给S%d commitIndex为 %d S%d的CommitIndex: %d nextIndex为: %+v",
				rf.me, i, args.LeaderCommit, rf.me, rf.commitIndex, rf.nextIndex)
			go rf.heartbeat(i, peer, args)
		}
		rf.rlock.Unlock()
		ms := 150
	EndLoop:
		for {
			select {
			case reply := <-rf.heartChan:
				if reply.Success {
					if reply.ReplyIndex != 0 {
						rf.rlock.Lock()
						rf.nextIndex[reply.Id] = reply.ReplyIndex
						rf.rlock.Unlock()
					}
					continue
				}
				rf.rlock.Lock()
				if rf.Term < reply.Term {
					Debug(dLeader, "S%d 给S%d发送信息后放弃成为lead\n", rf.me, reply.Id)
					rf.Term = reply.Term
					rf.leaderId = -1
					rf.startElection = false
					rf.rlock.Unlock()
					return
				}
				rf.rlock.Unlock()
			case <-time.After(time.Duration(ms) * time.Millisecond):
				break EndLoop
			}
		}
		rf.rlock.Lock()
	}
}

func (rf *Raft) wakeCommit() {
	Debug(dLeader, "S%d 唤醒commitChan1 开始", rf.me)
	rf.commitChan <- 1
	Debug(dLeader, "S%d 唤醒commitChan1 成功", rf.me)
}

// func (rf *Raft) groupLogReplication() {
// 	Debug(dLeader, "S%d 开始发送群体日志-----\n", rf.me)
// 	for i, peer := range rf.peers {
// 		if i == rf.me {
// 			continue
// 		}
// 		go rf.logReplication(i, peer)
// 	}

// }

func (rf *Raft) logReplication1(id int, peer *labrpc.ClientEnd) {
	afterFirstEqual := false

	for !rf.killed() {
		ms := 50
		time.Sleep(time.Millisecond * time.Duration(ms))
		rf.rlock.Lock()
		if rf.nextIndex[id] < rf.Log.len() {
			if rf.me != rf.leaderId {
				Debug(dLeader, "S%d 关闭给S%d 发送日志的携程 1", rf.me, id)
				rf.logRepu[id] = false
				rf.rlock.Unlock()
				return
			}
			args := AppendEntrieArgs{}
			args.Term = rf.Term
			args.LeaderId = rf.me
			reply := AppendEntrieReply{}
			// reply.FLogcnt = 100
			args.PrevLogIndex = rf.nextIndex[id] - 1
			args.PrevLogTerm = rf.Log.term(args.PrevLogIndex)
			args.LeaderCommit = min(rf.commitIndex, args.PrevLogIndex)
			if afterFirstEqual {
				for i := rf.nextIndex[id]; i < rf.Log.len(); i++ {
					o := LogEntry{Term: rf.Log.term(i), Command: rf.Log.command(i)}
					args.Entries = append(args.Entries, o)
				}
			} else {
				for i := rf.nextIndex[id]; i < rf.nextIndex[id]+1; i++ {
					o := LogEntry{Term: rf.Log.term(i), Command: rf.Log.command(i)}
					args.Entries = append(args.Entries, o)
				}
			}
			Debug(dLeader, "S%d 发送日志%+v 给S%d 此时S%d的nextIndex为：%+v", rf.me, args, id, rf.me, rf.nextIndex)
			if len(args.Entries) == 0 {
				Debug(dLeader, "S%d 关闭给S%d 发送日志的携程 2", rf.me, id)
				rf.logRepu[id] = false
				rf.rlock.Unlock()
				return
			}
			rf.rlock.Unlock()
			ok := peer.Call("Raft.AppendEntrie", args, &reply)

			if !ok {
				continue
			}
			rf.rlock.Lock()
			if reply.Success {
				afterFirstEqual = true
				Debug(dLeader, "S%d LW 发送给S%d的日志提交成功 reply: %+v", rf.me, id, reply)
				tNexIndex := rf.nextIndex[id]
				tMatchIndex := rf.matchIndex[id]
				rf.nextIndex[id] = reply.ReplyIndex
				rf.matchIndex[reply.Id] = rf.nextIndex[reply.Id] - 1
				Debug(dLeader, "S%d LN 发送给S%d的日志提交成功，更新 matchIndex为:%+v -> %+v nextIndex为：%+v -> %+v ", rf.me, id, tMatchIndex, rf.matchIndex, tNexIndex, rf.nextIndex)
			} else {
				if reply.Term > rf.Term {
					Debug(dLeader, "S%d 给S%d发送日志后放弃成为lead\n", rf.me, reply.Id)
					rf.Term = reply.Term
					rf.leaderId = -1
					rf.startElection = false
					rf.persist()
					rf.logRepu[id] = false
					rf.rlock.Unlock()
					Debug(dLeader, "S%d 关闭给S%d 发送日志的携程 3", rf.me, id)
					return
				}
				Debug(dLeader, "S%d 准备更新nextIndex[%d]从%d 到 %d: ", rf.me, id, rf.nextIndex[id], reply.ReplyIndex)
				if reply.ReplyIndex > 0 {
					Debug(dLeader, "S%d 已经更新nextIndex[%d]从%d 到 %d: ", rf.me, id, rf.nextIndex[id], reply.ReplyIndex)
					rf.nextIndex[id] = reply.ReplyIndex
				} else {
					rf.nextIndex[id] = 1
				}
			}
			rf.rlock.Unlock()
		} else {
			rf.logRepu[id] = false
			rf.rlock.Unlock()
			Debug(dLeader, "S%d 关闭给S%d 发送日志的携程 4", rf.me, id)
			return
		}

	}
}

func (rf *Raft) heartbeat(u int, peer *labrpc.ClientEnd, args AppendEntrieArgs) {
	reply := AppendEntrieReply{}
	ok := peer.Call("Raft.AppendEntrie", args, &reply)
	if !ok {
		return
	}
	rf.heartChan <- reply
}

func (rf *Raft) follow() {
	for {
		rf.rlock.Lock()
		if rf.killed() {
			rf.rlock.Unlock()
			return
		}
		rf.startElection = true
		rf.rlock.Unlock()
		ms := (rand.Int63()%300 + 800)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.rlock.Lock()
		if rf.startElection {
			// rf.term++
			rf.rlock.Unlock()
			return
		}
		rf.rlock.Unlock()
	}
}

func (rf *Raft) election() bool {
	// defer rf.rlock.Unlock()
	if !rf.preElection1() {
		// rf.rlock.Lock()
		return false
	}
	Debug(dVote, "S%d 开始正式选举", rf.me)
	for rf.killed() == false {
		conectNum := 0
		rf.rlock.Lock()
		if !rf.startElection {
			Debug(dVote, "S%d 退出选举\n", rf.me)
			rf.rlock.Unlock()
			return false
		}
		rf.Term++
		Debug(dVote, "S%d 开始选举 term为%d \n", rf.me, rf.Term)
		votedNum := 1
		rf.VotedFor = rf.me
		rf.rlock.Unlock()

		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.rlock.Lock()

			args := RequestVoteArgs{}
			args.CandidateId = rf.me

			args.Term = rf.Term

			args.LastLogIndex = rf.Log.len() - 1
			args.LastLogTerm = rf.Log.term(args.LastLogIndex)

			args.Term = rf.Term

			rf.rlock.Unlock()
			go rf.electionRequest(i, peer, args)
		}
		reply := RequestVoteReply{}
		ms := (rand.Int63()%200 + 300)
		rf.rlock.Lock()
	ForEnd:
		for {
			select {
			case reply = <-rf.electionChan:
				conectNum += 1
				if reply.Term == rf.Term && reply.VoteGranted && !reply.PreElcection {
					votedNum++
					if votedNum*2 > len(rf.peers) {
						Debug(dLog, "S%d 选举成功", rf.me)
						// log.Println(rf.me, "选举成功")
						rf.rlock.Unlock()
						return true
					}
				} else if reply.Term > rf.Term {
					rf.Term = reply.Term
					// rf.persist()
					rf.rlock.Unlock()
					return false
				}
			case <-time.After(time.Duration(ms) * time.Millisecond):
				break ForEnd
			}
		}
		if votedNum*2 > len(rf.peers) {
			log.Println(rf.me, "选举成功")
			rf.rlock.Unlock()
			return true
		}
		if conectNum*2 <= len(rf.peers) {
			rf.rlock.Unlock()
			return false
		}
		rf.rlock.Unlock()
		// 如果超过半数，就表示成功了
		Debug(dVote, "S%d选举结束,%d 票数:%d 继续选举\n", rf.me, rf.me, votedNum)
	}
	rf.rlock.Lock()
	rf.rlock.Unlock()
	return false
}

func (rf *Raft) preElection1() bool {
	defer rf.rlock.Unlock()
	rf.rlock.Lock()
	if !rf.startElection {
		Debug(dVote, "S%d 退出预选举\n", rf.me)
		return false
	}
	term := rf.Term + 1
	Debug(dVote, "S%d 开始预选举 term为%d \n", rf.me, rf.Term)
	votedNum := 1
	rf.VotedFor = rf.me
	rf.persist()
	rf.rlock.Unlock()

	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.rlock.Lock()

		args := RequestVoteArgs{}
		args.PreElcection = true
		args.CandidateId = rf.me
		args.LastLogIndex = rf.Log.len() - 1
		args.LastLogTerm = rf.Log.term(args.LastLogIndex)
		args.Term = term

		rf.rlock.Unlock()
		go rf.electionRequest(i, peer, args)
	}
	reply := RequestVoteReply{}
	ms := (rand.Int63()%200 + 300)
	rf.rlock.Lock()
ForEnd:
	for {
		select {
		case reply = <-rf.electionChan:
			if reply.Term == term && reply.VoteGranted {
				votedNum++
				if votedNum*2 > len(rf.peers) {
					Debug(dVote, "S%d 预选举成功", rf.me)
					// log.Println(rf.me, "选举成功")
					return true
				}
			} else if reply.Term > term {
				rf.Term = reply.Term
				rf.VotedFor = -1
				rf.persist()
				return false
			}
		case <-time.After(time.Duration(ms) * time.Millisecond):
			break ForEnd
		}
	}
	if votedNum*2 > len(rf.peers) {
		Debug(dVote, "S%d 预选举成功", rf.me)
		return true
	}
	Debug(dVote, "S%d 预选举失败，获得%d票数", rf.me, votedNum)
	rf.VotedFor = -1
	rf.persist()
	return false
}

func (rf *Raft) electionRequest(u int, peer *labrpc.ClientEnd, args RequestVoteArgs) {
	Debug(dVote, "S%d开始向%d要票\n", rf.me, u)
	reply := RequestVoteReply{}
	ok := peer.Call("Raft.RequestVote", args, &reply)
	if !ok {
		// Debug(dVote, "S%d 选举时连接%d 失败\n", u, rf.me)
		return
	}
	Debug(dVote, "S%d term %d 的情况下 向%d要票结束, 结果为%v\n", rf.me, reply.Term, u, reply.VoteGranted)
	rf.electionChan <- reply
}

func (rf *Raft) sendAppendEntrie(server int, args AppendEntrieArgs, reply *AppendEntrieReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntrie", args, reply)
	return ok
}

func (rf *Raft) commit() {
	for rf.killed() == false {
		select {
		case <-rf.commitChan:
			Debug(dInfo, "S%d commit被唤醒", rf.me)
			rf.rlock.Lock()
			Debug(dTrace, "S%d 的lastApplied为 %d commitIndex为 %d log长度为 %d", rf.me, rf.lastApplied, rf.commitIndex, rf.Log.len())
			for rf.lastApplied < rf.commitIndex && rf.lastApplied+1 < rf.Log.len() {
				index := rf.lastApplied + 1 + rf.logIndex
				// index := rf.lastApplied + 1
				msg := ApplyMsg{CommandIndex: index, Command: rf.Log.command(index), CommandValid: true}
				Debug(dTrace, "S%d 提交了日志 %+v", rf.me, msg)
				rf.applyMsgChan <- msg
				rf.lastApplied = index
			}
			rf.rlock.Unlock()
		default:
			ms := 100
			// Debug(dInfo, "S%d commit睡眠", rf.me)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.VotedFor = -1
	rf.leaderId = -1
	rf.electionChan = make(chan RequestVoteReply, 10)
	rf.heartChan = make(chan AppendEntrieReply, 10)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.logRepu = make([]bool, len(peers))
	rf.Term = 1

	rf.commitChan = make(chan int, 10)

	rf.applyMsgChan = applyCh
	rf.Log.init()

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commit()
	return rf
}
