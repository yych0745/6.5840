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

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	term          int
	startElection bool
	electionChan  chan RequestVoteReply
	heartChan     chan AppendEntrieReply
	appendChan    chan AppendEntrieReply
	applyMsgChan  chan ApplyMsg
	endChan       chan End
	votedFor      int
	rlock         sync.Mutex
	log           []LogEntry

	commitIndex int //index of highest log entry known to becommitted (initialized to 0, increasesmonotonically)
	lastApplied int

	nextIndex  []int
	matchIndex []int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}
type End struct {
	end      bool
	exitLead bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// defer rf.rlock.Unlock()
	rf.rlock.Lock()
	var term int
	var isleader bool
	term = rf.term
	if rf.leaderId == rf.me {
		isleader = true
		Debug(dInfo, "S%d think itself is leader ,term is %d\n", rf.me, rf.term)
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
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

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntrieArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntrieReply struct {
	Term    int
	Success bool
	Id      int
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
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.rlock.Lock()
	term = rf.term
	if rf.leaderId == rf.me {
		isLeader = true
	}
	index = len(rf.log)
	rf.rlock.Unlock()

	if !isLeader {
		return index, term, isLeader
	}
	Debug(dCommit, "S%d 接收到的命令为%v", rf.me, command)
	go rf.groupCommand(command)
	return index, term, isLeader
}

func (rf *Raft) groupCommand(command interface{}) {
	set := map[int]struct{}{}
	rf.rlock.Lock()
	entry := LogEntry{Term: rf.term, Command: command}
	rf.log = append(rf.log, entry)
	set[rf.me] = struct{}{}
	args := AppendEntrieArgs{}
	args.Entries = append(args.Entries, entry)
	args.LeaderId = rf.leaderId
	args.Term = rf.term
	rf.rlock.Unlock()
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendCommand(i, peer, args)
	}
	for {
		select {
		case end := <-rf.endChan:
			if end.end || end.exitLead {
				return
			}
		case reply := <-rf.appendChan:
			if reply.Success {
				// set[reply.Id] = struct{}{}
				// if (len(set)) == len(rf.peers) {
				// 	fmt.Println("send chan", l)
				// 	rf.applyMsgChan <- ApplyMsg{Command: command, CommandIndex: l, CommandValid: true}
				// 	rf.rlock.Lock()
				// 	rf.commitIndex = l
				// 	rf.rlock.Unlock()
				// 	return
				// }
				rf.nextIndex[reply.Id]++
				rf.matchIndex[reply.Id] = rf.nextIndex[reply.Id] - 1
				continue
			}

			if reply.Term > rf.term {
				rf.leaderId = -1
				rf.term = reply.Term
				return
			}

			go rf.sendCommand(reply.Id, rf.peers[reply.Id], args)

		}
	}
}

func (rf *Raft) sendCommand(u int, peer *labrpc.ClientEnd, args AppendEntrieArgs) {
	reply := AppendEntrieReply{}
	ok := peer.Call("Raft.AppendEntrie", args, &reply)
	if !ok {
		reply.Id = u
		reply.Success = false
		reply.Term = -1
	}
	rf.appendChan <- reply
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
				Debug(dVote, "S%d 选举成功 term为: %d\n", rf.me, rf.term)
				rf.leaderId = rf.me
				rf.rlock.Unlock()
			}
		} else {

			Debug(dLeader, "S%d 成为leader\n", rf.me)
			for i, _ := range rf.peers {
				rf.nextIndex[i] = len(rf.log)
			}
			rf.matchIndex = make([]int, len(rf.peers))
			rf.rlock.Unlock()
			rf.lead()
		}

	}
	rf.rlock.Lock()
	rf.endChan <- End{end: true}
	rf.rlock.Unlock()
}

func (rf *Raft) lead() {
	// 发送hearbeat
	for {
		Debug(dLeader, "S%d :i am leader\n", rf.me)
		rf.rlock.Lock()
		if rf.killed() {
			rf.rlock.Unlock()
			return
		}
		if rf.leaderId != rf.me {
			rf.endChan <- End{exitLead: true}
			rf.rlock.Unlock()
			return
		}
		args := AppendEntrieArgs{}
		args.Term = rf.term
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		rf.rlock.Unlock()
		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.rlock.Lock()
			// 每次心跳都会放入最后一个log看是否相等
			o := LogEntry{Term: rf.term, Command: rf.log[len(rf.log) - 1].Command}
			args.Entries = []LogEntry{o}

			if rf.nextIndex[i] < len(rf.log) {
				o := LogEntry{Term: rf.term, Command: rf.log[rf.nextIndex[i]].Command}
				args.Entries = []LogEntry{o}
			}

			rf.rlock.Unlock()
			go rf.heartbeat(i, peer, args)
		}
		ms := 200
	EndLoop:
		for {
			select {
			case reply := <-rf.heartChan:
				if reply.Success {
					rf.rlock.Lock()
					rf.nextIndex[reply.Id]++
					rf.matchIndex[reply.Id] = rf.nextIndex[reply.Id] - 1
					rf.rlock.Unlock()
					continue
				}
				rf.rlock.Lock()
				if rf.term < reply.Term {
					Debug(dLeader, "S%d 给S%d发送信息后放弃成为lead\n", rf.me, reply.Id)
					rf.term = reply.Term
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
	defer rf.rlock.Unlock()
	for {
		rf.rlock.Lock()
		if rf.killed() {
			return false
		}
		if !rf.startElection {
			Debug(dVote, "S%d 退出选举\n", rf.me)
			return false
		}
		rf.term++
		Debug(dVote, "S%d 开始选举 term为%d \n", rf.me, rf.term)
		votedNum := 1
		rf.votedFor = rf.me
		rf.rlock.Unlock()

		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.rlock.Lock()

			args := RequestVoteArgs{}
			args.CandidateId = rf.me

			args.Term = rf.term

			rf.rlock.Unlock()
			go rf.electionRequest(i, peer, args)
		}
		reply := RequestVoteReply{}
		ms := (rand.Int63()%300 + 400)
		rf.rlock.Lock()
	ForEnd:
		for {
			select {
			case reply = <-rf.electionChan:
				if reply.Term == rf.term && reply.VoteGranted {
					votedNum++
					if votedNum*2 > len(rf.peers) {
						Debug(dLog, "S%d 选举成功", rf.me)
						// log.Println(rf.me, "选举成功")
						return true
					}
				} else if reply.Term > rf.term {
					rf.term = reply.Term
					return false
				}
			case <-time.After(time.Duration(ms) * time.Millisecond):
				break ForEnd
			}
		}
		if votedNum*2 > len(rf.peers) {
			log.Println(rf.me, "选举成功")
			return true
		}
		rf.rlock.Unlock()
		// 如果超过半数，就表示成功了
		Debug(dVote, "S%d选举结束,%d 票数:%d 继续选举\n", rf.me, rf.me, votedNum)
	}
}

func (rf *Raft) electionRequest(u int, peer *labrpc.ClientEnd, args RequestVoteArgs) {
	// rf.rlock.Lock()
	Debug(dVote, "S%d开始向%d要票\n", rf.me, u)
	reply := RequestVoteReply{}
	ok := peer.Call("Raft.RequestVote", args, &reply)
	if !ok {
		Debug(dVote, "S%d 选举时连接%d 失败\n", u, rf.me)
		// rf.rlock.Unlock()
		return
	}
	Debug(dVote, "S%d 向%d要票结束, 结果为%v\n", rf.me, u, reply.VoteGranted)
	rf.electionChan <- reply
	// rf.rlock.Unlock()
}

func (rf *Raft) sendAppendEntrie(server int, args AppendEntrieArgs, reply *AppendEntrieReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntrie", args, reply)
	return ok
}

func (rf *Raft) AppendEntrie(args AppendEntrieArgs, reply *AppendEntrieReply) {
	reply.Id = rf.me
	rf.rlock.Lock()
	Debug(dLeader, "S%d 的term为 %d 发送消息给%d term 为%d\n", args.LeaderId, args.Term, rf.me, rf.term)
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		rf.rlock.Unlock()
		return
	}
	if args.Term > rf.term {
		rf.leaderId = args.LeaderId
	}
	rf.startElection = false
	reply.Success = true
	rf.term = args.Term
	reply.Term = rf.term
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
		Debug(dTrace, "S%d 获取到了S%d 增加给的%v日志，目前S%d 有%v日志", rf.me, args.LeaderId, args.Entries, rf.me, rf.log)
	}
	if args.LeaderCommit > rf.commitIndex {
		for i := max(rf.commitIndex, 1); i <= args.LeaderCommit; i++ {
			rf.applyMsgChan <- ApplyMsg{Command: rf.log[i].Command, CommandIndex: i, CommandValid: true}
			rf.commitIndex = i
		}
	}
	rf.rlock.Unlock()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果Candidate的term小于自身term，拒绝投票
	defer rf.rlock.Unlock()
	rf.rlock.Lock()
	if rf.term > args.Term {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}
	// 两边term相等，返回false
	if rf.term == args.Term {
		reply.Term = args.Term
		if rf.votedFor != -1 || rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
		} else {
			reply.VoteGranted = true
		}
		return
	}
	// 如果Candidate的term更大，那么不管什么，就投它
	rf.term = args.Term
	rf.leaderId = -1
	rf.votedFor = args.CandidateId
	reply.Term = rf.term
	reply.VoteGranted = true
	// 收到请求投票后就重置时间
	rf.startElection = false
	return
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
	rf.votedFor = -1
	rf.leaderId = -1
	rf.electionChan = make(chan RequestVoteReply, 10)

	rf.heartChan = make(chan AppendEntrieReply, 10)
	rf.appendChan = make(chan AppendEntrieReply, 10)
	rf.endChan = make(chan End, 10)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyMsgChan = applyCh
	rf.log = append(rf.log, LogEntry{Term: -1})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
