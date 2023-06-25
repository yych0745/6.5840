package raft

import (
	"bytes"

	"6.5840/labgob"
)

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果Candidate的term小于自身term，拒绝投票
	defer rf.rlock.Unlock()
	rf.rlock.Lock()
	// 收到请求投票后就重置时间
	rf.startElection = false
	reply.PreElcection = args.PreElcection

	// Debug(dVote, "S%d 要票，args为 %+v 投票者%d条件为 %+v", args.CandidateId, args, rf.me, rf)
	Debug(dVote, "S%d 要票，args为 %+v 投票者%+v", args.CandidateId, args, rf)
	if rf.Term > args.Term {
		reply.Term = rf.Term
		reply.VoteGranted = false
		Debug(dVote, "S%d 拒绝给S%d 原因1,", rf.me, args.CandidateId)
		return
	}
	if (args.LastLogTerm < rf.term(rf.Log.realLen()-1)) || ((args.LastLogTerm == rf.Log.term(rf.Log.realLen()-1) || args.LastLogTerm == rf.lastIncludedTerm) && args.LastLogIndex < rf.Log.realLen()-1) {
		reply.Term = rf.Term
		if rf.Term < args.Term && !args.PreElcection {
			// Debug(dVote, "S%d 的term在S%d的选举中更新了：%d->%d", rf.me, args.CandidateId, rf.Term, args.Term)
			// rf.Term = args.Term
			// rf.persist()
		}
		reply.VoteGranted = false
		Debug(dVote, "S%d 拒绝给S%d 原因2, S%d的lastLogIndex为:%d lastlogTerm为：%d 判断条件: %v %v", rf.me, args.CandidateId, rf.me, rf.Log.realLen()-1, rf.Log.term(rf.Log.realLen()-1), (args.LastLogTerm < rf.Log.term(rf.Log.realLen()-1) || args.LastLogTerm < rf.lastIncludedTerm), ((args.LastLogTerm == rf.Log.term(rf.Log.realLen()-1) || args.LastLogTerm == rf.lastIncludedTerm) && args.LastLogIndex < rf.Log.realLen()-1))
		return
	}

	if rf.Term == args.Term {
		reply.Term = args.Term
		if args.PreElcection {
			reply.VoteGranted = true
			return
		}
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			rf.VotedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
			Debug(dVote, "S%d 拒绝给S%d 原因3 投票者term为: %d,", rf.me, args.CandidateId, rf.Term)
		}
		return
	}
	// 如果Candidate的term更大，那么不管什么，就投它
	if !args.PreElcection {
		Debug(dVote, "S%d 的term在S%d的选举中更新了：%d->%d", rf.me, args.CandidateId, rf.Term, args.Term)
		rf.Term = args.Term
		rf.VotedFor = args.CandidateId
		rf.leaderId = -1
	}
	reply.Term = args.Term
	rf.persist()
	if rf.leaderId == rf.me {
		Debug(dLeader, "S%d 在收到S%d的竞选消息后放弃leader", rf.me, args.CandidateId)
	}
	reply.VoteGranted = true
	return
}
func (rf *Raft) AppendEntrie(args AppendEntrieArgs, reply *AppendEntrieReply) {
	pos := 0
	reply.Id = rf.me
	reply.ReplyIndex = 0
	rf.rlock.Lock()
	// reply.LogPos = args.LogPos
	// Debug(dLeader, "S%d 的term为 %d 发送消息给%d term 为%d\n", args.LeaderId, args.Term, rf.me, rf.term)
	if args.Term < rf.Term {
		reply.Success = false
		reply.Term = rf.Term
		rf.rlock.Unlock()
		pos = 1
		return
	}
	if args.Term > rf.Term {
		rf.leaderId = args.LeaderId
	}
	if args.Term == rf.Term && rf.leaderId == -1 {
		rf.leaderId = args.LeaderId
	}
	rf.startElection = false
	reply.Success = true
	rf.Term = args.Term
	reply.Term = rf.Term
	rf.persist()

	if len(args.Entries) > 0 {
		Debug(dLeader, "S%d %+v 接收S%d的日志args: %v ", rf.me, rf, args.LeaderId)
		Debug(dLeader, "S%d %+v 接收S%d的日志args: %v term结果为:%v 目前日志长度为%d %+v", rf.me, rf, args.LeaderId, args, rf.Log.valid(args), rf.Log.realLen(), rf)
	} else {
		Debug(dLeader, "S%d %+v 接收S%d的心跳args: %v ", rf.me, rf, args.LeaderId, args)
		Debug(dLeader, "S%d %+v 接收S%d的心跳args: %v term结果为:%v", rf.me, rf, args.LeaderId, args, rf.Log.valid(args))
	}

	if args.PrevLogIndex == -1 || args.PrevLogIndex < rf.Log.realLen() && rf.Log.term(args.PrevLogIndex) == args.PrevLogTerm {
		index := min(args.LeaderCommit, rf.Log.realLen()-1)
		if index > rf.commitIndex {
			rf.commitIndex = index
			rf.wakeCommit()
			Debug(dCommit, "S%d 更改commitIndex为 %d", rf.me, rf.commitIndex)
		}
	}

	if args.PrevLogIndex >= rf.Log.realLen() {
		reply.ReplyIndex = rf.Log.realLen()

		if len(args.Entries) == 0 {
			reply.Success = true
			rf.rlock.Unlock()
			pos = 2
			return
		}

		reply.Success = false
		rf.rlock.Unlock()
		pos = 3
		return
	}

	if args.PrevLogIndex == -1 || rf.term(args.PrevLogIndex) == args.PrevLogTerm {
		reply.NotHeartbeat = true
		reply.Success = true

		if len(args.Entries) == 0 {
			reply.Success = true
			rf.rlock.Unlock()
			pos = 4
			return
		}

		// 如果是加在当前日志的最后一个，那么直接加上
		if rf.Log.realLen() == args.PrevLogIndex+1 {
			rf.Log.append(args.Entries...)
		} else {
			rf.Log.cut(0, args.PrevLogIndex+1-rf.Log.Base)
			rf.Log.append(args.Entries...)
		}
		reply.ReplyIndex = rf.Log.realLen()
		rf.persist()
	} else if rf.term(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		index := args.PrevLogIndex
		term := rf.Log.term(index)
		for i := index - 1; i >= rf.Log.Base; i-- {
			if rf.Log.term(i) != term {
				index = i
				break
			}
		}
		Debug(dInfo, "S%d 切log的参数min(index: %d, args.PrevLogIndex: %d) log的长度: %d", rf.me, index, args.PrevLogIndex, rf.Log.len1())
		index = min(index, args.PrevLogIndex)
		// index = max(index, rf.commitIndex)
		rf.Log.cut(0, index+1-rf.Log.Base)
		rf.persist()
		Debug(dLog, "S%d 的log 因为接收到S%d 的内容%+v，被切掉了log, 剩下的长度为%d，返回的index为: %d\n", rf.me, args.LeaderId, args, rf.Log.realLen(), index+1)
		// reply.ReplyTerm = rf.Log.term(index)
		reply.ReplyIndex = index + 1

		if len(args.Entries) == 0 {
			reply.Success = true
			rf.rlock.Unlock()
			pos = 5
			return
		}

	}
	Debug(dLeader, "S%d 接收到S%d的内容 %+v %+v 返回值为+%v", rf.me, args.LeaderId, args, rf, reply)

	rf.rlock.Unlock()
	defer Debug(dLeader, "S%d 接收完S%d的日志的回复为%+v 返回点: %d", args.LeaderId, rf.me, reply, pos)

}

type InstallSnapshotArgs struct {
	Term, LeaderId, LastIncludedIndex, LastIncludedTerm, Offset int
	Data                                                        []byte
	Done                                                        bool
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	defer rf.rlock.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.rlock.Lock()
	rf.senSnap[server] = false
	if !ok {
		return ok
	}
	if reply.Term > rf.Term {
		rf.leaderId = -1
		rf.startElection = false
		rf.Term = reply.Term
		Debug(dLeader, "S%d 给S%d发送snapshot后放弃成为lead\n", rf.me, server)
	} else {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	return ok
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	defer rf.rlock.Unlock()
	rf.rlock.Lock()
	reply.Term = rf.Term
	rf.startElection = false
	Debug(dInfo, "S%d 收到S%d的snapshot", rf.me, args.LeaderId)
	// 1: 如果自己term更大，就不管
	if rf.Term > args.Term {
		return
	}

	// 2: 如果自己的更新，那么也不管
	if rf.Log.Base >= args.LastIncludedIndex {
		return
	}

	// 3: 如果自己的没有那么新
	// 3.1 如果有部分相同的日志和snapshot相同，那么就丢弃这部分日志
	if rf.Log.realLen() > args.LastIncludedIndex && rf.Log.term(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.Log.snapshot(args.LastIncludedIndex)
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.Log.Base = args.LastIncludedIndex
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	} else {
		// 如果比这个日志小，或者包含的index部分不同，那么就全部丢弃，全拿新的
		rf.Log.clean()
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.Log.Base = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	}
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
		Debug(dTrace, "S%d 的lastApplied更新2： %v -> %v", rf.me, rf.lastApplied, args.LastIncludedIndex)
		rf.lastApplied = args.LastIncludedIndex
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Term)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)
	rf.clock.Lock()
	rf.rlock.Unlock()
	msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
	rf.applyMsgChan <- msg
	rf.clock.Unlock()
	rf.rlock.Lock()
	Debug(dCommit, "S%d 上传snapshot 此时状态: %+v snapshot:%+v", rf.me, rf, msg)
}
