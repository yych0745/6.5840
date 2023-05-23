package raft

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
	Debug(dVote, "S%d 要票，args为 %+v 投票者%d条件为 log: %+v, term: %d, votedFor: %d", args.CandidateId, args, rf.me, rf.Log, rf.Term, rf.VotedFor)
	if rf.Term > args.Term {
		reply.Term = rf.Term
		reply.VoteGranted = false
		Debug(dVote, "S%d 拒绝给S%d 原因1,", rf.me, args.CandidateId)
		return
	}
	if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term || args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < len(rf.Log)-1 {
		reply.Term = args.Term
		if rf.Term < args.Term && !args.PreElcection {
			rf.Term = args.Term
			rf.persist()
		}
		Debug(dVote, "S%d 拒绝给S%d 原因2, S%d的lastLogIndex为:%d lastlogTerm为：%d", rf.me, args.CandidateId, rf.me, len(rf.Log)-1, rf.Log[len(rf.Log)-1].Term)
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
	defer Debug(dLeader, "S%d 接收S%d的日志的回复为%+v", args.LeaderId, rf.me, reply)
	reply.Id = rf.me
	reply.ReplyIndex = 0
	rf.rlock.Lock()
	// Debug(dLeader, "S%d 的term为 %d 发送消息给%d term 为%d\n", args.LeaderId, args.Term, rf.me, rf.term)
	if args.Term < rf.Term {
		reply.Success = false
		reply.Term = rf.Term
		rf.rlock.Unlock()
		return
	}
	if args.Term > rf.Term {
		rf.leaderId = args.LeaderId
	}
	rf.startElection = false
	reply.Success = true
	rf.Term = args.Term
	reply.Term = rf.Term
	rf.persist()

	Debug(dLeader, "S%d 目前有%v日志, 长度为%d, 接收S%d 的参数为%+v", rf.me, rf.Log, len(rf.Log), args.LeaderId, args)

	if args.PrevLogIndex == -1 || args.PrevLogIndex < len(rf.Log) && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
		index := min(args.LeaderCommit, len(rf.Log)-1)
		if index > rf.commitIndex {
			rf.commitIndex = index
			rf.wakeCommit()
			Debug(dCommit, "S%d 更改commitIndex为 %d", rf.me, rf.commitIndex)
		}
	}

	if args.PrevLogIndex >= len(rf.Log) {
		reply.ReplyIndex = len(rf.Log)

		if len(args.Entries) == 0 {
			reply.Success = true
			rf.rlock.Unlock()
			return
		}

		reply.Success = false
		rf.rlock.Unlock()
		return
	}

	if args.PrevLogIndex == -1 || rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
		reply.NotHeartbeat = true
		reply.Success = true

		if len(args.Entries) == 0 {
			reply.Success = true
			rf.rlock.Unlock()
			return
		}

		// 如果是加在当前日志的最后一个，那么直接加上
		if len(rf.Log) == args.PrevLogIndex+1 {
			rf.Log = append(rf.Log, args.Entries...)
		} else {
			rf.Log = append(rf.Log[:args.PrevLogIndex+1], args.Entries...)
		}
		reply.ReplyIndex = len(rf.Log)
		rf.persist()
	} else if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		index := args.PrevLogIndex
		term := rf.Log[index].Term
		for i := index - 1; i >= 0; i-- {
			if rf.Log[i].Term != term {
				index = i
				break
			}
		}
		index = min(index+1, args.PrevLogIndex)
		rf.Log = rf.Log[:index+1]
		rf.persist()
		Debug(dLog, "S%d 的log 因为接收到S%d 的内容%v，被切掉了log, 剩下的长度为%d，返回的index为: %d\n", rf.me, args.LeaderId, args, len(rf.Log), index)
		reply.ReplyTerm = rf.Log[index].Term
		reply.ReplyIndex = index

		if len(args.Entries) == 0 {
			reply.Success = true
			rf.rlock.Unlock()
			return
		}

	}
	Debug(dLeader, "S%d 接收到S%d的内容 %+v 当前S%d的日志为 %v commitIndex为 %d 返回值为+%v", rf.me, args.LeaderId, args, rf.me, rf.Log, rf.commitIndex, reply)

	rf.rlock.Unlock()
}
