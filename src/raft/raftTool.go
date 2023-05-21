package raft

func (rf *Raft) AppendEntrie(args AppendEntrieArgs, reply *AppendEntrieReply) {
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
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.Log)-1)
			Debug(dCommit, "S%d 更改commitIndex为 %d", rf.me, rf.commitIndex)
		}
	}

	if args.PrevLogIndex >= len(rf.Log) {
		reply.ReplyIndex = len(rf.Log) - 1

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
		reply.NotHearbeat = true
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
