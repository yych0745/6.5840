package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

type Log struct {
	V        []LogEntry
	LogIndex int
}

func (l *Log) String() string {
	s := fmt.Sprintf("len: %d { ", l.realLen())
	for i, v := range l.V {
		s += fmt.Sprintf("%d: [%v, %v] ", i, v.Term, v.Command)
	}
	s += "}"
	return s
}

func (l *Log) init() {
	l.LogIndex = 0
	l.V = make([]LogEntry, 0)
	l.append(LogEntry{0, 0})
}

func (l *Log) realLen() int {
	return len(l.V) + l.LogIndex
}

func (l *Log) len1() int {
	return len(l.V)
}

func (l *Log) command(index int) interface{} {
	if index == 0 {
		return 0
	}
	return l.V[index-l.LogIndex].Command
}

func (l *Log) term(index int) int {
	if index == 0 {
		return 0
	}
	return l.V[index-l.LogIndex].Term
}

func (l *Log) append(u ...LogEntry) {
	l.V = append(l.V, u...)
}

func (l *Log) cut(start int, end int) []LogEntry {
	l.V = l.cut1(start, end)
	return l.V
}

func (l *Log) cut1(start int, end int) []LogEntry {
	return l.V[start:end]
}

func (l *Log) valid(args AppendEntrieArgs) string {
	index := args.PrevLogIndex
	term := args.PrevLogTerm
	if index >= l.realLen() {
		return fmt.Sprintf("%d的长度大于日志%d", index, l.realLen())
	} else if l.term(index) != term {
		return fmt.Sprintf("args的term:%d 和log的term%d不同", index, l.term(index))
	}
	return fmt.Sprintf("args的term%d和log的term%d相同", term, l.term(index))
}

func (l *Log) snapshot(index int) {
	t := make([]LogEntry, 1)
	t[0] = LogEntry{0, 0} 
	t = append(t, l.V[index-l.LogIndex+1:]...)
	l.V = t
	Debug(dWarn, "%d   t: %+v", index-l.LogIndex+1, t)
}
