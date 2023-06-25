package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UUID int64
}

type PutAppendReply struct {
	Err     Err
	Success bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID int64
}

type GetReply struct {
	Err     Err
	Value   string
	Success bool
}

func (e Err) noError() bool {
	return e == ""
}
