package kvraft

import (
	"fmt"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDuplicateRequest = "ErrDuplicateRequest"
	ErrOpNotExecuted = "ErrOpNotExecuted"
)

type Err string

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	RequestID int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	RequestID int32
}

type GetReply struct {
	Err   Err
	Value string
}

const Debug = 0

func KVPrintf(kv *KVServer, format string, a ...interface{}) {
	if Debug > 0 {
		format = "%v: [KVServer %v] " + format + "\n"
		a = append([]interface{}{time.Now().Sub(kv.startTime).Milliseconds(), kv.me}, a...)
		fmt.Printf(format, a...)
	}
	return
}
