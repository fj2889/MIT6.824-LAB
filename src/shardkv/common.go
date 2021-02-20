package shardkv

import (
	"fmt"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrDuplicateRequest = "ErrDuplicateRequest"
	ErrOpNotExecuted    = "ErrOpNotExecuted"
)

type Err string

const (
	GetOp         = "Get"
	PutOp         = "Put"
	AppendOp      = "Append"
	ReconfigureOp = "Reconfigure"
	DeleteShard   = "DeleteShard"
	UpdateShard   = "UpdateShard"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
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

type PullShardArgs struct {
	ConfigNum int
	ShardNum  int
}

type PullShardReply struct {
	Err
	ConfigNum        int
	ShardData        map[string]string
	ClientRequestSeq map[int64]int32
}

type TellReadyArgs struct {
	ConfigNum int
	ShardNum  int
}

type TellReadyReply struct {
	Err
}

const Debug = 1

func KVPrintf(kv *ShardKV, format string, a ...interface{}) {
	if Debug > 0 {
		format = "%v: [Group %v, KVServer %v] " + format + "\n"
		a = append([]interface{}{time.Now().Sub(kv.startTime).Milliseconds(), kv.gid, kv.me}, a...)
		fmt.Printf(format, a...)
	}
	return
}
