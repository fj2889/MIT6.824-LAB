package shardmaster

import (
	"fmt"
	"time"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK                  = "OK"
	ErrWrongNum         = "ErrWrongNum"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrDuplicateRequest = "ErrDuplicateRequest"
	ErrOpNotExecuted    = "ErrOpNotExecuted"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClientID  int64
	RequestID int32
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs      []int
	ClientID  int64
	RequestID int32
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClientID  int64
	RequestID int32
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClientID  int64
	RequestID int32
}

type QueryReply struct {
	Err    Err
	Config Config
}

const (
	JoinOp  = "Join"
	LeaveOp = "Leave"
	MoveOp  = "Move"
	QueryOp = "Query"
)

const Debug = 0

func SMPrintf(sm *ShardMaster, format string, a ...interface{}) {
	if Debug > 0 {
		format = "%v: [ShardMaster %v] " + format + "\n"
		a = append([]interface{}{time.Now().Sub(sm.startTime).Milliseconds(), sm.me}, a...)
		fmt.Printf(format, a...)
	}
	return
}
