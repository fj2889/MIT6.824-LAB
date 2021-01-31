package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	uuid          int64
	requestId     int32
	currentLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.uuid = nrand()
	ck.requestId = 0
	ck.currentLeader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	atomic.AddInt32(&ck.requestId, 1)
	args := QueryArgs{
		Num: num,
		ClientID:  ck.uuid,
		RequestID: ck.requestId,
	}
	count := 0
	var result Config
	for {
		count++
		if count%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		var reply QueryReply
		ok := ck.servers[ck.currentLeader].Call("ShardMaster.Query", &args, &reply)
		if !ok {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			result = reply.Config
		case ErrWrongNum, ErrDuplicateRequest:
		case ErrWrongLeader, ErrOpNotExecuted:
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			continue
		}
		return result
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	atomic.AddInt32(&ck.requestId, 1)
	args := JoinArgs{
		Servers: servers,
		ClientID:  ck.uuid,
		RequestID: ck.requestId,
	}
	count := 0
	for {
		count++
		if count%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		var reply QueryReply
		ok := ck.servers[ck.currentLeader].Call("ShardMaster.Join", &args, &reply)
		if !ok {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
		case ErrDuplicateRequest:
		case ErrWrongLeader, ErrOpNotExecuted:
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			continue
		}
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	atomic.AddInt32(&ck.requestId, 1)
	args := LeaveArgs{
		GIDs: gids,
		ClientID:  ck.uuid,
		RequestID: ck.requestId,
	}
	count := 0
	for {
		count++
		if count%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		var reply QueryReply
		ok := ck.servers[ck.currentLeader].Call("ShardMaster.Leave", &args, &reply)
		if !ok {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
		case ErrDuplicateRequest:
		case ErrWrongLeader, ErrOpNotExecuted:
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			continue
		}
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	atomic.AddInt32(&ck.requestId, 1)
	args := MoveArgs{
		Shard: shard,
		GID:   gid,
		ClientID:  ck.uuid,
		RequestID: ck.requestId,
	}
	count := 0
	for {
		count++
		if count%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		var reply QueryReply
		ok := ck.servers[ck.currentLeader].Call("ShardMaster.Move", &args, &reply)
		if !ok {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
		case ErrDuplicateRequest:
		case ErrWrongLeader, ErrOpNotExecuted:
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			continue
		}
		return
	}
}
