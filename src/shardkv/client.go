package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"../labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	uuid          int64
	requestId     int32
	currentLeader map[int]int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.uuid = nrand()
	ck.requestId = 0
	ck.currentLeader = make(map[int]int)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	result := ""
	atomic.AddInt32(&ck.requestId, 1)
	args := GetArgs{
		Key:       key,
		ClientID:  ck.uuid,
		RequestID: ck.requestId,
	}
	reply := GetReply{}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok := ck.currentLeader[gid]; !ok {
				ck.currentLeader[gid] = 0
			}
		Loop:
			for count := 0; count < len(servers); count++ {
				srv := ck.make_end(servers[ck.currentLeader[gid]])
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if !ok {
					ck.currentLeader[gid] = (ck.currentLeader[gid] + 1) % len(servers)
					continue
				}
				switch reply.Err {
				case OK:
					result = reply.Value
				case ErrNoKey, ErrDuplicateRequest:
				case ErrWrongLeader, ErrOpNotExecuted:
					ck.currentLeader[gid] = (ck.currentLeader[gid] + 1) % len(servers)
					continue
				case ErrWrongGroup:
					break Loop
				}
				return result
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	atomic.AddInt32(&ck.requestId, 1)
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.uuid,
		RequestID: ck.requestId,
	}
	reply := PutAppendReply{}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok := ck.currentLeader[gid]; !ok {
				ck.currentLeader[gid] = 0
			}
		Loop:
			for count := 0; count < len(servers); count++ {
				srv := ck.make_end(servers[ck.currentLeader[gid]])
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if !ok {
					ck.currentLeader[gid] = (ck.currentLeader[gid] + 1) % len(servers)
					continue
				}
				switch reply.Err {
				case OK:
				case ErrDuplicateRequest:
				case ErrWrongLeader, ErrOpNotExecuted:
					ck.currentLeader[gid] = (ck.currentLeader[gid] + 1) % len(servers)
					continue
				case ErrWrongGroup:
					break Loop
				}
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
