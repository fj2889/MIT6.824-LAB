package shardmaster

import (
	"../raft"
	"../util"
	"math"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	configs []Config // indexed by config num

	startTime time.Time
	timeout   time.Duration

	clientRequestID map[int64]int32

	executedMsg       map[int]raft.ApplyMsg
	lastExecutedIndex int
	waitApplyCond     *sync.Cond
}

type Op struct {
	// Your data here.
	Servers map[int][]string // new GID -> servers mappings
	GIDs    []int
	Shard   int
	Num     int // desired config number

	Operation string
	ClientID  int64
	RequestID int32
}

func (sm *ShardMaster) getCurrentConfig() *Config {
	return &sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	command := Op{
		Servers:   args.Servers,
		Operation: JoinOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	paraName := "Servers-%v"
	paraData := []interface{}{args.Servers}
	reply.Err = sm.templateHandler(command, paraName, paraData)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	command := Op{
		GIDs:      args.GIDs,
		Operation: LeaveOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	paraName := "GIDs-%v"
	paraData := []interface{}{command.GIDs}
	reply.Err = sm.templateHandler(command, paraName, paraData)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	command := Op{
		GIDs:      []int{args.GID},
		Shard:     args.Shard,
		Operation: MoveOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	paraName := "ShardNum-%v, GID-%v"
	paraData := []interface{}{command.Shard, command.GIDs[0]}
	reply.Err = sm.templateHandler(command, paraName, paraData)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	if args.Num < 0 && args.Num != -1 {
		reply.Err = ErrWrongNum
		return
	}
	command := Op{
		Num:       args.Num,
		Operation: QueryOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	paraName := "ConfigNum-(%v)"
	paraData := []interface{}{command.Num}
	reply.Err = sm.templateHandler(command, paraName, paraData)
	if reply.Err == OK {
		if args.Num == -1 || args.Num >= sm.getCurrentConfig().Num {
			reply.Config = *sm.getCurrentConfig()
		} else {
			reply.Config = sm.configs[args.Num]
		}
		SMPrintf(sm, "reply the client-%v request-%v (Query) success: Num-%v, config-%v",
			args.ClientID%10000, args.RequestID, args.Num, reply.Config)
	}
}

func (sm *ShardMaster) templateHandler(command Op, paraName string, paraData []interface{}) Err {
	var result Err
	paraData1 := append([]interface{}{command.ClientID % 10000, command.RequestID, command.Operation}, paraData...)
	if command.Operation != QueryOp {
		// check duplicate
		sm.mu.Lock()
		if currentRequestID, ok := sm.clientRequestID[command.ClientID]; ok && currentRequestID >= command.RequestID {
			result = ErrDuplicateRequest
			SMPrintf(sm, "reply the client-%v request-%v (%v) duplicate command: "+paraName, paraData1...)
			sm.mu.Unlock()
			return result
		}
		sm.mu.Unlock()
	}

	// send the log to raft
	commandIndex, term, isLeader := sm.rf.Start(command)
	if !isLeader {
		result = ErrWrongLeader
		return result
	}
	paraData = append([]interface{}{command.ClientID % 10000, command.RequestID, command.Operation, commandIndex}, paraData...)
	SMPrintf(sm, "get a client-%v request-%v (%v): logIndex-%v, "+paraName, paraData...)

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.executedMsg[commandIndex] = raft.ApplyMsg{}
	defer delete(sm.executedMsg, commandIndex)

	// set a timer and wait command to be executed
	getRequestTime := time.Now()
	myTimer(&sm.mu, sm.waitApplyCond, sm.timeout)
	for sm.lastExecutedIndex < commandIndex && time.Now().Sub(getRequestTime) < sm.timeout {
		sm.waitApplyCond.Wait()
	}
	if time.Now().Sub(getRequestTime) >= sm.timeout {
		result = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (%v) timeout: logIndex-%v, "+paraName, paraData...)
		return result
	}

	// check applied command
	executedMsg := sm.executedMsg[commandIndex]
	if executedMsg.CommandTerm != term {
		result = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (%v) ErrOpNotExecuted: logIndex-%v, "+paraName, paraData...)
		return result
	}
	result = OK
	if command.Operation != QueryOp {
		paraData = append(paraData, sm.getCurrentConfig())
		SMPrintf(sm, "reply the client-%v request-%v (%v) success: logIndex-%v, "+paraName+", newConfig-%v",
			paraData...)
	}
	return result
}

func myTimer(mu *sync.Mutex, cond *sync.Cond, duration time.Duration) {
	go func() {
		time.Sleep(duration)
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}()
}

func (sm *ShardMaster) updateStateMachine() {
	for m := range sm.applyCh {
		if !m.CommandValid {
			SMPrintf(sm, "command is invalid, time to decode Snapshot")
			continue
		}
		command := m.Command.(Op)
		sm.mu.Lock()
		// prevent duplicate command here!!!!
		if sm.clientRequestID[command.ClientID] < command.RequestID {
			if command.Operation != QueryOp {
				sm.updateConfig(command, m.CommandIndex)
			} else {
				SMPrintf(sm, "execute client-%v request-%v [Query], logIndex-%v",
					command.ClientID%10000, command.RequestID, m.CommandIndex)
			}
			sm.clientRequestID[command.ClientID] = command.RequestID
		}
		// wake up client-facing RPC handler
		sm.lastExecutedIndex = m.CommandIndex
		sm.waitApplyCond.Broadcast()
		if _, isOk := sm.executedMsg[m.CommandIndex]; isOk {
			sm.executedMsg[m.CommandIndex] = m
		}
		if sm.killed() {
			sm.mu.Unlock()
			break
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) updateConfig(command Op, commandIndex int) {
	// already have lock
	SMPrintf(sm, "execute client-%v request-%v [%v], logIndex-%v",
		command.ClientID%10000, command.RequestID, command.Operation, commandIndex)
	newConfig := sm.createNewConfig()
	switch command.Operation {
	case MoveOp:
		newConfig.Shards[command.Shard] = command.GIDs[0]
	case JoinOp:
		for gid, servers := range command.Servers {
			if _, ok := sm.getCurrentConfig().Groups[gid]; !ok {
				newConfig.Groups[gid] = servers
			}
		}
		sm.rebalance(command)
	case LeaveOp:
		for _, gid := range command.GIDs {
			delete(newConfig.Groups, gid)
		}
		sm.rebalance(command)
	}

}

func (sm *ShardMaster) createNewConfig() *Config {
	newGroups := util.DeepCopyMap(sm.getCurrentConfig().Groups)
	newConfig := Config{
		Num:    sm.getCurrentConfig().Num + 1,
		Shards: sm.getCurrentConfig().Shards,
		Groups: newGroups,
	}
	sm.configs = append(sm.configs, newConfig)
	return sm.getCurrentConfig()
}

func (sm *ShardMaster) rebalance(command Op) {
	cfg := sm.getCurrentConfig()
	oldCfg := &sm.configs[cfg.Num-1]
	groupShardMap := getGroupShardMap(cfg)
	switch command.Operation {
	case JoinOp:
		avg := NShards / len(cfg.Groups)
		remain := NShards % len(cfg.Groups)
		for gid, _ := range command.Servers {
			shardNum := avg
			// there are (remain - len(oldCfg.Groups) groups getting avg+1 shard
			if remain > len(oldCfg.Groups) {
				shardNum++
				remain--
			}
			for i := 0; i < shardNum; i++ {
				maxGid := sm.getMaxShardsGroup(groupShardMap)
				cfg.Shards[groupShardMap[maxGid][0]] = gid
				groupShardMap[maxGid] = groupShardMap[maxGid][1:]
			}
		}
	case LeaveOp:
		if len(cfg.Groups) == 0 {
			cfg.Shards = [NShards]int{}
		} else {
			for _, gid := range command.GIDs {
				shardArray := groupShardMap[gid]
				delete(groupShardMap, gid)
				for _, shardID := range shardArray {
					minGid := sm.getMinShardsGroup(groupShardMap)
					cfg.Shards[shardID] = minGid
					groupShardMap[minGid] = append(groupShardMap[minGid], shardID)
				}
			}
		}
	}
}

func getGroupShardMap(cfg *Config) map[int][]int {
	result := make(map[int][]int)
	for gid, _ := range cfg.Groups {
		result[gid] = []int{}
	}
	for i := 0; i < len(cfg.Shards); i++ {
		if _, ok := result[cfg.Shards[i]]; ok {
			result[cfg.Shards[i]] = append(result[cfg.Shards[i]], i)
		} else {
			result[cfg.Shards[i]] = []int{i}
		}
	}
	return result
}

func (sm *ShardMaster) getMaxShardsGroup(groupShardMap map[int][]int) int {
	max := -1
	result := -1
	for gid, shards := range groupShardMap {
		if len(shards) > max || (len(shards) == max && result < gid) {
			max = len(shards)
			result = gid
		}
	}
	return result
}

func (sm *ShardMaster) getMinShardsGroup(groupShardMap map[int][]int) int {
	min := math.MaxInt32
	result := -1
	for gid, shards := range groupShardMap {
		if len(shards) < min || (len(shards) == min && result < gid) {
			min = len(shards)
			result = gid
		}
	}
	return result
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.startTime = time.Now()
	sm.timeout = 500 * time.Millisecond // 500ms for timeout
	sm.clientRequestID = make(map[int64]int32)
	sm.waitApplyCond = sync.NewCond(&sm.mu)
	sm.executedMsg = make(map[int]raft.ApplyMsg)

	go sm.updateStateMachine()

	return sm
}
