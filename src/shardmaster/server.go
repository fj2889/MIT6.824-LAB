package shardmaster

import (
	"../raft"
	"sort"
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

func (sm *ShardMaster) getCurrentConfig() Config {
	return sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// check duplicate
	sm.mu.Lock()
	if currentRequestID, ok := sm.clientRequestID[args.ClientID]; ok && currentRequestID >= args.RequestID {
		reply.Err = ErrDuplicateRequest
		SMPrintf(sm, "reply the client-%v request-%v (Join): Servers-%v, duplicate command",
			args.ClientID%10000, args.RequestID, args.Servers)
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	// send the log to raft
	command := Op{
		Servers:   args.Servers,
		Operation: JoinOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	commandIndex, term, isLeader := sm.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	SMPrintf(sm, "get a client-%v request-%v (Join): logIndex-%v, servers-%v",
		args.ClientID%10000, args.RequestID, commandIndex, args.Servers)

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
		reply.Err = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (Join) timeout: logIndex-%v, servers-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.Servers)
		return
	}

	// check applied command
	executedMsg := sm.executedMsg[commandIndex]
	if executedMsg.CommandTerm != term {
		reply.Err = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (Join) ErrOpNotExecuted: logIndex-%v, servers-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.Servers)
		return
	}
	reply.Err = OK
	SMPrintf(sm, "reply the client-%v request-%v (Join) success: logIndex-%v, servers-%v, new_groups-%v",
		args.ClientID%10000, args.RequestID, commandIndex, args.Servers, sm.getCurrentConfig().Groups)
	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// check duplicate
	sm.mu.Lock()
	if currentRequestID, ok := sm.clientRequestID[args.ClientID]; ok && currentRequestID >= args.RequestID {
		reply.Err = ErrDuplicateRequest
		SMPrintf(sm, "reply the client-%v request-%v (Leave): GIDs-%v, duplicate command",
			args.ClientID%10000, args.RequestID, args.GIDs)
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	// send the log to raft
	command := Op{
		GIDs:      args.GIDs,
		Operation: LeaveOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	commandIndex, term, isLeader := sm.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	SMPrintf(sm, "get a client-%v request-%v (Leave): logIndex-%v, GIDs-%v",
		args.ClientID%10000, args.RequestID, commandIndex, args.GIDs)

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
		reply.Err = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (Leave) timeout: logIndex-%v, GIDs-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.GIDs)
		return
	}

	// check applied command
	executedMsg := sm.executedMsg[commandIndex]
	if executedMsg.CommandTerm != term {
		reply.Err = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (Leave) ErrOpNotExecuted: logIndex-%v, GIDs-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.GIDs)
		return
	}
	reply.Err = OK
	SMPrintf(sm, "reply the client-%v request-%v (Leave) success: logIndex-%v, GIDs-%v, new_groups-%v",
		args.ClientID%10000, args.RequestID, commandIndex, args.GIDs, sm.getCurrentConfig().Groups)
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// check duplicate
	sm.mu.Lock()
	if currentRequestID, ok := sm.clientRequestID[args.ClientID]; ok && currentRequestID >= args.RequestID {
		reply.Err = ErrDuplicateRequest
		SMPrintf(sm, "reply the client-%v request-%v (Move): GID-%v, Shard-%v, duplicate command",
			args.ClientID%10000, args.RequestID, args.GID, args.Shard)
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	// send the log to raft
	command := Op{
		GIDs:      []int{args.GID},
		Shard:     args.Shard,
		Operation: MoveOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	commandIndex, term, isLeader := sm.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	SMPrintf(sm, "get a client-%v request-%v (Move): logIndex-%v, GID-%v, Shard-%v",
		args.ClientID%10000, args.RequestID, commandIndex, args.GID, args.Shard)

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
		reply.Err = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (Move) timeout: logIndex-%v, GID-%v, Shard-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.GID, args.Shard)
		return
	}

	// check applied command
	executedMsg := sm.executedMsg[commandIndex]
	if executedMsg.CommandTerm != term {
		reply.Err = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (Move) ErrOpNotExecuted: logIndex-%v, GIDs-%v, Shard-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.GID, args.Shard)
		return
	}
	reply.Err = OK
	SMPrintf(sm, "reply the client-%v request-%v (Move) success: logIndex-%v, GID-%v, Shard-%v, new_groups-%v",
		args.ClientID%10000, args.RequestID, commandIndex, args.GID, args.Shard, sm.getCurrentConfig().Groups)
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// do not need check duplicate
	// send the log to raft
	command := Op{
		Num:       args.Num,
		Operation: QueryOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	commandIndex, term, isLeader := sm.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	SMPrintf(sm, "get a client-%v request-%v (Query): logIndex-%v, Num-%v",
		args.ClientID%10000, args.RequestID, commandIndex, args.Num)

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
		reply.Err = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (Query) timeout: logIndex-%v, Num-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.Num)
		return
	}

	// check applied command
	executedMsg := sm.executedMsg[commandIndex]
	if executedMsg.CommandTerm != term {
		reply.Err = ErrOpNotExecuted
		SMPrintf(sm, "reply the client-%v request-%v (Query) ErrOpNotExecuted: logIndex-%v, Num-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.Num)
		return
	}
	if args.Num < 0 && args.Num != -1 {
		reply.Err = ErrWrongNum
		return
	}
	reply.Err = OK
	if args.Num == -1 || args.Num >= sm.getCurrentConfig().Num {
		reply.Config = sm.getCurrentConfig()
	} else {
		reply.Config = sm.configs[args.Num]
	}
	SMPrintf(sm, "reply the client-%v request-%v (Query) success: logIndex-%v, Num-%v, config-%v",
		args.ClientID%10000, args.RequestID, commandIndex, args.Num, reply.Config)
	return
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
			switch command.Operation {
			case JoinOp:
				sm.ExecuteJoin(command, m.CommandIndex)
			case LeaveOp:
				sm.ExecuteLeave(command, m.CommandIndex)
			case MoveOp:
				sm.ExecuteMove(command, m.CommandIndex)
			case QueryOp:
				SMPrintf(sm, "execute client-%v request-%v [Query], logIndex-%v",
					command.ClientID%10000, command.RequestID, m.CommandIndex)
			default:
				SMPrintf(sm, "unknow command operation type, logIndex-%v", m.CommandIndex)
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

func (sm *ShardMaster) ExecuteJoin(command Op, commandIndex int) {
	SMPrintf(sm, "execute client-%v request-%v [Join], logIndex-%v",
		command.ClientID%10000, command.RequestID, commandIndex)
	newGroups := make(map[int][]string)
	for gid, server := range sm.getCurrentConfig().Groups {
		newGroups[gid] = server
	}
	for gid, servers := range command.Servers {
		if _, ok := sm.getCurrentConfig().Groups[gid]; !ok {
			newGroups[gid] = servers
		}
	}
	newConfig := Config{
		Num:    sm.getCurrentConfig().Num + 1,
		Groups: newGroups,
	}
	sm.configs = append(sm.configs, newConfig)
	sm.divideShard()
}

func (sm *ShardMaster) ExecuteLeave(command Op, commandIndex int) {
	SMPrintf(sm, "execute client-%v request-%v [Leave], logIndex-%v",
		command.ClientID%10000, command.RequestID, commandIndex)
	newGroups := make(map[int][]string)
	for gid, server := range sm.getCurrentConfig().Groups {
		newGroups[gid] = server
	}
	for _, gid := range command.GIDs {
		delete(newGroups, gid)
	}
	newConfig := Config{
		Num:    sm.getCurrentConfig().Num + 1,
		Groups: newGroups,
	}
	sm.configs = append(sm.configs, newConfig)
	sm.divideShard()
}

func (sm *ShardMaster) divideShard() {
	gids := make([]int, 0)
	for gid, _ := range sm.getCurrentConfig().Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	j := 0
	num := sm.getCurrentConfig().Num
	for i := 0; i < len(sm.configs[num].Shards); i++ {
		if len(gids) > 0 {
			sm.configs[num].Shards[i] = gids[j]
			j = (j + 1) % len(gids)
		} else {
			sm.configs[num].Shards[i] = 0
		}
	}
}

func (sm *ShardMaster) ExecuteMove(command Op, commandIndex int) {
	SMPrintf(sm, "execute client-%v request-%v [Move], logIndex-%v",
		command.ClientID%10000, command.RequestID, commandIndex)
	// copy config
	newGroups := make(map[int][]string)
	for gid, server := range sm.getCurrentConfig().Groups {
		newGroups[gid] = server
	}
	newConfig := Config{
		Num:    sm.getCurrentConfig().Num + 1,
		Shards: sm.getCurrentConfig().Shards,
		Groups: newGroups,
	}
	sm.configs = append(sm.configs, newConfig)
	// modify new config
	num := sm.getCurrentConfig().Num
	sm.configs[num].Shards[command.Shard] = command.GIDs[0]
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
