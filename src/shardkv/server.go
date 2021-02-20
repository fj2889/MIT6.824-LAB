package shardkv

import (
	"../labrpc"
	"../shardmaster"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "../raft"
import "sync"
import "../labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Config    shardmaster.Config
	Shards    map[int]Shard
	ShardNum  int
	Operation string
	ClientID  int64
	RequestID int32
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	dead     int32 // set by Kill()
	make_end func(string) *labrpc.ClientEnd
	gid      int
	masters  []*labrpc.ClientEnd
	mck      *shardmaster.Clerk

	maxraftstate int             // snapshot if log grows this big
	persister    *raft.Persister // used to read snapshot and raft state size, do not be used to persist

	// Your definitions here.
	startTime   time.Time
	timeout     time.Duration
	groupLeader map[int]int

	// state machine
	stateMachine    map[int]Shard
	config          shardmaster.Config
	shardWaitToPull map[int]targetGroupInfo // key -> shardNum, value -> targetGroup
	shardWaitToOut  map[int]int             // key->shardNum, value -> oldConfigNum

	executedMsg       map[int]raft.ApplyMsg
	lastExecutedIndex int
	waitApplyCond     *sync.Cond
}

type targetGroupInfo struct {
	TargetGroupID int
	Servers       []string
	OldConfigNum  int
}

type Shard struct {
	ShardID          int
	ConfigNum        int
	Data             map[string]string
	ClientRequestSeq map[int64]int32
}

func (kv *ShardKV) getShardByKey(key string) Shard {
	s, ok := kv.stateMachine[key2shard(key)]
	if !ok {
		KVPrintf(kv, "wrong group when get shard by key")
		panic("wrong group when get shard by key")
	}
	return s
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	command := Op{
		Key:       args.Key,
		Operation: GetOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	reply.Err = kv.templateHandler(command)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if reply.Err == OK {
		value, isOk := kv.getShardByKey(args.Key).Data[args.Key]
		if isOk {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		KVPrintf(kv, "reply the client-%v request-%v (GET) %v: key-%v, value-%v",
			args.ClientID%10000, args.RequestID, reply.Err, args.Key, reply.Value)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op{
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	reply.Err = kv.templateHandler(command)
}

func (kv *ShardKV) templateHandler(command Op) Err {
	var result Err
	paraData1 := []interface{}{command.ClientID % 10000, command.RequestID, command.Operation, command.Key, command.Value}
	kv.mu.Lock()
	// check group
	shardNum := key2shard(command.Key)
	result = kv.checkGroup(shardNum)
	if result == ErrWrongGroup || result == ErrOpNotExecuted {
		paraData1 = append(paraData1, kv.config.Shards[shardNum], kv.config)
		KVPrintf(kv, "reply the client-%v request-%v (%v) wrong group or shard not ready: key-%v, value-%v, target_gid-%v, config-%v", paraData1...)
		kv.mu.Unlock()
		return result
	}
	if command.Operation != GetOp {
		// check duplicate
		if currentRequestID, ok := kv.getShardByKey(command.Key).ClientRequestSeq[command.ClientID]; ok &&
			currentRequestID >= command.RequestID {
			result = ErrDuplicateRequest
			KVPrintf(kv, "reply the client-%v request-%v (%v) duplicate command: key-%v, value-%v", paraData1...)
			kv.mu.Unlock()
			return result
		}
	}
	kv.mu.Unlock()

	// send the log to raft
	commandIndex, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		result = ErrWrongLeader
		return result
	}
	paraData := []interface{}{command.ClientID % 10000, command.RequestID, command.Operation, commandIndex, command.Key, command.Value}
	KVPrintf(kv, "get a client-%v request-%v (%v), logIndex-%v, key-%v, value-%v", paraData...)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.executedMsg[commandIndex] = raft.ApplyMsg{}
	defer delete(kv.executedMsg, commandIndex)

	// set a timer and wait command to be executed
	getRequestTime := time.Now()
	myTimer(&kv.mu, kv.waitApplyCond, kv.timeout)
	for kv.lastExecutedIndex < commandIndex && time.Now().Sub(getRequestTime) < kv.timeout {
		kv.waitApplyCond.Wait()
	}
	if time.Now().Sub(getRequestTime) >= kv.timeout {
		result = ErrOpNotExecuted
		KVPrintf(kv, "reply the client-%v request-%v (%v) timeout: logIndex-%v, key-%v, value-%v", paraData...)
		return result
	}

	// check applied command
	executedMsg := kv.executedMsg[commandIndex]
	if executedMsg.CommandTerm != term {
		result = ErrOpNotExecuted
		KVPrintf(kv, "reply the client-%v request-%v (%v) ErrOpNotExecuted: logIndex-%v, key-%v, value-%v", paraData...)
		return result
	}
	// check group again
	result = kv.checkGroup(shardNum)
	if result == ErrWrongGroup || result == ErrOpNotExecuted {
		paraData1 = append(paraData1, kv.config.Shards[shardNum], kv.config)
		KVPrintf(kv, "reply the client-%v request-%v (%v) wrong group or shard not ready: key-%v, value-%v, target_gid-%v, config-%v", paraData1...)
		return result
	}
	result = OK
	if command.Operation != GetOp {
		KVPrintf(kv, "reply the client-%v request-%v (%v) success: logIndex-%v, key-%v, value-%v", paraData...)
	}
	return result
}

func (kv *ShardKV) checkGroup(shardNum int) Err {
	// already have lock
	if kv.config.Shards[shardNum] != kv.gid {
		return ErrWrongGroup
	}
	if _, exist := kv.shardWaitToPull[shardNum]; exist {
		return ErrOpNotExecuted
	}
	return OK
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.ConfigNum = kv.stateMachine[args.ShardNum].ConfigNum
	if args.ConfigNum >= kv.stateMachine[args.ShardNum].ConfigNum {
		return
	}
	reply.ShardData = deepCopyShardData(kv.stateMachine[args.ShardNum].Data)
	reply.ClientRequestSeq = deepCopyClientRequestSeq(kv.stateMachine[args.ShardNum].ClientRequestSeq)
}

func (kv *ShardKV) TellReady(args *TellReadyArgs, reply *TellReadyReply) {
	command := Op{
		ShardNum: args.ShardNum,
		Config: shardmaster.Config{
			Num: args.ConfigNum,
		},
		Operation: DeleteShard,
	}
	commandIndex, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
	KVPrintf(kv, "put delete shard operation into raft log: logIndex-%v, shardNum-%v",
		commandIndex, args.ShardNum)
}

func myTimer(mu *sync.Mutex, cond *sync.Cond, duration time.Duration) {
	go func() {
		time.Sleep(duration)
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) updateStateMachine() {
	for m := range kv.applyCh {
		if !m.CommandValid {
			KVPrintf(kv, "command is invalid, time to decode Snapshot")
			rawSnapshot := m.Command.([]byte)
			kv.decodeKVState(rawSnapshot)
			continue
		}
		command := m.Command.(Op)
		kv.mu.Lock()
		if command.Operation == UpdateShard {
			for shardNum, shardValue := range command.Shards {
				if shardValue.ConfigNum > kv.stateMachine[shardNum].ConfigNum {
					KVPrintf(kv, "execute [UpdateShard], logIndex-%v, shardNum-%v, shard-%v",
						m.CommandIndex, shardNum, command.Shards[shardNum])
					go kv.sendReadySignal(shardNum, kv.shardWaitToPull[shardNum])
					kv.stateMachine[shardNum] = deepCopyShard(shardValue)
					delete(kv.shardWaitToPull, shardNum)
				}
			}
		} else if command.Operation == DeleteShard {
			// check not need this shard!!!
			if kv.config.Shards[command.ShardNum] != kv.gid && command.Config.Num >= kv.shardWaitToOut[command.ShardNum] {
				KVPrintf(kv, "execute [DeleteShard], logIndex-%v, shardNum-%v",
					m.CommandIndex, command.ShardNum)
				delete(kv.stateMachine, command.ShardNum)
			}
		} else if command.Operation == ReconfigureOp {
			// reconfigure
			if command.Config.Num > kv.config.Num {
				kv.updateWaitShards(kv.config, command.Config)
				for shardNum, _ := range kv.stateMachine {
					if _, ok := kv.shardWaitToPull[shardNum]; !ok {
						newShard := kv.stateMachine[shardNum]
						newShard.ConfigNum = command.Config.Num
						kv.stateMachine[shardNum] = newShard
					}
				}
				kv.config = deepCopyConfig(command.Config)
				KVPrintf(kv, "execute [reconfigure], logIndex-%v, new_config-%v",
					m.CommandIndex, kv.config)
			}
		} else {
			// prevent duplicate command here!!!! prevent concurrent reconfig and request
			shardNum := key2shard(command.Key)
			e := kv.checkGroup(shardNum)
			if e == OK && kv.getShardByKey(command.Key).ClientRequestSeq[command.ClientID] < command.RequestID {
				switch command.Operation {
				case GetOp:
				case PutOp:
					kv.stateMachine[shardNum].Data[command.Key] = command.Value
				case AppendOp:
					kv.stateMachine[shardNum].Data[command.Key] += command.Value
				default:
					KVPrintf(kv, "unknow command operation type, logIndex-%v", m.CommandIndex)
					panic("unknow command operation type")
				}
				KVPrintf(kv, "execute client-%v request-%v [%v], logIndex-%v, key-%v, value-%v, new_value-%v",
					command.ClientID%10000, command.RequestID, command.Operation, m.CommandIndex, command.Key, command.Value, kv.stateMachine[shardNum].Data[command.Key])
				kv.stateMachine[shardNum].ClientRequestSeq[command.ClientID] = command.RequestID
			}
		}
		// wake up client-facing RPC handler
		kv.lastExecutedIndex = m.CommandIndex
		kv.waitApplyCond.Broadcast()
		if _, isOk := kv.executedMsg[m.CommandIndex]; isOk {
			kv.executedMsg[m.CommandIndex] = m
		}
		kv.monitorRaftState(m.CommandIndex, m.CommandTerm)
		if kv.killed() {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) updateWaitShards(oldConfig shardmaster.Config, newConfig shardmaster.Config) {
	// already have lock
	incomeShards := kv.getIncomeShardsWithLock(oldConfig, newConfig)
	for _, shardNum := range incomeShards {
		targetGroup := oldConfig.Shards[shardNum]
		kv.shardWaitToPull[shardNum] = targetGroupInfo{
			TargetGroupID: targetGroup,
			Servers:       oldConfig.Groups[targetGroup],
			OldConfigNum:  oldConfig.Num,
		}
	}
	outShards := kv.getOutShardsWithLock(oldConfig, newConfig)
	for _, shardNum := range outShards {
		kv.shardWaitToOut[shardNum] = oldConfig.Num
	}
}

func (kv *ShardKV) monitorRaftState(logIndex int, term int) {
	// already have kv lock
	portion := 2.0 / 3
	if kv.maxraftstate <= 0 || kv.persister.RaftStateSize() < int(float64(kv.maxraftstate)*portion) {
		return
	}
	//KVPrintf(kv, "approach to maxraftstate, start snapshot, logIndex = %v, raftstatesize = %v",
	//	logIndex, kv.persister.RaftStateSize())
	rawSnapshot := kv.encodeKVState()
	// cannot take kv's lock, so need a new goroutine
	go func() { kv.rf.TakeSnapshot(logIndex, term, rawSnapshot) }()
}

func (kv *ShardKV) encodeKVState() []byte {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	err := encoder.Encode(kv.stateMachine)
	err = encoder.Encode(kv.config)
	err = encoder.Encode(kv.shardWaitToPull)
	err = encoder.Encode(kv.shardWaitToOut)
	if err != nil {
		fmt.Printf("encode kv state failed on kvserver %v, err = %v\n", kv.me, err)
	}
	data := w.Bytes()
	return data
}

func (kv *ShardKV) decodeKVState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var stateMachine map[int]Shard
	var cfg shardmaster.Config
	var shardWaitToPull map[int]targetGroupInfo
	var shardWaitToOut map[int]int
	if d.Decode(&stateMachine) != nil || d.Decode(&cfg) != nil || d.Decode(&shardWaitToPull) != nil ||
		d.Decode(&shardWaitToOut) != nil {
		fmt.Printf("decode kv state failed on kvserver %v\n", kv.me)
	} else {
		kv.mu.Lock()
		kv.stateMachine = stateMachine
		kv.config = cfg
		kv.shardWaitToPull = shardWaitToPull
		kv.shardWaitToOut = shardWaitToOut
		kv.mu.Unlock()
	}
}

func getShardsByGID(gid int, cfg shardmaster.Config) map[int]int {
	result := make(map[int]int)
	for i := 0; i < len(cfg.Shards); i++ {
		if cfg.Shards[i] == gid {
			result[i] = gid
		}
	}
	return result
}

func (kv *ShardKV) sendReadySignal(shardNum int, g targetGroupInfo) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	servers := g.Servers
	targetGroup := g.TargetGroupID
	args := TellReadyArgs{
		ShardNum:  shardNum,
		ConfigNum: g.OldConfigNum,
	}
	reply := TellReadyReply{}
Loop:
	for {
		if _, ok := kv.groupLeader[targetGroup]; !ok {
			kv.groupLeader[targetGroup] = 0
		}
		for i := 0; i < len(servers); i++ {
			srv := kv.make_end(servers[kv.groupLeader[targetGroup]])
			KVPrintf(kv, "send TellReady RPC to server-%v-%v, shardNum-%v, oldCfgNum-%v", targetGroup,
				kv.groupLeader[targetGroup], shardNum, g.OldConfigNum)
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.TellReady", &args, &reply)
			kv.mu.Lock()
			if !ok || reply.Err == ErrWrongLeader {
				kv.groupLeader[targetGroup] = (kv.groupLeader[targetGroup] + 1) % len(servers)
				continue
			}
			break Loop
		}
		kv.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
		kv.mu.Lock()
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) getIncomeShardsWithLock(oldConfig shardmaster.Config, newConfig shardmaster.Config) []int {
	// already get lock
	var incomeShard []int
	oldShards := getShardsByGID(kv.gid, oldConfig)
	newShards := getShardsByGID(kv.gid, newConfig)
	if len(oldShards) >= len(newShards) {
		return incomeShard
	}
	for shardNum, _ := range newShards {
		if _, ok := oldShards[shardNum]; ok {
			continue
		}
		incomeShard = append(incomeShard, shardNum)
	}
	return incomeShard
}

func (kv *ShardKV) getOutShardsWithLock(oldConfig shardmaster.Config, newConfig shardmaster.Config) []int {
	// already get lock
	var outShard []int
	oldShards := getShardsByGID(kv.gid, oldConfig)
	newShards := getShardsByGID(kv.gid, newConfig)
	if len(oldShards) <= len(newShards) {
		return outShard
	}
	for shardNum, _ := range oldShards {
		if _, ok := newShards[shardNum]; ok {
			continue
		}
		outShard = append(outShard, shardNum)
	}
	return outShard
}

func (kv *ShardKV) requestShardData(shardNum int, g targetGroupInfo) map[int]Shard {
	// already have lock
	targetGroup := g.TargetGroupID
	result := make(map[int]Shard)
	if targetGroup == 0 {
		result[shardNum] = Shard{
			ShardID:          shardNum,
			ConfigNum:        1,
			Data:             make(map[string]string),
			ClientRequestSeq: make(map[int64]int32),
		}
		return result
	}
	args := PullShardArgs{
		ConfigNum: g.OldConfigNum,
		ShardNum:  shardNum,
	}
	reply := PullShardReply{}
	servers := g.Servers
	if _, ok := kv.groupLeader[targetGroup]; !ok {
		kv.groupLeader[targetGroup] = 0
	}
	for i := 0; i < len(servers); i++ {
		srv := kv.make_end(servers[kv.groupLeader[targetGroup]])
		KVPrintf(kv, "send PullShard request to server-%v-%v, shardNum-%v, oldShard.configNum-%v",
			targetGroup, kv.groupLeader[targetGroup], shardNum, g.OldConfigNum)
		kv.mu.Unlock()
		ok := srv.Call("ShardKV.PullShard", &args, &reply)
		kv.mu.Lock()
		if !ok || reply.ConfigNum <= g.OldConfigNum {
			kv.groupLeader[targetGroup] = (kv.groupLeader[targetGroup] + 1) % len(servers)
			KVPrintf(kv, "do not get shardNum-%v from server-%v-%v, ok-%v, reply.ConfigNum-%v",
				shardNum, targetGroup, kv.groupLeader[targetGroup], ok, reply.ConfigNum)
			continue
		}
		result[shardNum] = Shard{
			ShardID:          shardNum,
			ConfigNum:        Min(reply.ConfigNum, kv.config.Num),
			Data:             deepCopyShardData(reply.ShardData),
			ClientRequestSeq: deepCopyClientRequestSeq(reply.ClientRequestSeq),
		}
		KVPrintf(kv, "get shardNum-%v from server-%v-%v, newShardVersion-%v",
			shardNum, targetGroup, kv.groupLeader[targetGroup], result[shardNum].ConfigNum)
		break
	}
	return result
}

func (kv *ShardKV) tryToPullShards() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			for shardNum, g := range kv.shardWaitToPull {
				newShard := kv.requestShardData(shardNum, g)
				if len(newShard) == 0 {
					continue
				}
				command := Op{
					Operation: UpdateShard,
					Shards:    newShard,
				}
				kv.mu.Unlock()
				commandIndex, _, _ := kv.rf.Start(command)
				kv.mu.Lock()
				KVPrintf(kv, "get shardNum-%v, put shard into raft, logIndex-%v", shardNum, commandIndex)
			}
			kv.mu.Unlock()
		}
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
	}
}

func (kv *ShardKV) checkConfigUpdate() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			if len(kv.shardWaitToPull) == 0 {
				newConfigNum := kv.config.Num + 1
				kv.mu.Unlock()
				newConfig := kv.mck.Query(newConfigNum)
				kv.mu.Lock()
				if kv.config.Num < newConfig.Num {
					KVPrintf(kv, "find new configuration, old config = %v, new config = %v", kv.config, newConfig)
					kv.mu.Unlock()
					command := Op{
						Operation: ReconfigureOp,
						Config:    newConfig,
					}
					commandIndex, _, _ := kv.rf.Start(command)
					kv.mu.Lock()
					KVPrintf(kv, "put new config into raft, logIndex-%v", commandIndex)
				}
			}
			kv.mu.Unlock()
		}
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.SetGroup(kv.gid) // for test print

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.startTime = time.Now()
	kv.timeout = 500 * time.Millisecond // 500ms for timeout
	kv.groupLeader = make(map[int]int)
	kv.stateMachine = make(map[int]Shard)
	for i := 0; i < len(kv.config.Shards); i++ {
		kv.stateMachine[i] = Shard{
			ShardID:          i,
			ConfigNum:        0,
			Data:             make(map[string]string),
			ClientRequestSeq: make(map[int64]int32),
		}
	}
	kv.shardWaitToPull = make(map[int]targetGroupInfo)
	kv.shardWaitToOut = make(map[int]int)
	kv.waitApplyCond = sync.NewCond(&kv.mu)
	kv.executedMsg = make(map[int]raft.ApplyMsg)

	kv.decodeKVState(kv.persister.ReadSnapshot())

	go kv.updateStateMachine()
	go kv.checkConfigUpdate()
	go kv.tryToPullShards()

	return kv
}
