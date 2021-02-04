package shardkv

// import "../shardmaster"
import (
	"../labrpc"
	"../shardmaster"
	"bytes"
	"encoding/json"
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
	config   shardmaster.Config
	mck      *shardmaster.Clerk

	maxraftstate int             // snapshot if log grows this big
	persister    *raft.Persister // used to read snapshot and raft state size, do not be used to persist

	// Your definitions here.
	startTime time.Time
	timeout   time.Duration

	stateMachine     map[string]string
	clientRequestSeq map[int64]int32

	executedMsg       map[int]raft.ApplyMsg
	lastExecutedIndex int
	waitApplyCond     *sync.Cond
}

type Shard struct {
	ShardID int
	Data map[string]string
	clientRequestSeq map[int64]int32
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
		value, isOk := kv.stateMachine[args.Key]
		if isOk {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	}
	KVPrintf(kv, "reply the client-%v request-%v (GET) success: key-%v, value-%v, reply.Err-%v",
		args.ClientID%10000, args.RequestID, args.Key, reply.Value, reply.Err)
	return
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
	// check duplicate
	kv.mu.Lock()
	if command.Operation != GetOp {
		if currentRequestID, ok := kv.clientRequestSeq[command.ClientID]; ok && currentRequestID >= command.RequestID {
			result = ErrDuplicateRequest
			KVPrintf(kv, "reply the client-%v request-%v (%v) duplicate command: key-%v, value-%v", paraData1...)
			kv.mu.Unlock()
			return result
		}
	}
	// check group
	if kv.config.Shards[key2shard(command.Key)] != kv.gid {
		result = ErrWrongGroup
		paraData1 = append(paraData1, kv.config.Shards[key2shard(command.Key)], kv.config)
		KVPrintf(kv, "reply the client-%v request-%v (%v) wrong group: key-%v, value-%v, target_gid-%v, config-%v", paraData1...)
		kv.mu.Unlock()
		return result
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
	if kv.config.Shards[key2shard(command.Key)] != kv.gid {
		result = ErrWrongGroup
		paraData1 = append(paraData1, kv.config.Shards[key2shard(command.Key)], kv.config)
		KVPrintf(kv, "reply the client-%v request-%v (%v) wrong group: key-%v, value-%v, target_gid-%v, config-%v", paraData1...)
		return result
	}
	result = OK
	if command.Operation != GetOp {
		KVPrintf(kv, "reply the client-%v request-%v (%v) success: logIndex-%v, key-%v, value-%v", paraData...)
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

func (kv *ShardKV) checkDuplicateAndShard(command Op) bool {
	if (command.Key == "" && command.Config.Num != kv.config.Num) || (kv.clientRequestSeq[command.ClientID] < command.RequestID &&
		kv.config.Shards[key2shard(command.Key)] == kv.gid) {
		return true
	}
	return false
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
		// prevent duplicate command here!!!! prevent concurrent reconfig and request
		if kv.checkDuplicateAndShard(command) {
			switch command.Operation {
			case GetOp:
				KVPrintf(kv, "execute client-%v request-%v [GET], logIndex-%v",
					command.ClientID%10000, command.RequestID, m.CommandIndex)
			case PutOp:
				kv.stateMachine[command.Key] = command.Value
				KVPrintf(kv, "execute client-%v request-%v [PUT], logIndex-%v, key-%v, value-%v",
					command.ClientID%10000, command.RequestID, m.CommandIndex, command.Key, command.Value)
			case AppendOp:
				kv.stateMachine[command.Key] = kv.stateMachine[command.Key] + command.Value
				KVPrintf(kv, "execute client-%v request-%v [APPEND], logIndex-%v, key-%v, value-%v, new_value-%v",
					command.ClientID%10000, command.RequestID, m.CommandIndex, command.Key, command.Value, kv.stateMachine[command.Key])
			case ReconfigureOp:
				kv.config = deepCopyConfig(command.Config)
				KVPrintf(kv, "execute [reconfigure], logIndex-%v, new_config-%v",
					m.CommandIndex, kv.config)
			default:
				KVPrintf(kv, "unknow command operation type, logIndex-%v", m.CommandIndex)
			}
			kv.clientRequestSeq[command.ClientID] = command.RequestID
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

func (kv *ShardKV) monitorRaftState(logIndex int, term int) {
	// already have kv lock
	portion := 2.0 / 3
	if kv.maxraftstate <= 0 || kv.persister.RaftStateSize() < int(float64(kv.maxraftstate)*portion) {
		return
	}
	KVPrintf(kv, "approach to maxraftstate, start snapshot, raftstatesize = %v", kv.persister.RaftStateSize())
	rawSnapshot := kv.encodeKVState()
	// cannot take kv's lock, so need a new goroutine
	go func() { kv.rf.TakeSnapshot(logIndex, term, rawSnapshot) }()
}

func (kv *ShardKV) encodeKVState() []byte {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	err := encoder.Encode(kv.stateMachine)
	err = encoder.Encode(kv.clientRequestSeq)
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
	var stateMachine map[string]string
	var clientRequestID map[int64]int32
	if d.Decode(&stateMachine) != nil || d.Decode(&clientRequestID) != nil {
		fmt.Printf("decode kv state failed on kvserver %v\n", kv.me)
	} else {
		kv.mu.Lock()
		kv.stateMachine = stateMachine
		kv.clientRequestSeq = clientRequestID
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) checkConfigUpdate() {
	for {
		newConfig := kv.mck.Query(-1)
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			break
		}
		if kv.config.Num != newConfig.Num {
			KVPrintf(kv, "find new configuration, old config = %v, new config = %v", kv.config, newConfig)
			kv.mu.Unlock()
			command := Op{
				Operation: ReconfigureOp,
				Config:    newConfig,
			}
			_, _, _ = kv.rf.Start(command)
			kv.mu.Lock()
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

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.startTime = time.Now()
	kv.timeout = 500 * time.Millisecond // 500ms for timeout
	kv.stateMachine = make(map[string]string)
	kv.clientRequestSeq = make(map[int64]int32)
	kv.waitApplyCond = sync.NewCond(&kv.mu)
	kv.executedMsg = make(map[int]raft.ApplyMsg)

	kv.decodeKVState(kv.persister.ReadSnapshot())

	go kv.updateStateMachine()
	go kv.checkConfigUpdate()

	return kv
}

// ---------------------------------------------- Util -----------------------------------------------
func deepCopyConfig(src shardmaster.Config) shardmaster.Config {
	buffer, _ := json.Marshal(src)
	var dst shardmaster.Config
	_ = json.Unmarshal(buffer, &dst)
	return dst
}
