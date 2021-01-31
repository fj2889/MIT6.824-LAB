package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	ClientID  int64
	RequestID int32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int             // snapshot if log grows this big
	persister    *raft.Persister // used to read snapshot and raft state size, do not be used to persist

	// Your definitions here.
	startTime time.Time
	timeout   time.Duration

	stateMachine    map[string]string
	clientRequestID map[int64]int32

	executedMsg       map[int]raft.ApplyMsg
	lastExecutedIndex int
	waitApplyCond     *sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// get do not need check duplicate
	// send the log to raft
	command := Op{
		Key:       args.Key,
		Operation: GetOp,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	commandIndex, term, isLeader := kv.rf.Start(command) // cannot hold kv mutex!!!
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	KVPrintf(kv, "get a client-%v request-%v (GET), key-%v, logIndex-%v",
		args.ClientID%10000, args.RequestID, args.Key, commandIndex)

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
		reply.Err = ErrOpNotExecuted
		KVPrintf(kv, "reply the client-%v request-%v (GET) timeout: logIndex-%v, key-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.Key)
		return
	}

	// check applied command， should compare term!!!
	executedMsg := kv.executedMsg[commandIndex]
	if executedMsg.CommandTerm != term {
		reply.Err = ErrOpNotExecuted
		KVPrintf(kv, "reply the client-%v request-%v (GET) ErrOpNotExecuted: logIndex-%v, key-%v",
			args.ClientID%10000, args.RequestID, commandIndex, args.Key)
		return
	}

	kv.getValue(args, reply)
	KVPrintf(kv, "reply the client-%v request-%v (GET) success: logIndex-%v, key-%v, value-%v",
		args.ClientID%10000, args.RequestID, commandIndex, args.Key, reply.Value)
	return
}

func (kv *KVServer) getValue(args *GetArgs, reply *GetReply) {
	// get value, already have lock
	value, isOk := kv.stateMachine[args.Key]
	if isOk {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// check duplicate
	kv.mu.Lock()
	if currentRequestID, ok := kv.clientRequestID[args.ClientID]; ok && currentRequestID >= args.RequestID {
		reply.Err = ErrDuplicateRequest
		KVPrintf(kv, "reply the client-%v request-%v (%v): key-%v, value-%v, duplicate command",
			args.ClientID%10000, args.RequestID, args.Op, args.Key, args.Value)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// send the log to raft
	command := Op{
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	commandIndex, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	KVPrintf(kv, "get a client-%v request-%v (%v), logIndex-%v, key-%v, value-%v",
		args.ClientID%10000, args.RequestID, args.Op, commandIndex, args.Key, args.Value)

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
		reply.Err = ErrOpNotExecuted
		KVPrintf(kv, "reply the client-%v request-%v (%v) timeout: logIndex-%v, key-%v, value-%v",
			args.ClientID%10000, args.RequestID, args.Op, commandIndex, args.Key, args.Value)
		return
	}

	// check applied command
	executedMsg := kv.executedMsg[commandIndex]
	if executedMsg.CommandTerm != term {
		reply.Err = ErrOpNotExecuted
		KVPrintf(kv, "reply the client-%v request-%v (%v) ErrOpNotExecuted: logIndex-%v, key-%v, value-%v",
			args.ClientID%10000, args.RequestID, args.Op, commandIndex, args.Key, args.Value)
		return
	}
	reply.Err = OK
	KVPrintf(kv, "reply the client-%v request-%v (%v) success: logIndex-%v, key-%v, value-%v",
		args.ClientID%10000, args.RequestID, args.Op, commandIndex, args.Key, args.Value)
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

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) updateStateMachine() {
	for m := range kv.applyCh {
		if !m.CommandValid {
			KVPrintf(kv, "command is invalid, time to decode Snapshot")
			rawSnapshot := m.Command.([]byte)
			kv.decodeKVState(rawSnapshot)
			continue
		}
		command := m.Command.(Op)
		kv.mu.Lock()
		// prevent duplicate command here!!!!
		if kv.clientRequestID[command.ClientID] >= command.RequestID {
			//KVPrintf(kv, "client-%v request-%v logIndex-%v, duplicate command, do not execute",
			//	command.ClientID%10000, command.RequestID, m.CommandIndex)
		} else {
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
			default:
				KVPrintf(kv, "unknow command operation type, logIndex-%v", m.CommandIndex)
			}
			kv.clientRequestID[command.ClientID] = command.RequestID
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

func (kv *KVServer) monitorRaftState(logIndex int, term int) {
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

func (kv *KVServer) encodeKVState() []byte {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	err := encoder.Encode(kv.stateMachine)
	err = encoder.Encode(kv.clientRequestID)
	if err != nil {
		fmt.Printf("encode kv state failed on kvserver %v, err = %v\n", kv.me, err)
	}
	data := w.Bytes()
	return data
}

func (kv *KVServer) decodeKVState(data []byte) {
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
		kv.clientRequestID = clientRequestID
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.startTime = time.Now()
	kv.timeout = 500 * time.Millisecond // 500ms for timeout
	kv.stateMachine = make(map[string]string)
	kv.clientRequestID = make(map[int64]int32)
	kv.waitApplyCond = sync.NewCond(&kv.mu)
	kv.executedMsg = make(map[int]raft.ApplyMsg)

	kv.decodeKVState(kv.persister.ReadSnapshot())

	go kv.updateStateMachine()

	return kv
}
