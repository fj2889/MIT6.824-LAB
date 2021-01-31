package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State

	// for snapshot
	lastIncludedIndex int
	lastIncludedTerm  int

	// state for all server
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	allBegin    time.Time // just for print out

	// state for follower
	lastHeart    time.Time
	hbInterval   time.Duration // heartbeat interval
	electTimeout time.Duration

	// state for leader
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == leader

	return term, isleader
}

func (rf *Raft) logIndexInSlice(logIndex int) int {
	if logIndex-rf.lastIncludedIndex < 0 {
		RaftPrint(rf, "use logIndex already in snapshot, logIndex = %v, rf.lastIncludedIndex = %v, len(rf.log) = %v, lastLogIndex = %v",
			logIndex, rf.lastIncludedIndex, len(rf.log), rf.lastLog().LogIndex)
		panic("use logIndex already in snapshot")
	}
	return logIndex - rf.lastIncludedIndex
}

func (rf *Raft) lastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) encodePersistedState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	err = e.Encode(rf.votedFor)
	err = e.Encode(rf.log)
	err = e.Encode(rf.lastIncludedIndex)
	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		RaftPrint(rf, "encode when persist failed, err = %v", err)
		panic("encode when persist failed")
	}
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// read and persist should keep the same order !!!!!
	data := rf.encodePersistedState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// read and persist should keep the same order !!!!!
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Printf("decode persisted state failed on raft server %v\n", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.mu.Unlock()
	}
}

func (rf *Raft) TakeSnapshot(lastIncludedIndex int, lastIncludedTerm int, rawSnapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// due to new goroutine to execute this function, there could be multiple execution of this func at the same time
	if rf.lastApplied > lastIncludedIndex || rf.lastIncludedIndex >= lastIncludedIndex {
		return
	}
	RaftPrint(rf, "begin to take snapshot in raft，current raftStateSize = %v, new lastIncludedIndex = %v",
		rf.persister.RaftStateSize(), lastIncludedIndex)
	logs := make([]LogEntry, 0)
	// keep log[0] due to commitIndex and lastApplied start from 0!!!
	logs = append(logs, rf.log[rf.logIndexInSlice(lastIncludedIndex):]...)

	rf.log = logs
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	rf.persister.SaveStateAndSnapshot(rf.encodePersistedState(), rawSnapshot)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// request have higher term or this server have not voted yet
	if args.Term > rf.currentTerm || (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if args.Term > rf.currentTerm {
			rf.state = follower
			rf.currentTerm = args.Term
			rf.persist()
		}
		// election restrict
		if rf.electionRestrict(args) {
			rf.succedToVote(args, reply)
			return
		}
		RaftPrint(rf, "not vote for server %v due to election restrict", args.CandidateId)
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

func (rf *Raft) electionRestrict(args *RequestVoteArgs) bool {
	if args.LastLogTerm > rf.lastLog().LogTerm ||
		(args.LastLogTerm == rf.lastLog().LogTerm && args.LastLogIndex >= rf.lastLog().LogIndex) {
		return true
	}
	return false
}

func (rf *Raft) succedToVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// already have lock
	RaftPrint(rf, "vote for peer %v", args.CandidateId)
	rf.lastHeart = time.Now()
	rf.state = follower
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.persist()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.LastLogIndex = rf.lastLog().LogIndex
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// put these three change out of log consistency check!!!
	rf.lastHeart = time.Now()
	rf.state = follower
	rf.currentTerm = args.Term
	rf.persist()

	if rf.lastIncludedIndex > args.PrevLogIndex {
		// prevLogIndex is in the log which has already been snapshot
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = rf.lastIncludedIndex + 1
		return
	}

	// log consistency check
	if args.PrevLogIndex <= rf.lastLog().LogIndex && rf.log[rf.logIndexInSlice(args.PrevLogIndex)].LogTerm == args.PrevLogTerm {
		// can not truncate rf.log without check, if rf.log have more entries than args.entries!!!
		flag := false // whether there is a conflict
		lastIndex := Min(len(args.Entries), len(rf.log[rf.logIndexInSlice(args.PrevLogIndex)+1:]))
		for i := 0; i < lastIndex; i++ {
			if rf.log[rf.logIndexInSlice(args.PrevLogIndex)+1+i].LogTerm != args.Entries[i].LogTerm {
				flag = true
				break
			}
		}
		// if rf.log have more log entries without conflict, do not truncate
		if len(rf.log[rf.logIndexInSlice(args.PrevLogIndex)+1:]) <= len(args.Entries) || flag {
			rf.log = rf.log[:rf.logIndexInSlice(args.PrevLogIndex)+1]
			rf.log = append(rf.log, args.Entries...)
		}
		rf.persist()
		// min(leaderCommit, index of last new entry)!!!
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		reply.Success = true
	} else {
		RaftPrint(rf, "log is not consistent, AE failed")
		if args.PrevLogIndex <= rf.lastLog().LogIndex {
		}
		xIndex := 0
		xTerm := 0
		if args.PrevLogIndex <= rf.lastLog().LogIndex {
			xTerm = rf.log[rf.logIndexInSlice(args.PrevLogIndex)].LogTerm
			for i := 0; i < len(rf.log); i++ {
				if rf.log[i].LogTerm == xTerm {
					xIndex = i
					break
				}
			}
		}
		reply.XIndex = xIndex
		reply.XTerm = xTerm
		reply.Success = false
		return
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	RaftPrint(rf, "get InstallSnapshot RPC")
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.lastHeart = time.Now()
	rf.state = follower
	rf.currentTerm = args.Term
	rf.persist()

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		RaftPrint(rf, "in InstallSnapshot RPC,  rf.lastIncludedIndex-%v >= args.LastIncludedIndex-%v",
			rf.lastIncludedIndex, args.LastIncludedIndex)
		return
	}
	logs := make([]LogEntry, 0)
	// logs[0] for placement, must be consistent !!!! should not use origin rf.log[rf.logIndexInSlice(args.LastIncludedIndex)]
	logs = append(logs, LogEntry{
		LogIndex: args.LastIncludedIndex,
		LogTerm:  args.LastIncludedTerm,
		Command:  nil,
	})
	if args.LastIncludedIndex < rf.lastLog().LogIndex {
		logs = append(logs, rf.log[rf.logIndexInSlice(args.LastIncludedIndex)+1:]...)
	}
	rf.log = logs
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.encodePersistedState(), args.Data)

	rf.lastApplied = args.LastIncludedIndex
	//rf.commitIndex = args.LastIncludedIndex
	RaftPrint(rf, "InstallSnapshot complete")

	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		CommandIndex: -1,
		Command:      rf.persister.ReadSnapshot(),
		CommandTerm:  rf.currentTerm,
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.state != leader {
		isLeader = false
		return index, term, isLeader
	}

	// logIndex start from 1
	index = rf.lastLog().LogIndex + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		LogTerm:  rf.currentTerm,
		LogIndex: index,
		Command:  command,
	})
	rf.persist()
	RaftPrint(rf, "get a client request, command = %v, logIndex = %v", command, index)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) run() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			RaftPrint(rf, "get quit signal")
			rf.mu.Unlock()
			break
		}
		switch rf.state {
		case leader:
			rf.mu.Unlock()
			rf.initLeader()
			rf.doLeaderJob()
		case candidate:
			rf.mu.Unlock()
			rf.doCandidateJob()
		case follower:
			rf.mu.Unlock()
			rf.doFollowerJob()
		default:
			rf.mu.Unlock()
			fmt.Printf("find an unknown server state on server %v at Term %v\n", rf.me, rf.currentTerm)
			rf.Kill()
			return
		}
	}
}

func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// at very beginning, the initial value of nextIndex is 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLog().LogIndex + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) doLeaderJob() {
	for {
		if rf.killed() {
			break
		}

		rf.mu.Lock()
		if rf.state != leader {
			RaftPrint(rf, "not be a leader at begining of doLeaderJob")
			rf.mu.Unlock()
			break
		}
		rf.lastHeart = time.Now() // lastheart in leader means last time the leader send heartbeat to others
		RaftPrint(rf, "send heartbeat to all others")
		rf.mu.Unlock()

		// send AppendEntries RPC to all other servers
		count, finished := 0, 0
		cond := sync.NewCond(&rf.mu)
		myTimer(&rf.mu, cond, rf.hbInterval) // prevent too long no reply from Call
		rf.traverseToAppendEntries(&finished, &count, cond)

		rf.mu.Lock()
		for (count+1) <= len(rf.peers)/2 && finished < len(rf.peers)-1 &&
			time.Now().Sub(rf.lastHeart) < rf.hbInterval && rf.state == leader {
			cond.Wait()
		}
		if rf.state != leader {
			RaftPrint(rf, "not be a leader after send heartbeat to all others")
			rf.mu.Unlock()
			break
		}
		if (count + 1) > len(rf.peers)/2 {
			RaftPrint(rf, "heartbeat reach majority server")
			// After every success heartbeat, push forward commitIndex
			rf.pushLeaderCommitIndex()
		} else {
			RaftPrint(rf, "leader do not get majority reply")
		}
		lastHeart := rf.lastHeart
		hbInterval := rf.hbInterval
		rf.mu.Unlock()

		if time.Now().Sub(lastHeart) < hbInterval {
			time.Sleep(hbInterval - time.Now().Sub(lastHeart))
		}
	}
}

func (rf *Raft) pushLeaderCommitIndex() {
	// already have the lock
	// compute the new commitIndex for leader
	n := rf.commitIndex
	for {
		n++
		totalMatch := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= n {
				totalMatch++
			}
		}
		if (totalMatch + 1) <= len(rf.peers)/2 {
			break
		}
	}
	// Safety: do not commit entries from previous term!!!
	if (n-1) > rf.commitIndex && rf.log[rf.logIndexInSlice(n-1)].LogTerm == rf.currentTerm {
		rf.commitIndex = n - 1
		RaftPrint(rf, "update commit index to %v", rf.commitIndex)
	}
}

func (rf *Raft) traverseToAppendEntries(finished *int, count *int, cond *sync.Cond) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			rf.mu.Unlock()
			go rf.sendInstallSnapshot(finished, count, cond, i)
		} else {
			rf.mu.Unlock()
			go rf.sendAppendEntries(finished, count, cond, i)
		}
	}
}

func (rf *Raft) sendInstallSnapshot(finished *int, count *int, cond *sync.Cond, serverIndex int) {
	rf.mu.Lock()
	if rf.state != leader {
		RaftPrint(rf, "(SS) not be a leader when prepare snapshotArgs for server %v", serverIndex)
		*finished++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.peers[serverIndex].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		rf.mu.Lock()
		//RaftPrint(rf, "do not get reply from peer %v", serverIndex)
		*finished++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	// check if an old delay reply
	if rf.state != leader || args.Term != rf.currentTerm {
		RaftPrint(rf, "(SS) get old reply or not be leader after get reply from server %v", serverIndex)
		*finished++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.currentTerm {
		RaftPrint(rf, "(SS) get a larger term %v from peer %v", reply.Term, serverIndex)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = follower
	} else {
		rf.nextIndex[serverIndex] = args.LastIncludedIndex + 1
		rf.matchIndex[serverIndex] = args.LastIncludedIndex
		*count++
	}
	*finished++
	cond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(finished *int, count *int, cond *sync.Cond, serverIndex int) {
	rf.mu.Lock()
	// fix trick bug, should check state here!!!
	if rf.state != leader || rf.nextIndex[serverIndex] <= rf.lastIncludedIndex {
		RaftPrint(rf, "(AE) not be a leader when prepare heartbeat for server %v, or rf.nextIndex[%v]-%v <= rf.lastIncludedIndex-%v",
			serverIndex, serverIndex, rf.nextIndex[serverIndex], rf.lastIncludedIndex)
		*finished++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}
	args := rf.createAppendEntriesArgs(serverIndex)
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	ok := rf.peers[serverIndex].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		rf.mu.Lock()
		//RaftPrint(rf, "do not get reply from peer %v", serverIndex)
		*finished++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	// check if an old delay reply
	if rf.state != leader || args.Term != rf.currentTerm {
		RaftPrint(rf, "(AE) get old reply or not be leader after get reply from server %v", serverIndex)
		*finished++
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}
	if reply.Success {
		// update nextIndex and matchIndex  should use previous args' value !!!!
		rf.nextIndex[serverIndex] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[serverIndex] = args.PrevLogIndex + len(args.Entries)
		*count++
	} else if reply.Term > rf.currentTerm {
		RaftPrint(rf, "(AE) get a larger term %v from peer %v", reply.Term, serverIndex)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = follower
	} else {
		rf.handleInconsistency(serverIndex, args, reply)
	}
	*finished++
	cond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) createAppendEntriesArgs(serverIndex int) AppendEntriesArgs {
	// already have lock
	var entries []LogEntry
	if rf.nextIndex[serverIndex] <= rf.lastLog().LogIndex {
		// should not use entries = rf.log[rf.nextIndex[serverIndex]:], will cause data race!!!
		entries = append(entries, rf.log[rf.logIndexInSlice(rf.nextIndex[serverIndex]):]...)
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.log[rf.logIndexInSlice(rf.nextIndex[serverIndex])-1].LogIndex,
		PrevLogTerm:  rf.log[rf.logIndexInSlice(rf.nextIndex[serverIndex])-1].LogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) handleInconsistency(serverIndex int, args AppendEntriesArgs, reply AppendEntriesReply) {
	// already have lock
	if reply.XTerm == -1 {
		// case 0: nextInt is smaller than follower's lastIncludedIndex
		rf.nextIndex[serverIndex] = reply.XIndex
	}
	if reply.LastLogIndex < args.PrevLogIndex {
		// case 1: log length in server-index too short
		rf.nextIndex[serverIndex] = reply.LastLogIndex + 1
	} else {
		lastIndexOfXterm := 0 // leader's last log entry for XTerm
		for j := 0; j < len(rf.log); j++ {
			if rf.log[j].LogTerm == reply.XTerm {
				lastIndexOfXterm = j
			}
		}
		if lastIndexOfXterm == 0 {
			// case 2: leader does not have log with Xterm
			rf.nextIndex[serverIndex] = reply.XIndex
		} else {
			// case 3
			rf.nextIndex[serverIndex] = lastIndexOfXterm
		}
	}
}

func (rf *Raft) doCandidateJob() {
	// start election
	for {
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		if rf.state != candidate {
			rf.mu.Unlock()
			break
		}
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		// restart election timeout at the start of election
		rf.electTimeout = time.Duration(250+rand.Intn(150)) * time.Millisecond
		rf.lastHeart = time.Now()
		RaftPrint(rf, "start a new election")
		rf.mu.Unlock()

		// send RequestVote RPC to all other servers
		count, finished := 0, 0
		cond := sync.NewCond(&rf.mu)
		myTimer(&rf.mu, cond, rf.electTimeout) // prevent too long no reply from Call
		rf.traverseToRequestVote(&finished, &count, cond)

		rf.mu.Lock()
		for (count+1) <= len(rf.peers)/2 && finished < len(rf.peers)-1 &&
			time.Now().Sub(rf.lastHeart) < rf.electTimeout && rf.state == candidate {
			cond.Wait()
		}
		if rf.state != candidate {
			RaftPrint(rf, "not be a candidate")
			rf.mu.Unlock()
			break
		}
		if (count + 1) > len(rf.peers)/2 {
			RaftPrint(rf, "become leader")
			// get enough vote: candidate to leader
			rf.state = leader
			rf.mu.Unlock()
			break
		}
		RaftPrint(rf, "do not get enough votes or election timeout")
		lastHeart := rf.lastHeart
		electTimeout := rf.electTimeout
		rf.mu.Unlock()
		// wait for timeout to elapse before starting the next election
		if time.Now().Sub(lastHeart) < electTimeout {
			time.Sleep(electTimeout - time.Now().Sub(lastHeart))
		}
	}
}

func (rf *Raft) traverseToRequestVote(finished *int, count *int, cond *sync.Cond) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			rf.mu.Lock()
			if rf.state != candidate {
				RaftPrint(rf, "not be a candidate when prepare vote request for server %v", index)
				*finished++
				cond.Broadcast()
				rf.mu.Unlock()
				return
			}
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastLog().LogIndex,
				LastLogTerm:  rf.lastLog().LogTerm,
			}
			reply := RequestVoteReply{}
			RaftPrint(rf, "request vote from peer %v", index)
			rf.mu.Unlock()
			ok := rf.sendRequestVote(index, &args, &reply)
			if !ok {
				rf.mu.Lock()
				//RaftPrint(rf, "do not get reply from peer %v", index)
				*finished++
				cond.Broadcast()
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			if rf.state != candidate || args.Term != rf.currentTerm {
				RaftPrint(rf, "get old reply or not be candidate after get reply from server %v", index)
				*finished++
				cond.Broadcast()
				rf.mu.Unlock()
				return
			}
			*finished++
			// get voted
			if reply.VoteGranted {
				*count++
			} else if reply.Term > rf.currentTerm {
				// if Term > currentTerm
				RaftPrint(rf, "get a larger term %v from peer %v", reply.Term, index)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				// higher term: candidate to follower
				rf.state = follower
			}
			cond.Broadcast()
			rf.mu.Unlock()
		}(i)
	}
}

func myTimer(mu *sync.Mutex, cond *sync.Cond, duration time.Duration) {
	go func() {
		time.Sleep(duration)
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}()
}

func (rf *Raft) doFollowerJob() {
	for {
		rf.mu.Lock()
		lastHeart := rf.lastHeart
		electTimeout := rf.electTimeout
		if time.Now().Sub(rf.lastHeart) > rf.electTimeout {
			RaftPrint(rf, "election timeout")
			// election timeout: follower to candidate
			rf.state = candidate
			rf.mu.Unlock()
			break
		} else {
			rf.mu.Unlock()
			time.Sleep(electTimeout - time.Now().Sub(lastHeart))
		}
	}
}

func (rf *Raft) doApply() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			break
		}
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				RaftPrint(rf, "apply commandIndex = %v command = %v", i, rf.log[rf.logIndexInSlice(i)].Command)
				rf.applyCh <- ApplyMsg{
					Command:      rf.log[rf.logIndexInSlice(i)].Command,
					CommandIndex: rf.log[rf.logIndexInSlice(i)].LogIndex,
					CommandTerm:  rf.currentTerm,
					CommandValid: true,
				}
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.allBegin = time.Now()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.lastHeart = time.Now()
	rf.hbInterval = 100 * time.Millisecond                                 // 150ms < 10 times/sec
	rf.electTimeout = time.Duration(250+rand.Intn(150)) * time.Millisecond // 250 - 400ms

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.lastIncludedIndex
	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntry{
			LogIndex: 0,
			LogTerm:  -1,
			Command:  nil,
		})
	}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go rf.run()
	go rf.doApply()

	return rf
}
