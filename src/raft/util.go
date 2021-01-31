package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const (
	Debug     = 0
	PrintRaft = false
)

func Min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

func Max(x int, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func RaftPrint(rf *Raft, format string, a ...interface{}) {
	if PrintRaft {
		format = "%v: [raft %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{time.Now().Sub(rf.allBegin).Milliseconds(), rf.me, rf.state, rf.currentTerm}, a...)
		fmt.Printf(format, a...)
	}
}
