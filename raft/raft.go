package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

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

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func (rf *Raft) logMsg(topic Topic, msg string) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Milliseconds()
		log.Printf("%v %v %v T%v S%v %v\n", time, topic, rf.getCurrentState(), rf.getCurrentTermNumber(), rf.me, msg)
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = -1
	}
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	prevState := rf.readPersist(persister.ReadRaftState())
	if !prevState {
		newTerm := generateNewTerm(0, follower, generateNewElectionTimeout())
		rf.setTerm(newTerm)
	}

	rf.logMsg(PERSIST, fmt.Sprintf("Read state. votedFor: %v, term: %v, entries: %v", rf.getVotedFor(), rf.getCurrentTermNumber(), rf.getLogEntries()))

	go rf.tickr()

	return rf
}
