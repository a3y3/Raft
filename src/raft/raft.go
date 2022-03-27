package raft

import (
	"sync/atomic"

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
func (rp *Raft) Kill() {
	atomic.StoreInt32(&rp.dead, 1)
	// Your code here, if desired.
}

func (rp *Raft) killed() bool {
	z := atomic.LoadInt32(&rp.dead)
	return z == 1
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
	rp := &Raft{}
	rp.peers = peers
	rp.persister = persister
	rp.me = me

	// initialize from state persisted before a crash
	rp.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rp.ticker()

	return rp
}
