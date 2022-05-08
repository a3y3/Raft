package raft

import (
	"fmt"
)

//
// !! DEPRECATED !!
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	index -= 1 // tests assume logs are 1 indexed
	rf.logMsg(SNAPSHOT, fmt.Sprintf("Snapshot called - trimming all entries upto and including %v", index))
	rf.mu.Lock()
	offset := rf.log.Offset
	rf.log.Entries = rf.log.Entries[index-offset+1:]
	rf.log.Offset = index + 1
	rf.log.SnapShot.Data = snapshot
	rf.log.SnapShot.TermNumber = rf.currentTerm.Number
	rf.mu.Unlock()
	rf.logMsg(SNAPSHOT, fmt.Sprintf("Trimmed logs: %v (offset %v)", rf.getLogEntries(), rf.getOffset()))
}
