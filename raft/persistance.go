package raft

import (
	"bytes"
	"log"

	"6.824/labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) unsafePersist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) bool {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return false
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term Term
	var logEntries []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&logEntries) != nil {
		log.Fatalf("Decoding failed!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.logEntries = logEntries
		rf.mu.Unlock()
	}
	return true
}
