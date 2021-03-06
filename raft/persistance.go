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
	e.Encode(rf.log.Entries)
	e.Encode(rf.log.Offset)
	e.Encode(rf.log.SnapShot.TermNumber)
	e.Encode(rf.log.SnapShot.Index) // we could just encode rf.log, but the tests are dumb and will fail with the error "log size too large" if we also encode the snapshot here -.-
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.log.SnapShot.Data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) bool {
	if data == nil || len(data) < 1 {
		return false
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term Term
	var logEntries []LogEntry
	var offset int
	var snapshotTermNumber int
	var snapshotIndex int

	if d.Decode(&term) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&offset) != nil ||
		d.Decode(&snapshotTermNumber) != nil ||
		d.Decode(&snapshotIndex) != nil {
		log.Fatalf("Decoding failed!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.log = Log{
			Entries: logEntries,
			Offset:  offset,
			SnapShot: SnapShot{
				Data:       rf.persister.Copy().snapshot,
				TermNumber: snapshotTermNumber,
				Index:      snapshotIndex,
			},
		}
		rf.mu.Unlock()
	}

	return true
}
