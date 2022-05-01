package raft

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
func (rp *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}
