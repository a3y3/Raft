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
// that index. Raft will now trim its upto this index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	index -= 1 // tests assume logs are 1 indexed
	rf.logMsg(SNAPSHOT, fmt.Sprintf("Snapshot called - trimming all entries upto and including %v", index))
	rf.mu.Lock()
	offset := rf.log.Offset
	if index-offset+1 > len(rf.log.Entries) {
		// Install snapshot RPC was likely called just before this, so we can't take a snapshot
		rf.mu.Unlock()
		rf.logMsg(SNAPSHOT, fmt.Sprintf("Warning: Skipping Snapshot() since index-offset+1=%v, and logEntries=%v (offset %v)", index-offset+1, rf.getLogEntries(), rf.getOffset()))
		return
	}
	rf.log.Entries = rf.log.Entries[index-offset+1:]
	rf.log.Offset = index + 1
	rf.log.SnapShot.Data = snapshot
	rf.log.SnapShot.TermNumber = rf.currentTerm.Number
	rf.log.SnapShot.Index = index
	rf.unsafePersist()
	rf.mu.Unlock()
	rf.logMsg(SNAPSHOT, fmt.Sprintf("Trimmed logs: %v (offset %v)", rf.getLogEntries(), rf.getOffset()))
}

// InstallSnapshot RPC handler is called by the leader on a server when it determines that it needs to send information to the server that it trimmed.
// The server upon receiving this RPC, will delete its log entries, store the snapshot with itself, and update the state machine with the contents.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.rpcLock.Lock()
	defer rf.rpcLock.Unlock()

	currentTerm := rf.getCurrentTermNumber()

	*reply = InstallSnapshotReply{
		TermNumber: currentTerm,
	}

	if args.TermNumber >= currentTerm {
		rf.setReceivedHeartBeat(true)
		if args.TermNumber > currentTerm {
			term := generateNewTerm(args.TermNumber, follower, generateNewElectionTimeout())
			rf.setTerm(term)
		}
		snapshotIndex := args.LastIncludedIndex
		snapshotTerm := args.LastIncludedTerm
		logEntries := rf.getLogEntries()
		offset := rf.getOffset()
		resetStateMachine := false
		if snapshotIndex-offset >= 0 && snapshotIndex-offset < len(logEntries) && logEntries[snapshotIndex-offset].Term == snapshotTerm {
			rf.setLogEntries(logEntries[snapshotIndex-offset+1:])
			rf.logMsg(INSTALL_SNAP, fmt.Sprintf("Trimmed logs until %v. New logs are %v", snapshotIndex-offset, rf.getLogEntries()))
		} else {
			rf.setLogEntries(make([]LogEntry, 0))
			rf.logMsg(INSTALL_SNAP, "Deleted all log entries!")
			resetStateMachine = true
		}
		rf.setOffset(snapshotIndex + 1)
		rf.mu.Lock()
		rf.log.SnapShot = SnapShot{
			Data:       args.SnapshotData,
			TermNumber: snapshotTerm,
			Index:      snapshotIndex,
		}
		rf.unsafePersist()
		rf.mu.Unlock()
		rf.logMsg(INSTALL_SNAP, fmt.Sprintf("Updated offset to %v and installed Snapshot!", snapshotIndex+1))
		if resetStateMachine {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.SnapshotData,
				SnapshotTerm:  snapshotTerm,
				SnapshotIndex: snapshotIndex + 1,
			}
			rf.setLastApplied(snapshotIndex)
			rf.logMsg(INSTALL_SNAP, "Sent Snapshot data on applyCh!")
		}
	}
}

// Send an AppenEntries RPC to a server.
// server is the index of the target server in rf.peers[].
//
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
