package raft

func (rf *Raft) tryAppendEntry(command interface{}) {
	rf.mu.Lock()
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm.number,
	}
	rf.logEntries = append(rf.logEntries, logEntry)
	rf.mu.Unlock()

	rf.sendLogfEntries()
}

func (rf *Raft) sendLogfEntries() {
	logEntries := rf.getLogEntries()
	currentTerm := rf.getCurrentTermNumber()
	commitIndex := rf.getCommitIndex()
	for server_idx := range rf.peers {
		if server_idx != rf.me {
			nextIndex := rf.getNextIndexFor(server_idx)
			prevIndex := nextIndex - 1
			prevTerm := 0
			if prevIndex != -1 {
				prevTerm = logEntries[prevIndex].Term
			}
			appendEntriesArgs := AppendEntriesArgs{
				AppendEntriesTermNumber: currentTerm,
				LeaderId:                rf.me,
				PrevLogIndex:            prevIndex,
				PrevLogTerm:             prevTerm,
				Entries:                 logEntries[nextIndex:],
				LeaderCommit:            commitIndex,
			}
			appendEntriesReply := AppendEntriesReply{}
			go func(server_idx int, appendEntriesArgs AppendEntriesArgs, appendEntriesReply AppendEntriesReply) {
				ok := false
				for !rf.killed() && !ok {
					ok = rf.sendAppendEntries(server_idx, &appendEntriesArgs, &appendEntriesReply)
				}
			}(server_idx, appendEntriesArgs, appendEntriesReply)
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(appendEntriesReply AppendEntriesReply) {

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

	if rf.getCurrentState() != leader {
		return -1, -1, false
	}

	rf.mu.Lock()
	index := len(rf.logEntries)
	currentTerm := rf.currentTerm.number
	rf.mu.Unlock()

	go rf.tryAppendEntry(command)

	return index, currentTerm, true
}
