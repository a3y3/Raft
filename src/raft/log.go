package raft

import "fmt"

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rpcLock.Lock()
	defer rf.rpcLock.Unlock()

	currentTerm := rf.getCurrentTermNumber()
	success := false

	if args.AppendEntriesTermNumber >= currentTerm {
		rf.setReceivedHeartBeat(true)
		rf.setLeaderId(args.LeaderId)
		if args.AppendEntriesTermNumber > currentTerm {
			term := generateNewTerm(args.AppendEntriesTermNumber, follower, generateNewElectionTimeout())
			rf.setTerm(term)
		}
		prevIndex := args.PrevLogIndex
		logEntries := rf.getLogEntries()

		if prevIndex == -1 {
			// upsert without comparing
			rf.logMsg(APPLOGREQ, "Upserting without comparing as prevIndex is -1")
			rf.upsertLogs(0, args.Entries)
			success = true
		} else {
			// first check if prevIndex is valid
			if prevIndex >= len(logEntries) {
				rf.logMsg(APPLOGREQ, fmt.Sprintf("prevIndex is too high (%v > %v). Replying false", prevIndex, len(logEntries)))
				success = false
			} else {
				// finally, we can compare the terms of the 2 prev indices
				if logEntries[prevIndex].Term != args.PrevLogTerm {
					rf.logMsg(APPLOGREQ, fmt.Sprintf("Term mismatch, replying false (%v != %v)", logEntries[prevIndex].Term, args.PrevLogTerm))
					success = false
				} else {
					rf.logMsg(APPLOGREQ, "Terms match! Upserting...")
					rf.upsertLogs(prevIndex+1, args.Entries)
					success = true
				}
			}
		}

		if success && args.LeaderCommit > rf.getCommitIndex() {
			newCommitIndex := min(args.LeaderCommit, rf.getLogLength()-1)
			rf.logMsg(APPLOGREQ, fmt.Sprintf("Updating commitIndex to %v", newCommitIndex))
			rf.setCommitIndex(newCommitIndex)
		}
	}

	*reply = AppendEntriesReply{
		ReplyEntriesTermNumber: currentTerm,
		Success:                success,
	}
}

func (rf *Raft) upsertLogs(startingIndex int, leaderLogs []LogEntry) {
	rf.mu.Lock()
	for _, logEntry := range leaderLogs {
		if startingIndex >= len(rf.logEntries) {
			rf.logEntries = append(rf.logEntries, logEntry)
		} else {
			if rf.logEntries[startingIndex].Term != logEntry.Term {
				//Fig 2. AppendEntries.3
				rf.logEntries = rf.logEntries[:startingIndex]   // Trim everything including this entry
				rf.logEntries = append(rf.logEntries, logEntry) // append the new log
			} else {
				rf.logEntries[startingIndex] = logEntry
			}
		}
		startingIndex++
	}
	rf.mu.Unlock()

	rf.logMsg(UPSERTLOG, fmt.Sprintf("Upsert finished. New logs are %v", rf.getLogEntries()))
}

func (rf *Raft) sendLogEntries(server_idx int, currentTerm int) {
	ok := false

	for !ok && !rf.killed() && rf.getCurrentState() == leader {
		logEntries := rf.getLogEntries()
		nextIndex := rf.getNextIndexFor(server_idx)
		prevIndex := nextIndex - 1
		prevTerm := 0
		if prevIndex != -1 {
			prevTerm = logEntries[prevIndex].Term
		}
		commitIndex := rf.getCommitIndex()
		args := AppendEntriesArgs{
			AppendEntriesTermNumber: currentTerm,
			LeaderId:                rf.me,
			PrevLogIndex:            prevIndex,
			PrevLogTerm:             prevTerm,
			Entries:                 logEntries[nextIndex:],
			LeaderCommit:            commitIndex,
		}
		reply := AppendEntriesReply{}
		ok = rf.sendAppendEntries(server_idx, &args, &reply)
		if ok {
			if rf.getCurrentTermNumber() > currentTerm {
				rf.logMsg(APPLOGREPL, "My term is greater than the term I started this RPC with - returning")
				return
			}
			if reply.ReplyEntriesTermNumber > currentTerm {
				rf.logMsg(APPLOGREPL, fmt.Sprintf("Follower has a higher term - returning to follower state!"))
				rf.setCurrentState(follower)
				return
			}
			if reply.Success {
				rf.logMsg(APPLOGREPL, fmt.Sprintf("Got a success reply! Updating %v's nextIndex from %v to %v", server_idx, rf.getNextIndexFor(server_idx), len(logEntries)))
				rf.setNextIndexFor(server_idx, len(logEntries))
				rf.setMatchIndexFor(server_idx, len(logEntries)-1)
			} else {
				rf.decrementNextIndexFor(server_idx)
				rf.logMsg(APPLOGREPL, fmt.Sprintf("Got a non-success reply from %v, so decremented their nextIndex to %v", server_idx, rf.getNextIndexFor(server_idx)))
				ok = false
			}
		}
	}

	rf.mu.Lock()
	maxIndex := 0
	for _, matchIndex := range rf.matchIndex {
		if matchIndex > maxIndex {
			maxIndex = matchIndex
		}
	}

	for N := rf.commitIndex + 1; N <= maxIndex; N++ {
		numServers := 0
		for _, matchIndex := range rf.matchIndex {
			if matchIndex >= N {
				numServers += 1
			}
		}
		if numServers > len(rf.peers)/2 && rf.logEntries[N].Term == rf.currentTerm.number {
			go rf.logMsg(APPLOGREQ, fmt.Sprintf("Found N as %v! Updating commitIndex", N))
			go rf.setCommitIndex(N)
		}
	}
	rf.mu.Unlock()

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
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm.number,
	}
	rf.logEntries = append(rf.logEntries, logEntry) // this will be sent to followers in the next HB
	rf.mu.Unlock()
	rf.logMsg(LOGENTRIES, fmt.Sprintf("Appended new entry to self. New logEntries is %v", rf.getLogEntries()))

	return index + 1, currentTerm, true // the tests assume logs are 1-indexed
}
