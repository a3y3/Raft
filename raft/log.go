package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rpcLock.Lock()
	defer rf.rpcLock.Unlock()

	currentTerm := rf.getCurrentTermNumber()
	success := false
	xIndex := 0

	if args.AppendEntriesTermNumber >= currentTerm {
		rf.setReceivedHeartBeat(true)
		rf.setLeaderId(args.LeaderId)
		if args.AppendEntriesTermNumber > currentTerm {
			term := generateNewTerm(args.AppendEntriesTermNumber, follower, generateNewElectionTimeout())
			rf.setTerm(term)
		}
		prevIndex := args.PrevLogIndex
		logEntries := rf.getLogEntries()
		offset := rf.getOffset()

		if prevIndex == -1 {
			// upsert without comparing
			rf.logMsg(APPLOGREQ, "Upserting without comparing as prevIndex is -1")
			rf.upsertLogs(0, args.Entries)
			success = true
		} else {
			// first check if prevIndex is valid
			if prevIndex >= len(logEntries)+offset {
				rf.logMsg(APPLOGREQ, fmt.Sprintf("prevIndex is too high (%v > %v). Replying false", prevIndex, len(logEntries)+offset))
				success = false
				xIndex = max(0, len(logEntries)+offset-1)
			} else {
				// finally, we can compare the terms of the 2 prev indices
				offset := rf.getOffset()
				if prevIndex-offset < 0 {
					success = false
					xIndex = offset
				} else if logEntries[prevIndex-offset].Term != args.PrevLogTerm {
					success = false
					prevTerm := logEntries[prevIndex-offset].Term
					i := prevIndex
					for i = prevIndex - offset; i-offset >= 0; i-- {
						term := logEntries[i].Term
						if term != prevTerm {
							break
						}
					}
					xIndex = i + 1
					rf.logMsg(APPLOGREQ, fmt.Sprintf("Term mismatch, replying false (%v != %v). prevIndex: %v, xIndex: %v", logEntries[prevIndex-offset].Term, args.PrevLogTerm, prevIndex, xIndex))
				} else {
					rf.logMsg(APPLOGREQ, "Upserting...")
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

	if success { // we modified some entries
		rf.mu.Lock()
		rf.unsafePersist()
		rf.mu.Unlock()
	}

	*reply = AppendEntriesReply{
		ReplyEntriesTermNumber: currentTerm,
		Success:                success,
		XIndex:                 xIndex,
	}
}

func (rf *Raft) upsertLogs(startingIndex int, leaderLogs []LogEntry) {
	rf.mu.Lock()
	startingIndex -= rf.log.Offset
	for _, logEntry := range leaderLogs {
		if startingIndex >= rf.log.Offset+len(rf.log.Entries) {
			rf.log.Entries = append(rf.log.Entries, logEntry)
		} else {
			if rf.log.Entries[startingIndex].Term != logEntry.Term {
				//Fig 2. AppendEntries.3
				rf.log.Entries = rf.log.Entries[:startingIndex]   // Trim everything including this entry
				rf.log.Entries = append(rf.log.Entries, logEntry) // append the new log
			} else {
				rf.log.Entries[startingIndex] = logEntry
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
		offset := rf.getOffset()
		nextIndex := rf.getNextIndexFor(server_idx)
		prevIndex := nextIndex - 1
		prevTerm := 0
		if prevIndex != -1 {
			if prevIndex-offset < 0 {
				time.Sleep(time.Millisecond * 10) // InstallSnapshot()
				continue
			} else {
				prevTerm = logEntries[prevIndex-offset].Term
			}
		}
		commitIndex := rf.getCommitIndex()
		args := AppendEntriesArgs{
			AppendEntriesTermNumber: currentTerm,
			LeaderId:                rf.me,
			PrevLogIndex:            prevIndex,
			PrevLogTerm:             prevTerm,
			Entries:                 logEntries[nextIndex-offset:],
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
				rf.logMsg(APPLOGREPL, fmt.Sprintf("Got a success reply! Updating %v's nextIndex from %v to %v", server_idx, rf.getNextIndexFor(server_idx), len(logEntries)+offset))
				rf.setNextIndexFor(server_idx, len(logEntries)+offset)
				rf.setMatchIndexFor(server_idx, len(logEntries)+offset-1)
			} else {
				rf.setNextIndexFor(server_idx, reply.XIndex)
				rf.logMsg(APPLOGREPL, fmt.Sprintf("Got a non-success reply from %v, so decremented their nextIndex to %v", server_idx, rf.getNextIndexFor(server_idx)))
				ok = false
			}
		}
	}

	higherCommitIndex := -1
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
		if numServers > len(rf.peers)/2 && rf.log.Entries[N-rf.log.Offset].Term == rf.currentTerm.Number {
			higherCommitIndex = N
		}
	}
	rf.mu.Unlock()
	if higherCommitIndex != -1 {
		rf.logMsg(APPLOGREQ, fmt.Sprintf("Found a higherCommitIndex as %v! Updating commitIndex", higherCommitIndex))
		rf.setCommitIndex(higherCommitIndex)
	}

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
	index := len(rf.log.Entries) + rf.log.Offset
	currentTerm := rf.currentTerm.Number
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm.Number,
	}
	rf.log.Entries = append(rf.log.Entries, logEntry)
	rf.matchIndex[rf.me] = len(rf.log.Entries) + rf.log.Offset - 1
	rf.unsafePersist()
	rf.mu.Unlock()

	rf.logMsg(LOGENTRIES, fmt.Sprintf("Appended new entry to self. New logEntries is %v", rf.getLogEntries()))

	for server_idx := range rf.peers {
		if server_idx != rf.me {
			go rf.sendLogEntries(server_idx, currentTerm)
		}
	}

	return index + 1, currentTerm, true // the tests assume logs are 1-indexed
}
