package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// getNewElectionTimeout is called to calculate a new, random timeout value.
// This happens at when this Raft server boots up and at the start of every election.
func generateNewElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	min := HB_WAIT_MIN
	max := HB_WAIT_MAX
	milliseconds := min + rand.Intn(max-min)
	return time.Millisecond * time.Duration(milliseconds)
}

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
			rf.upsertLogs(0, args.Entries)
			success = true
		} else {
			// first check if prevIndex is valid
			if prevIndex >= len(logEntries) {
				success = false
			} else {
				// finally, we can compare the terms of the 2 prev indices
				if logEntries[prevIndex].Term != args.PrevLogIndex {
					success = false
				} else {
					rf.upsertLogs(prevIndex+1, args.Entries)
					success = true
				}
			}
		}

		if args.LeaderCommit > rf.getCommitIndex() {
			rf.setCommitIndex(min(args.LeaderCommit, rf.getLogLength()-1))
		}
	}

	*reply = AppendEntriesReply{
		ReplyEntriesTermNumber: currentTerm,
		Success:                success,
		Id:                     rf.me,
		LogLength:              rf.getLogLength(),
	}
}

func (rf *Raft) upsertLogs(startingIndex int, leaderLogs []LogEntry) {
	rf.mu.Lock()
	for _, logEntry := range leaderLogs {
		if startingIndex >= len(rf.logEntries) {
			rf.logEntries = append(rf.logEntries, logEntry)
		} else {
			rf.logEntries[startingIndex] = logEntry
		}
		startingIndex++
	}
	defer rf.mu.Unlock()
}

func (rf *Raft) leader() {
	rf.setCurrentState(leader)
	rf.initNextIndex()
	rf.logMsg("Starting my reign as a leader!", LEAD)
	currentTerm := rf.getCurrentTermNumber()
	for !rf.killed() && rf.getCurrentState() == leader {
		logEntries := rf.getLogEntries()
		commitIndex := rf.getCommitIndex()
		for server_idx := range rf.peers {
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
			go rf.sendLogEntries(server_idx, appendEntriesArgs, appendEntriesReply)
		}
		time.Sleep(time.Millisecond * time.Duration(HB_INTERVAL))
		rf.logMsg("Sent HBs", LEAD)
	}
	rf.logMsg("Stepping down from being a leader", LEAD)
}

func (rf *Raft) sendLogEntries(server_idx int, args AppendEntriesArgs, reply AppendEntriesReply) {
	ok := false
	for !ok && !rf.killed() && rf.getCurrentState() == leader {
		ok = rf.sendAppendEntries(server_idx, &args, &reply)
		if ok {
			if reply.ReplyEntriesTermNumber > rf.getCurrentTermNumber() {
				rf.setCurrentState(follower)
				return
			}
			if reply.Success {
				rf.setNextIndexFor(reply.Id, reply.LogLength)
			} else {
				rf.decrementNextIndexFor(reply.Id)
			}
		}
	}
}

func (rf *Raft) candidate() {
	newTerm := generateNewTerm(rf.getCurrentTermNumber()+1, candidate, generateNewElectionTimeout())
	rf.setTerm(newTerm) // set this as the new term (also sets state to candidate)
	currentTermNumber := rf.getCurrentTermNumber()
	rf.logMsg("Started new term as a candidate!", ELCTN)
	rf.setVotedFor(rf.me) // vote for itself
	votes := 1

	reqVotesArgs := RequestVoteArgs{
		ReqVotesTermNumber: currentTermNumber,
		CandidateId:        rf.me,
	}

	votesChan := make(chan struct {
		RequestVoteReply
		bool
	})
	for server_idx := range rf.peers {
		if server_idx != rf.me {
			go func(server_idx int) {
				replyVotesArgs := RequestVoteReply{}
				rf.logMsg(fmt.Sprintf("Requesting vote from %v", server_idx), ELCTN)
				ok := rf.sendRequestVote(server_idx, &reqVotesArgs, &replyVotesArgs)
				votesChan <- struct {
					RequestVoteReply
					bool
				}{replyVotesArgs, ok}
			}(server_idx)
		}
	}
	for pair := range votesChan {
		// check if this election is still valid.
		if rf.getCurrentTermNumber() > currentTermNumber {
			rf.logMsg("Running an expired election, cancelling it...", ELCTN)
			return
		}
		replyVotesArgs := pair.RequestVoteReply
		ok := pair.bool

		if ok {
			rf.logMsg(fmt.Sprintf("Got rpc reply with term %v and vote %v.", replyVotesArgs.ReplyVotesTermNumber, replyVotesArgs.VoteGranted), ELCTN)
			if replyVotesArgs.ReplyVotesTermNumber > rf.getCurrentTermNumber() {
				rf.logMsg("A new term has already started! Cancelling my election.", ELCTN)
				return
			}
			if replyVotesArgs.ReplyVotesTermNumber == rf.getCurrentTermNumber() {
				voteGranted := replyVotesArgs.VoteGranted
				if voteGranted {
					votes++
				}
				if votes > len(rf.peers)/2 {
					rf.logMsg(fmt.Sprintf("got majority (%v out of % v votes) so becoming leader\n", votes, len(rf.peers)), ELCTN)
					go rf.leader()
					return // early exit this election
				}
			}
		}
	}

	// if we reached here, we didn't become a leader
	rf.logMsg(fmt.Sprintf("Received %v out of %v votes, so I didn't get elected.", votes, len(rf.peers)), ELCTN)
}

// This routine runs forever.
// If a heartbeat is not received, it changes the state to a candidate.
// Multiple instances of it should never be started.
func (rf *Raft) tickr() {
	for !rf.killed() {
		timeout := rf.getElectionTimeout()
		rf.logMsg(fmt.Sprintf("Sleeping for %v ms", timeout), TIMER)
		time.Sleep(timeout)

		recvdHb := rf.getReceivedHeartBeat()
		if recvdHb {
			rf.setReceivedHeartBeat(false)
		} else {
			rf.logMsg("Did not receive a heartbeat! Starting election...", TIMER)
			go rf.candidate()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	termNumber := rf.getCurrentTermNumber()
	currentState := rf.getCurrentState()
	return termNumber, currentState == leader
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.rpcLock.Lock()
	defer rf.rpcLock.Unlock()

	currentTerm := rf.getCurrentTermNumber()
	if args.ReqVotesTermNumber < currentTerm {
		rf.logMsg(fmt.Sprintf("term number %v is lesser than currentTerm %v, so not voting for %v!", args.ReqVotesTermNumber, currentTerm, args.CandidateId), VOTE)
		reply.ReplyVotesTermNumber = currentTerm
		reply.VoteGranted = false
		return
	}
	if args.ReqVotesTermNumber > currentTerm {
		term := generateNewTerm(args.ReqVotesTermNumber, follower, generateNewElectionTimeout())
		rf.setTerm(term)
		rf.logMsg("Changed my term before voting!", VOTE)
	}
	votedFor := rf.getVotedFor()
	if votedFor == -1 {
		rf.logMsg(fmt.Sprintf("voting yes for %v!", args.CandidateId), VOTE)
		reply.ReplyVotesTermNumber = rf.getCurrentTermNumber()
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateId)
		rf.setReceivedHeartBeat(true) // when we vote yes, give the server some time to send HBs.
	} else {
		rf.logMsg(fmt.Sprintf("Already voted for %v in current term, so not voting for %v", votedFor, args.CandidateId), VOTE)
		reply.ReplyVotesTermNumber = rf.getCurrentTermNumber()
		reply.VoteGranted = false
	}
}

//
// Send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Send an AppenEntries RPC to a server.
// server is the index of the target server in rf.peers[].
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
