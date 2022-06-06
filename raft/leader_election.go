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

// leader represents a server that successfully won an election for a term.
// Sends AppendEntries on a fixed interval, and exits once it's no longer the leader.
func (rf *Raft) leader() {
	rf.setCurrentState(leader)
	rf.initNextIndex()
	rf.logMsg(LEADER, "Starting my reign as a leader!")
	currentTerm := rf.getCurrentTermNumber()
	for !rf.killed() && rf.getCurrentState() == leader {
		rf.setReceivedHeartBeat(true)
		rf.logMsg(LEADER, "Sending HBs")
		for server_idx := range rf.peers {
			if server_idx != rf.me {
				go rf.sendLogEntries(server_idx, currentTerm)
			}
		}
		rf.logMsg(LEADER, "Sent HBs")
		time.Sleep(time.Millisecond * time.Duration(HB_INTERVAL))
	}
	rf.logMsg(LEADER, "Stepping down from being a leader")
}

// candidate represents a server that has started a new election.
// Votes for itself, then requests votes from other servers for a specific term.
// Exits once it has either become a new leader, has lost an election, or has timed out waiting for votes.
func (rf *Raft) candidate() {
	newTerm := generateNewTerm(rf.getCurrentTermNumber()+1, candidate, generateNewElectionTimeout())
	rf.setTerm(newTerm) // set this as the new term (also sets state to candidate)
	currentTermNumber := rf.getCurrentTermNumber()
	rf.logMsg(ELECTION, "Started new term as a candidate!")
	rf.setVotedFor(rf.me) // vote for itself
	votes := 1
	offset := rf.getOffset()
	logEntries := rf.getLogEntries()
	lastLogIndex := rf.getLogLength() - 1
	lastLogTerm := -1
	if lastLogIndex != -1 {
		if lastLogIndex-offset == -1 {
			lastLogTerm = rf.getSnapshotTermNumber()
		} else {
			lastLogTerm = logEntries[lastLogIndex-offset].Term
		}
	}

	reqVotesArgs := RequestVoteArgs{
		ReqVotesTermNumber: currentTermNumber,
		CandidateId:        rf.me,
		LastLogIndex:       lastLogIndex,
		LastLogTerm:        lastLogTerm,
	}

	votesChan := make(chan struct {
		RequestVoteReply
		bool
	})
	for server_idx := range rf.peers {
		if server_idx != rf.me {
			go func(server_idx int) {
				replyVotesArgs := RequestVoteReply{}
				rf.logMsg(ELECTION, fmt.Sprintf("Requesting vote from %v", server_idx))
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
			rf.logMsg(ELECTION, "Running an expired election, cancelling it...")
			return
		}
		replyVotesArgs := pair.RequestVoteReply
		ok := pair.bool

		if ok {
			rf.logMsg(ELECTION, fmt.Sprintf("Got rpc reply with term %v and vote %v.", replyVotesArgs.ReplyVotesTermNumber, replyVotesArgs.VoteGranted))
			if replyVotesArgs.ReplyVotesTermNumber > rf.getCurrentTermNumber() {
				rf.logMsg(ELECTION, "A new term has already started! Cancelling my election.")
				return
			}
			if replyVotesArgs.ReplyVotesTermNumber == rf.getCurrentTermNumber() {
				voteGranted := replyVotesArgs.VoteGranted
				if voteGranted {
					votes++
				}
				if votes > len(rf.peers)/2 {
					rf.logMsg(ELECTION, fmt.Sprintf("got majority (%v out of % v votes) so becoming leader\n", votes, len(rf.peers)))
					go rf.leader()
					return // early exit this election
				}
			}
		}
	}

	// if we reached here, we didn't become a leader
	rf.logMsg(ELECTION, fmt.Sprintf("Received %v out of %v votes, so I didn't get elected.", votes, len(rf.peers)))
}

// This routine runs forever.
// If a heartbeat is not received, it changes the state to a candidate.
// Multiple instances of it should never be started.
func (rf *Raft) tickr() {
	for !rf.killed() {
		timeout := rf.getElectionTimeout()
		rf.logMsg(TIMER, fmt.Sprintf("Sleeping for %v ms", timeout))
		time.Sleep(timeout)

		recvdHb := rf.getReceivedHeartBeat()
		if recvdHb {
			rf.setReceivedHeartBeat(false)
		} else {
			rf.logMsg(TIMER, "Did not receive a heartbeat! Starting election...")
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
// RequestVote RPC handler is handled by a server from whom another server is requesting a vote.
// Replies true if vote is granted, and false otherwise. See the Raft paper for details on when the server replies yes, or no.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.rpcLock.Lock()
	defer rf.rpcLock.Unlock()

	currentTerm := rf.getCurrentTermNumber()
	if args.ReqVotesTermNumber < currentTerm {
		rf.logMsg(VOTE, fmt.Sprintf("term number %v is lesser than currentTerm %v, so not voting for %v!", args.ReqVotesTermNumber, currentTerm, args.CandidateId))
		reply.ReplyVotesTermNumber = currentTerm
		reply.VoteGranted = false
		return
	}
	if args.ReqVotesTermNumber > currentTerm {
		term := generateNewTerm(args.ReqVotesTermNumber, follower, generateNewElectionTimeout())
		rf.setTerm(term)
		rf.logMsg(VOTE, "Changed my term before voting!")
	}
	votedFor := rf.getVotedFor()
	if votedFor == -1 && rf.isUpTLogDate(args.LastLogIndex, args.LastLogTerm) {
		rf.logMsg(VOTE, fmt.Sprintf("voting yes for %v!", args.CandidateId))
		reply.ReplyVotesTermNumber = rf.getCurrentTermNumber()
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateId)
		rf.setReceivedHeartBeat(true) // when we vote yes, give the server some time to send HBs.
	} else {
		if votedFor != -1 {
			rf.logMsg(VOTE, fmt.Sprintf("Already voted for %v in current term, so not voting for %v", votedFor, args.CandidateId))
		} else {
			rf.logMsg(VOTE, fmt.Sprintf("Candidate's log isn't up to date as mine, so voting no"))
		}
		reply.ReplyVotesTermNumber = rf.getCurrentTermNumber()
		reply.VoteGranted = false
	}
}

// returns true if the candidate's logs is atleast as up to date as this server's logs
func (rf *Raft) isUpTLogDate(candidateLastIndex int, candidateLastTerm int) bool {
	myEntries := rf.getLogEntries()
	offset := rf.getOffset()
	myLastIndex := len(myEntries) + offset - 1
	myLastTerm := -1
	if myLastIndex != -1 {
		if myLastIndex-offset == -1 {
			myLastTerm = rf.getSnapshotTermNumber()
		} else {
			myLastTerm = myEntries[myLastIndex-offset].Term
		}
	}
	if candidateLastTerm > myLastTerm {
		return true
	}
	if candidateLastTerm < myLastTerm {
		return false
	}
	// last terms are equal
	return candidateLastIndex >= myLastIndex
}

//
// Send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Send an AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
