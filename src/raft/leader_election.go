package raft

import (
	"math/rand"
	"time"
)

// getNewElectionTimeout is called to calculate a new, random timeout value.
// This happens at when this Raft server boots up and at the start of every election.
func generateNewElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	min := HB_INTERVAL + HB_WAIT_MIN
	max := HB_INTERVAL + HB_WAIT_MAX
	milliseconds := min + rand.Intn(max-min)
	return time.Millisecond * time.Duration(milliseconds)
}

func (rf *Raft) leader() {
	if rf.getCurrentState() == leader {
		return
	}
	rf.setCurrentState(leader)
	for rf.getCurrentState() == leader {
		time.Sleep(time.Millisecond * time.Duration(HB_INTERVAL))

	}
}

func (rf *Raft) candidate() {
	if rf.getCurrentState() == candidate {
		return
	}
	newTerm := rf.CreateNewTerm(candidate, generateNewElectionTimeout())
	currentTermNumber := rf.getCurrentTermNumber()
	rf.setTerm(newTerm)   // set this as the new term (also sets state to candidate)
	rf.setVotedFor(rf.me) // vote for itself
	votes := 1

	reqVotesArgs := RequestVoteArgs{
		term:        currentTermNumber,
		candidateId: rf.me,
	}
	replyVotesArgs := RequestVoteReply{}

	for server_idx := range rf.peers {
		ok := rf.sendRequestVote(server_idx, &reqVotesArgs, &replyVotesArgs)
		if ok {
			voteGranted := replyVotesArgs.voteGranted
			if voteGranted {
				votes++
			}
			if replyVotesArgs.term > currentTermNumber {
				go rf.follower() // a new term has already started
				return
			}
		}
	}

	// election went well, let's check the results
	elected := votes >= len(rf.peers)/2
	if elected {
		go rf.leader()
	} else {
		go rf.follower()
	}
}

// This go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) follower() {
	if rf.getCurrentState() == follower { // avoid creating multiple followers on this server. That will be disastrous!
		return
	}
	rf.setCurrentState(follower)
	for rf.getCurrentState() == follower {
		timeout := rf.getElectionTimeout()

		time.Sleep(timeout)

		recvdHb := rf.getReceivedHeartBeat()
		if recvdHb {
			rf.setReceivedHeartBeat(false)
		} else {
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
	currentTerm := rf.getCurrentTermNumber()
	votedFor := rf.getVotedFor()
	if args.term < currentTerm || votedFor == -1 {
		reply.term = currentTerm
		reply.voteGranted = false
	} else {
		reply.term = currentTerm
		reply.voteGranted = true
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
