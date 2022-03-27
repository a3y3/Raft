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
	min := HB_INTERVAL + HB_WAIT_MIN
	max := HB_INTERVAL + HB_WAIT_MAX
	milliseconds := min + rand.Intn(max-min)
	return time.Millisecond * time.Duration(milliseconds)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	currentTerm := rf.getCurrentTermNumber()
	if args.TermNumber >= currentTerm {
		if args.TermNumber > currentTerm {
			term := rf.GetNewTerm(follower, generateNewElectionTimeout())
			rf.setTerm(term)
			go rf.follower()
		}
		rf.setReceivedHeartBeat(true)
	}
}

func (rf *Raft) leader() {
	if rf.getCurrentState() == leader {
		return
	}
	rf.setCurrentState(leader)
	for rf.getCurrentState() == leader {
		appendEntriesArgs := AppendEntriesArgs{
			TermNumber: rf.getCurrentTermNumber(),
			LeaderId:   rf.me,
		}
		appendEntriesReply := AppendEntriesReply{}
		for server_idx := range rf.peers {
			if server_idx != rf.me {
				rf.sendAppendEntries(server_idx, &appendEntriesArgs, &appendEntriesReply)
			}
		}
		fmt.Printf("(%v) leader: sleeping for %v\n", rf.me, time.Millisecond*time.Duration(HB_INTERVAL))
		time.Sleep(time.Millisecond * time.Duration(HB_INTERVAL))
	}
}

func (rf *Raft) candidate() {
	if rf.getCurrentState() == candidate {
		return
	}
	newTerm := rf.GetNewTerm(candidate, generateNewElectionTimeout())
	rf.setTerm(newTerm) // set this as the new term (also sets state to candidate)
	currentTermNumber := rf.getCurrentTermNumber()
	fmt.Printf("(%v) Started new term %v", rf.me, currentTermNumber)
	rf.setVotedFor(rf.me) // vote for itself
	votes := 1

	reqVotesArgs := RequestVoteArgs{
		TermNumber:  currentTermNumber,
		CandidateId: rf.me,
	}
	replyVotesArgs := RequestVoteReply{}

	for server_idx := range rf.peers {
		ok := rf.sendRequestVote(server_idx, &reqVotesArgs, &replyVotesArgs)
		if ok {
			voteGranted := replyVotesArgs.VoteGranted
			if voteGranted {
				votes++
			}
			if replyVotesArgs.TermNumber > currentTermNumber {
				fmt.Printf("(%v) A new term has already started! Cancelling my election and becoming a follower\n", rf.me)
				go rf.follower() // a new term has already started
				return
			}
		}
	}

	// election went well, let's check the results
	elected := votes >= len(rf.peers)/2
	fmt.Printf("(%v) got %v votes, so elected is %v\n", rf.me, votes, elected)
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
		fmt.Printf("(%v) follower: sleeping for %v\n", rf.me, timeout)
		time.Sleep(timeout)

		recvdHb := rf.getReceivedHeartBeat()
		if recvdHb {
			rf.setReceivedHeartBeat(false)
		} else {
			fmt.Printf("(%v) Did not receive a heartbeat! Starting election...\n", rf.me)
			go rf.candidate()
			return
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
	if args.TermNumber < currentTerm || votedFor == -1 {
		reply.TermNumber = currentTerm
		reply.VoteGranted = false
	} else {
		reply.TermNumber = currentTerm
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateId)
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
