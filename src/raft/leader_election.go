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
	currentTerm := rf.getCurrentTermNumber()
	if args.AppendEntriesTermNumber >= currentTerm {
		if args.AppendEntriesTermNumber > currentTerm {
			term := rf.GetNewTerm(follower, generateNewElectionTimeout())
			rf.setTerm(term)
			rf.currentTerm.number = args.AppendEntriesTermNumber
			rf.logMsg(fmt.Sprintf("Stepping down as leader and updating my term"))
			go rf.follower()
		}
		rf.setReceivedHeartBeat(true)
	}
	rf.rpcLock.Unlock()
}

func (rf *Raft) leader() {
	if rf.killed() || rf.getCurrentState() == leader {
		return
	}
	rf.setCurrentState(leader)
	for !rf.killed() && rf.getCurrentState() == leader {
		appendEntriesArgs := AppendEntriesArgs{
			AppendEntriesTermNumber: rf.getCurrentTermNumber(),
			LeaderId:                rf.me,
		}
		appendEntriesReply := AppendEntriesReply{}
		rf.logMsg("Sending hb to peers")
		for server_idx := range rf.peers {
			if server_idx != rf.me {
				rf.sendAppendEntries(server_idx, &appendEntriesArgs, &appendEntriesReply)
			}
		}
		rf.logMsg("Finished sending hb to peers")
		time.Sleep(time.Millisecond * time.Duration(HB_INTERVAL))
	}
}

func (rf *Raft) candidate() {
	if rf.killed() || rf.getCurrentState() == candidate {
		return
	}
	newTerm := rf.GetNewTerm(candidate, generateNewElectionTimeout())
	rf.setTerm(newTerm) // set this as the new term (also sets state to candidate)
	currentTermNumber := rf.getCurrentTermNumber()
	rf.logMsg("Started new term!")
	rf.setVotedFor(rf.me) // vote for itself
	votes := 1

	reqVotesArgs := RequestVoteArgs{
		ReqVotesTermNumber: currentTermNumber,
		CandidateId:        rf.me,
	}

	for server_idx := range rf.peers {
		if server_idx != rf.me {
			replyVotesArgs := RequestVoteReply{}
			rf.logMsg(fmt.Sprintf("Requesting vote from %v", server_idx))
			ok := rf.sendRequestVote(server_idx, &reqVotesArgs, &replyVotesArgs)
			if ok {
				rf.logMsg(fmt.Sprintf("Got rpc reply with term %v and vote %v. currentTerm variable is %v", replyVotesArgs.ReplyVotesTermNumber, replyVotesArgs.VoteGranted, currentTermNumber))
				if rf.getCurrentTermNumber() > currentTermNumber {
					rf.logMsg("Running an expired election, cancelling it...")
					return
				}
				if replyVotesArgs.ReplyVotesTermNumber >= rf.getCurrentTermNumber() {
					voteGranted := replyVotesArgs.VoteGranted
					if voteGranted {
						votes++
					}
					if replyVotesArgs.ReplyVotesTermNumber > rf.getCurrentTermNumber() {
						rf.logMsg("A new term has already started! Cancelling my election.")
						return
					}
				}
			}
		}
	}

	if rf.getCurrentTermNumber() > currentTermNumber {
		rf.logMsg("Exiting expired election!")
		return
	}

	// election went well, let's check the results
	elected := votes > len(rf.peers)/2
	rf.logMsg(fmt.Sprintf("got %v out of % v votes, so elected is %v\n", votes, len(rf.peers), elected))
	if elected {
		go rf.leader()
	} else {
		go rf.follower()
	}
}

// This go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) follower() {
	if rf.killed() || rf.getCurrentState() == follower { // avoid creating multiple followers on this server. That will be disastrous!
		return
	}
	rf.setCurrentState(follower)
	for !rf.killed() && rf.getCurrentState() == follower {
		timeout := rf.getElectionTimeout()
		rf.logMsg(fmt.Sprintf("Sleeping for %v ms", timeout))
		time.Sleep(timeout)

		recvdHb := rf.getReceivedHeartBeat()
		if recvdHb {
			rf.setReceivedHeartBeat(false)
		} else {
			rf.logMsg("Did not receive a heartbeat! Starting election...")
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
	rf.rpcLock.Lock()
	currentTerm := rf.getCurrentTermNumber()
	if args.ReqVotesTermNumber < currentTerm {
		rf.logMsg(fmt.Sprintf("term number %v is lesser than currentTerm %v, so not voting for %v!", args.ReqVotesTermNumber, currentTerm, args.CandidateId))
		reply.ReplyVotesTermNumber = currentTerm
		reply.VoteGranted = false
		return
	}
	if args.ReqVotesTermNumber > currentTerm {
		term := rf.GetNewTerm(follower, generateNewElectionTimeout())
		rf.setTerm(term)
		rf.logMsg("Changed my term before voting!")
	}
	votedFor := rf.getVotedFor()
	if votedFor == -1 {
		rf.logMsg(fmt.Sprintf("voting yes for %v!", args.CandidateId))
		reply.ReplyVotesTermNumber = rf.getCurrentTermNumber()
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateId)
	} else {
		rf.logMsg(fmt.Sprintf("Already voted for %v in current term, so not voting for %v", votedFor, args.CandidateId))
		reply.ReplyVotesTermNumber = rf.getCurrentTermNumber()
		reply.VoteGranted = false
	}

	rf.rpcLock.Unlock()
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
