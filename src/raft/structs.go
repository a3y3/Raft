package raft

import (
	"sync"
	"time"

	"6.824/labrpc"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex // Lock to protect shared access to this peer's state
	rpcLock           sync.Mutex
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	currentTerm       Term                // current Term of the Raft peer.
	logEntries        []logEntry
	receivedHeartbeat bool
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm.votedFor
}

func (rf *Raft) setVotedFor(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm.votedFor = index
}

func (rf *Raft) getCurrentTermNumber() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm.number
}

func (rf *Raft) getCurrentState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm.state
}

func (rf *Raft) setCurrentState(state State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm.state = state
}

func (rf *Raft) getElectionTimeout() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm.electionTimeout
}

func (rf *Raft) setElectionTimeout(newTimeout time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm.electionTimeout = newTimeout
}

func (rf *Raft) getReceivedHeartBeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.receivedHeartbeat
}

func (rf *Raft) setReceivedHeartBeat(value bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.receivedHeartbeat = value
}

type Term struct {
	number          int
	votedFor        int           // candidateId that this server voted for in this term
	electionTimeout time.Duration // the timeout for this term. Reset every election term.
	state           State         // state enum {follower/candidate/leader} the server was in for this term.
}

func (rf *Raft) setTerm(term Term) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) GetNewTerm(newState State, electionTimeout time.Duration) Term {
	return Term{
		number:          rf.getCurrentTermNumber() + 1,
		votedFor:        -1,
		electionTimeout: electionTimeout,
		state:           newState,
	}
}

type logEntry struct {
	log string // log message
}

type State int

const (
	undefined State = iota
	follower
	candidate
	leader
)

func (s State) String() string {
	switch s {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	}
	return "Unknown state!"
}

const (
	// all times in milliseconds
	HB_INTERVAL int = 200 // send a heartbeat per this time
	HB_WAIT_MIN int = 300 // allow this much time at least for HB
	HB_WAIT_MAX int = 600 // allow this much time at most for HB
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type AppendEntriesArgs struct {
	AppendEntriesTermNumber int
	LeaderId                int
}

type AppendEntriesReply struct {
	ReplyEntriesTermNumber int
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	ReqVotesTermNumber int
	CandidateId        int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	ReplyVotesTermNumber int
	VoteGranted          bool
}
