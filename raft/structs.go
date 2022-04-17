package raft

import (
	"fmt"
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
	receivedHeartbeat bool
	applyCh           chan ApplyMsg
	logEntries        []LogEntry
	commitIndex       int // index of highest logEntry known to be commited
	lastApplied       int // index of highest logEntry applied to state machine

	nextIndex  []int // leader only - index of next entry for each follower
	matchIndex []int // leader only - index of highest entry known to be replicated for each follower
}

func (rf *Raft) setMatchIndexFor(server int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[server] = index
}

func (rf *Raft) getLastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

func (logEntry LogEntry) String() string {
	return fmt.Sprintf("(%v);", logEntry.Term)
}

func (rf *Raft) initNextIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logEntries)
	}
}

// Warning - Applies a commit without locks. Please make sure locks are acquired BEFORE calling this function.
func (rf *Raft) unsafeSetCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	for rf.commitIndex > rf.lastApplied {
		go rf.logMsg(APPLOGREQ, fmt.Sprintf("CommitIndex %v is higher than lastApplied %v, incrementing lastApplied by 1!", rf.commitIndex, rf.lastApplied))
		rf.lastApplied += 1
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied + 1, // the tests assume logs are 1-indexed
		}
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) getCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) getLogLength() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.logEntries)
}

func (rf *Raft) getLogEntries() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return append([]LogEntry{}, rf.logEntries...) // create a copy to avoid race
}

func (rf *Raft) decrementNextIndexFor(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = max(0, rf.nextIndex[server]-1)
}

func (rf *Raft) setNextIndexFor(server int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = index
}

func (rf *Raft) getNextIndexFor(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[server]
}

func (rf *Raft) setLeaderId(leaderId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm.LeaderId = leaderId
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm.VotedFor
}

func (rf *Raft) setVotedFor(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm.VotedFor == -1 {
		rf.currentTerm.VotedFor = index
		rf.unsafePersist()
	} else {
		// logMsg needs to be on a separate thread as logMsg acquires mu lock (which this thread already has). This is not ideal, but as this is the only instance of it (and it's an extremely rare case), it's okay.
		go rf.logMsg(VOTE, "Warning: Avoided a rare condition in which a server could vote for 2 different servers in the same term.")
	}
}

func (rf *Raft) getCurrentTermNumber() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm.Number
}

func (rf *Raft) getCurrentState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm.State
}

func (rf *Raft) setCurrentState(state State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm.State = state
}

func (rf *Raft) getElectionTimeout() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm.ElectionTimeout
}

func (rf *Raft) setElectionTimeout(newTimeout time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm.ElectionTimeout = newTimeout
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
	Number          int
	VotedFor        int           // candidateId that this server voted for in this term
	ElectionTimeout time.Duration // the timeout for this term. Reset every election term.
	State           State         // state enum {follower/candidate/leader} the server was in for this term.
	LeaderId        int           // id of the leader for this term.
}

func (rf *Raft) setTerm(term Term) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.unsafePersist()
}

func generateNewTerm(number int, state State, electionTimeout time.Duration) Term {
	return Term{
		Number:          number,
		VotedFor:        -1,
		ElectionTimeout: electionTimeout,
		State:           state,
	}
}

type LogEntry struct {
	Command interface{} // command for the state machine
	Term    int         // term when entry was first received by leader
}

type State int

const (
	follower State = iota
	candidate
	leader
)

type Topic string

const (
	TIMER    Topic = "TIMER"
	VOTE     Topic = "VOTE"
	LEAD     Topic = "LEAD"
	ELECTION Topic = "ELCTION"

	LOGENTRIES Topic = "LOGSTATUS"
	APPLOGREPL Topic = "APPLOGREPL"
	APPLOGREQ  Topic = "APPLOGREQ"
	UPSERTLOG  Topic = "UPSERTLOG"

	PERSIST Topic = "PERSIST"
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
	HB_INTERVAL int = 150 // send a heartbeat per this much time
	HB_WAIT_MIN int = 250 // allow this much time at least for HB
	HB_WAIT_MAX int = 500 // allow this much time at most for HB
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
	PrevLogIndex            int
	PrevLogTerm             int
	Entries                 []LogEntry
	LeaderCommit            int
}

type AppendEntriesReply struct {
	ReplyEntriesTermNumber int
	Success                bool
	XIndex                 int
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	ReqVotesTermNumber int
	CandidateId        int

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	ReplyVotesTermNumber int
	VoteGranted          bool
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
