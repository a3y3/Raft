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
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	state       State               // state enum {follower/candidate/leader}
	currentTerm Term                // current Term of the Raft peer.
	logEntries  []logEntry
}

type Term struct {
	number            int
	votedFor          int           // candidateId that this server voted for in this term
	electionTimeout   time.Duration // the timeout for this term. Reset every election term.
	receivedHeartbeat bool
}

type logEntry struct {
	log string // log message
}

type State int

const (
	follower State = iota
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
	HB_INTERVAL int = 1000 // ms
	HB_WAIT_MIN int = 200  // allow this much time at least for HB; ms
	HB_WAIT_MAX int = 400  // allow this much time at most for HB; ms
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

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
}
