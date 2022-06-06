# Raft
Raft is a *distributed consensus protocol* for creating a *replicated state machine*.

- **Distributed Consensus** - Making disparate servers agree on a single decision.
- **Replicated State Machine** - A state machine is a server that processes instructions/data in a specific, deterministic order. The idea of a replicated state machine is to make several servers behave as one single, collective state machine.  Of course, to name a few, the challenges  are handling network partitions, out of order message deliveries, and servers crashing at any time. The Raft protocol ensures that all instructions (called "logs") are executed on all servers in the exact same order, despite these challenges.

### How does Raft ensure this?
Raft separates the problem into the following 4 sub problems:
- Leader Election (see [leader_election.go](raft/leader_election.go)): By dividing time into logical "terms", a "leader" in each term can dictate what happens and in what order the other "follower" servers execute logs.
- Log Replication (see [log_replication.go](raft/log_replication.go)): By storing logs on each server with specific indices and terms, it's easy to decide in what order to execute the logs, and detect out of order message deliveries.
- Safety (see [persistance.go](raft/persistance.go)): By continously storing current state on persistent disk, fault recovery is basically loading a serialized state of data on boot.
-  Log Compaction (see [log_compaction.go](raft/log_compaction.go)): By periodically compressing logs into "snapshots", Raft logs are guaranteed to be small, thus saving on bandwidth.

### Build & Run
This repository is an implementation of Raft in Go. It will be used in the future to create a fault tolerant Key Value store.
#### Build
Install Go 1.18, then: 
```
cd raft
go build
```
#### Run
Since this is more of a library, there's nothing specifically you can do to run it. However, the tests provided in the repo simulate a faulty, unreliable network connection where packets (requests and replies!) may arrive out of order and servers can disconnect or crash at any time. 

`export VERBOSE=1` will turn on verbose logging so you can see what's going on. Then, 
1. `go test 2A` will test leader election
2. `go test 2B` will test log replication
3. `go test 2C` will test crash/fault tolerance
4. `go test 2D` will test log compaction.

### Acknowledgements
[MIT's course on Distributed Systems](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html) \[6.824\] provided the skeleton for this, with an absolutely ruthless testing framework. Props to them for making it free and accessible! 
