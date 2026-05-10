Overview
In this project you will implement the Chandy-Lamport algorithm for computing the global state of a distributed system. Unlike Projects 1.1 and 1.2, where you designed the coordination logic yourself, here you are implementing a precisely specified protocol from the research literature. The challenge is not inventing the design — it is understanding it deeply enough to implement it correctly under concurrency.

The global-state recording algorithm runs concurrently with the underlying computation without altering it. Your implementation will run on top of a token-passing system using a discrete-time simulator.

Where this fits in the course The Chandy-Lamport paper was covered in the Apr 16 lecture on Global State. The Apr 14 lecture on Logical Clocks provides essential background — you need to understand why global state cannot simply be read off independently from each process before you can appreciate what the algorithm is doing. Read both before starting the design document.

This project is almost pure protocol reasoning. Performance is not the point. The design document carries the most weight of any project so far — getting the protocol right on paper before you implement it is the intended workflow.

Algorithm Summary
Starting a snapshot on a server

Record local state
Send marker messages on all outbound channels
On receiving a marker message

If snapshot not yet started: record local state and send markers on all other outbound channels
Begin recording messages received on all other channels
Stop recording messages on this channel
Termination: when all servers have received markers on all interfaces

Key invariant: total token count must be preserved across any snapshot

Protocol assumptions

No failures; all messages arrive intact and exactly once
Channels are unidirectional and FIFO ordered
There is a communication path between any two processes
Any process may initiate the snapshot
The algorithm does not interfere with normal process execution
System Architecture
The implementation uses a discrete-time simulator to order events. Understanding how the simulator interacts with servers is essential before you start implementing.

Figure 1 — Overall system 

The simulator reads a topology file and an event file. It injects events into the system via InjectEvents() and coordinates with the servers. When a snapshot completes, the simulator outputs the global snapshot.

Figure 2 — Simulator-server interaction 

The simulator initiates snapshots by calling StartSnapshot(server_id) on the target server. Servers signal completion back to the simulator via NotifySnapshotComplete(server_id, snap_id). The simulator then collects the result via CollectSnapshot(snap_id).

Figure 3 — Per-tick server execution 

On each simulator tick, every server processes incoming messages via HandlePacket(msg), which may trigger StartSnapshot(snap_id). Servers communicate with each other via SendTokens(). This tick-driven model is what makes the system discrete-time — all events are ordered by the simulator clock.

Project Setup
Download the starter code: chandy_lamport.tgzDownload chandy_lamport.tgz
Extract it: tar -zxvf chandy_lamport.tgz
You only need to modify server.go and simulator.go — do not modify other files
Codebase overview

File	Role
server.go	A process in the distributed algorithm
simulator.go	Discrete-time simulator managing events
logger.go	Records events for debugging
common.go	Debug flag and shared message types
snapshot_test.go	Tests you need to pass
syncmap.go	Thread-safe map implementation
queue.go	Queue interface
test_common.go	Topology and event parsing helpers
test_data/	Test inputs and expected results
Suggested Approach
Start by understanding how the simulator builds a topology and injects events — read test_common.go and snapshot_test.go
Study the simulator (simulator.go) and how it interacts with server modules (Figure 2)
Study the server (server.go) and its snapshot and communication functions (Figure 3)
Start simple: implement single snapshots first (Phase 1), then extend to concurrent snapshots (Phase 2)
Test regularly — pass the 2-node case before moving to 3 nodes, then 8 nodes
Before You Start
This project includes three deliverables: a design document, an implementation, and a short experiment. The design document carries 45% of the grade — complete it before writing code. The template is on the next page.