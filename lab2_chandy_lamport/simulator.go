package chandy_lamport

import (
	"log"
	"math/rand"
	"sync"
)

type snapshotCompletion struct {
	completed map[string]bool
	done      chan struct{}
	closed    bool
}

// Max random delay added to packet delivery
const maxDelay = 5

// Simulator is the entry point to the distributed snapshot application.
//
// It is a discrete time simulator, i.e. events that happen at time t + 1 come
// strictly after events that happen at time t. At each time step, the simulator
// examines messages queued up across all the links in the system and decides
// which ones to deliver to the destination.
//
// The simulator is responsible for starting the snapshot process, inducing servers
// to pass tokens to each other, and collecting the snapshot state after the process
// has terminated.
type Simulator struct {
	rng            *rand.Rand
	time           int
	nextSnapshotId int
	servers        map[string]*Server // key = server ID
	logger         *Logger
	mu             sync.Mutex
	snapshots      map[int]*snapshotCompletion
}

func NewSimulator() *Simulator {
	return &Simulator{
		rand.New(rand.NewSource(8053172852482175524)),
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
		sync.Mutex{},
		make(map[int]*snapshotCompletion),
	}
}

// Return the receive time of a message after adding a random delay.
// Note: since we only deliver one message to a given server at each time step,
// the message may be received *after* the time step returned in this function.
func (sim *Simulator) GetReceiveTime() int {
	return sim.time + 1 + sim.rng.Intn(5)
}

// Add a server to this simulator with the specified number of starting tokens
func (sim *Simulator) AddServer(id string, tokens int) {
	server := NewServer(id, tokens, sim)
	sim.servers[id] = server
}

// Add a unidirectional link between two servers
func (sim *Simulator) AddForwardLink(src string, dest string) {
	server1, ok1 := sim.servers[src]
	server2, ok2 := sim.servers[dest]
	if !ok1 {
		log.Fatalf("Server %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Server %v does not exist\n", dest)
	}
	server1.AddOutboundLink(server2)
}

// Run an event in the system
func (sim *Simulator) InjectEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.servers[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.serverId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step, handling all send message events
// that expire at the new time step, if any.
func (sim *Simulator) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the servers,
	// we must also iterate through the servers and the links in a deterministic way
	for _, serverId := range getSortedKeys(sim.servers) {
		server := sim.servers[serverId]
		for _, dest := range getSortedKeys(server.outboundLinks) {
			link := server.outboundLinks[dest]
			// Deliver at most one packet per server at each time step to
			// establish total ordering of packet delivery to each server
			if !link.events.Empty() {
				e := link.events.Peek().(SendMessageEvent)
				if e.receiveTime <= sim.time {
					link.events.Pop()
					sim.logger.RecordEvent(
						sim.servers[e.dest],
						ReceivedMessageEvent{e.src, e.dest, e.message})
					sim.servers[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Start a new snapshot process at the specified server
func (sim *Simulator) StartSnapshot(serverId string) {
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++

	sim.mu.Lock()
	sim.snapshots[snapshotId] = &snapshotCompletion{
		completed: make(map[string]bool),
		done:      make(chan struct{}),
		closed:    false,
	}
	sim.mu.Unlock()

	sim.logger.RecordEvent(sim.servers[serverId], StartSnapshot{serverId, snapshotId})
	sim.servers[serverId].StartSnapshot(snapshotId)
}

// Callback for servers to notify the simulator that the snapshot process has
// completed on a particular server
func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})

	sim.mu.Lock()
	defer sim.mu.Unlock()

	snapshot, ok := sim.snapshots[snapshotId]
	if !ok {
		log.Fatalf("Unknown snapshot ID %v completed by server %v\n", snapshotId, serverId)
	}
	if snapshot.completed[serverId] {
		return
	}
	snapshot.completed[serverId] = true
	if len(snapshot.completed) == len(sim.servers) && !snapshot.closed {
		snapshot.closed = true
		close(snapshot.done)
	}
}

// Collect and merge snapshot state from all the servers.
// This function blocks until the snapshot process has completed on all servers.
func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	sim.mu.Lock()
	snapshot, ok := sim.snapshots[snapshotId]
	sim.mu.Unlock()
	if !ok {
		log.Fatalf("Attempted to collect unknown snapshot ID %v\n", snapshotId)
	}

	<-snapshot.done

	snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}
	for _, serverId := range getSortedKeys(sim.servers) {
		serverSnapshot := sim.servers[serverId].getSnapshot(snapshotId)
		snap.tokens[serverId] = serverSnapshot.tokens
		snap.messages = append(snap.messages, serverSnapshot.messages...)
	}
	return &snap
}
