package raft

import (
	"fmt"
	"iter"
	"net"
	"testing"
	"time"
)

type Harness struct {
	transports  map[ServerId]*HttpTransport
	raftServers map[ServerId]*RaftServer
	stateDirs   map[ServerId]string
	addrs       map[ServerId]net.Addr
	serverIds   []ServerId
}

// NewHarness creates a number of raft servers with HTTP transports,
// starting at server ID 1.
func NewHarness(t *testing.T, n int) *Harness {
	t.Helper()
	serverIds := []ServerId{}
	for id := range n {
		serverIds = append(serverIds, ServerId(id+1)) // start IDs at 1
	}
	transports := map[ServerId]*HttpTransport{}
	raftServers := map[ServerId]*RaftServer{}
	stateDirs := map[ServerId]string{}
	addrs := map[ServerId]net.Addr{}
	for _, id := range serverIds {
		transport, err := NewHttpTransport()
		if err != nil {
			t.Fatalf("NewHttpTransport failed: %v", err)
		}
		stateDir := t.TempDir()
		raftSrv, err := NewRaftServer(id, filter(serverIds, id), stateDir, &NoopStateMachine{})
		if err != nil {
			t.Fatalf("NewRaftServer failed: %v", err)
		}
		transport.Host(raftSrv)

		transports[id] = transport
		raftServers[id] = raftSrv
		stateDirs[id] = stateDir
		addrs[id] = transport.Addr()
	}
	// Tell each transport about the others' addresses
	for _, transport := range transports {
		transport.peerAddrs = addrs
	}
	return &Harness{
		transports:  transports,
		raftServers: raftServers,
		stateDirs:   stateDirs,
		addrs:       addrs,
		serverIds:   serverIds,
	}
}

func (h *Harness) StartAll() {
	// Start everything up
	for _, transport := range h.transports {
		go transport.Serve()
	}
	for _, raft := range h.raftServers {
		go raft.Start()
	}
}

func (h *Harness) ShutdownAll() {
	for _, transport := range h.transports {
		transport.Shutdown()
	}
	for _, raft := range h.raftServers {
		raft.Shutdown()
	}
}

// WaitForOneLeader waits for there to be a stable cluster,
// 1 leader and 0 candidates.
func (h *Harness) WaitForOneLeader(t *testing.T) {
	// Wait up to 2,000 ms (should be enough for 300ms election timeout max)
	const sleepLen = 5 * time.Millisecond
	const waitIterations = 400

	t.Helper()
	leaders, followers, candidates := 0, 0, 0
	for range waitIterations {
		leaders, followers, candidates = 0, 0, 0
		for _, srv := range h.raftServers {
			if srv == nil {
				continue // removed the leader
			}
			switch srv.Role() {
			case RaftRoleFollower:
				followers += 1
			case RaftRoleCandidate:
				candidates += 1
			case RaftRoleLeader:
				leaders += 1
			}
		}
		if leaders == 1 && candidates == 0 {
			break
		}
		time.Sleep(sleepLen)
	}
	if !(leaders == 1 && candidates == 0) {
		t.Fatalf("got leaders = %d, candidates = %d, wanted 1, 0",
			leaders, candidates)
	}
}

func (h *Harness) Shutdown(id ServerId) {
	h.raftServers[id].Shutdown()
	h.transports[id].Shutdown()
	delete(h.raftServers, id)
	// transport is retained for future reuse of serverID
}

// Restart restarts the server id, which must have been previously
// shutdown with ShutdownServer.
func (h *Harness) Restart(id ServerId) error {
	if _, found := h.raftServers[id]; found {
		return fmt.Errorf("Tried to restart server already running: %d", id)
	}
	var err error
	newRaftServer, err := NewRaftServer(id, filter(h.serverIds, id), h.stateDirs[id], &NoopStateMachine{})
	newRaftServer.transport = h.transports[id] // reuse transport
	if err != nil {
		return fmt.Errorf("Error creating new server for leaderID %d", id)
	}
	h.raftServers[id] = newRaftServer
	go h.transports[id].Serve()
	go h.raftServers[id].Start()
	return nil
}

func (h *Harness) Servers() iter.Seq[*RaftServer] {
	return func(yield func(*RaftServer) bool) {
		for _, rs := range h.raftServers {
			if !yield(rs) {
				return
			}
		}
	}
}
func (h *Harness) PersistentStates(
	t *testing.T,
) iter.Seq2[ServerId, persistentState] {
	return func(yield func(ServerId, persistentState) bool) {
		for id, stateDir := range h.stateDirs {
			ps, err := loadPersistentState(stateDir)
			if err != nil {
				t.Fatalf("Failed to load persistent state for server %d: %v", id, err)
			}
			if !yield(id, ps) {
				return
			}
		}
	}
}

// filter removes items matching `remove` from `list`.
// Used to create the list of peers for a given raft server such
// that the list doesn't include the server itself.
func filter[T comparable](list []T, remove T) []T {
	filtered := []T{}
	for _, item := range list {
		if item != remove {
			filtered = append(filtered, item)
		}
	}
	return filtered
}
