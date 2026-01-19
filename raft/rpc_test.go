package raft

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestSingleServerChangesTermAppendEntries(t *testing.T) {
	stateDir := t.TempDir()
	raftSrv, err := NewRaftServer(1, []ServerId{}, stateDir)
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	// Server should start as follower
	if role := raftSrv.Role(); role != RaftRoleFollower {
		t.Errorf("srv.Role() = %s, want RaftRoleFollower", role)
	}

	// If we send an AppendEntries for a later term,
	// the raft server should change term.
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         5,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if res.Term != 5 {
		t.Errorf("res.Term = %d, want 5", res.Term)
	}

	if role := raftSrv.Role(); role != RaftRoleFollower {
		t.Errorf("srv.Role() = %s, want RaftRoleFollower", role)
	}
	if term := raftSrv.state.currentTerm; term != 5 {
		t.Errorf("srv.state.currentTerm = %d, want 5", term)
	}
}

func TestSingleServerBecomesLeader(t *testing.T) {
	stateDir := t.TempDir()
	transport, err := NewHttpTransport()
	if err != nil {
		t.Fatalf("NewHttpTransport failed: %v", err)
	}
	raftSrv, err := NewRaftServer(1, []ServerId{}, stateDir)
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	transport.Host(raftSrv)

	go transport.Serve()
	go raftSrv.Start()

	// Server should start as follower
	if role := raftSrv.Role(); role != RaftRoleFollower {
		t.Errorf("srv.Role() = %s, want RaftRoleFollower", role)
	}

	// Wait for election timeout (150-300ms) plus some buffer
	time.Sleep(350 * time.Millisecond)

	// Server should be the leader, because there is only one
	// server, so by voting for itself it is in the majority.
	if role := raftSrv.Role(); role != RaftRoleLeader {
		t.Errorf("srv.Role() = %s, want RaftRoleLeader", role)
	}

	// Clean up
	raftSrv.Shutdown()
	transport.Shutdown()
}

func TestThreeServersOneLeaderAtStartup(t *testing.T) {
	transports := map[ServerId]*HttpTransport{}
	raftServers := map[ServerId]*RaftServer{}
	stateDirs := map[ServerId]string{}
	addrs := map[ServerId]net.Addr{}
	serverIds := []ServerId{1, 2, 3}
	for _, id := range serverIds {
		transport, err := NewHttpTransport()
		if err != nil {
			t.Fatalf("NewHttpTransport failed: %v", err)
		}
		stateDir := t.TempDir()
		raftSrv, err := NewRaftServer(id, filter(serverIds, id), stateDir)
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
	// Start everything up
	for _, transport := range transports {
		go transport.Serve()
	}
	for _, raft := range raftServers {
		go raft.Start()
	}

	// All servers should start as Followers
	for id, raft := range raftServers {
		if role := raft.Role(); role != RaftRoleFollower {
			t.Errorf("srv[%d].Role() = %s, want RaftRoleFollower", id, role)
		}
	}

	// Two servers should be followers, one should be leader
	wantedState := makeWantedState(1, 0, 2)
	waitForWantedState(t, raftServers, wantedState)

	// Clean up
	for _, transport := range transports {
		transport.Shutdown()
	}
	for _, raft := range raftServers {
		raft.Shutdown()
	}
}

func TestThreeServersPersistTerm(t *testing.T) {
	transports := map[ServerId]*HttpTransport{}
	raftServers := map[ServerId]*RaftServer{}
	stateDirs := map[ServerId]string{}
	addrs := map[ServerId]net.Addr{}
	serverIds := []ServerId{1, 2, 3}
	for _, id := range serverIds {
		transport, err := NewHttpTransport()
		if err != nil {
			t.Fatalf("NewHttpTransport failed: %v", err)
		}
		stateDir := t.TempDir()
		raftSrv, err := NewRaftServer(id, filter(serverIds, id), stateDir)
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
	// Start everything up
	for _, transport := range transports {
		go transport.Serve()
	}
	for _, raft := range raftServers {
		go raft.Start()
	}

	// Wait for leader
	wantedState := makeWantedState(1, 0, 2)
	waitForWantedState(t, raftServers, wantedState)

	// Get the leader's term before shutdown
	var leaderTerm Term
	for _, raft := range raftServers {
		if raft.Role() == RaftRoleLeader {
			leaderTerm = raft.CurrentTerm()
			break
		}
	}

	// Shutdown all servers to ensure state is persisted
	for _, raft := range raftServers {
		raft.Shutdown()
	}
	for _, transport := range transports {
		transport.Shutdown()
	}

	// Check state for each server
	nVoted := 0
	for id, stateDir := range stateDirs {
		ps, err := LoadPersistentState(stateDir)
		if err != nil {
			t.Fatalf("Failed to load persistent state for server %d: %v", id, err)
		}
		if ps.CurrentTerm != leaderTerm {
			t.Errorf("Server %d ps.CurrentTerm = %d, want %d", id, ps.CurrentTerm, leaderTerm)
		}
		if ps.VotedFor != noVote {
			nVoted++
		}
	}
	if nVoted < 2 {
		// At least two servers must have voted (inc. candidate)
		// for a server to win.
		t.Errorf("nVoted = %d, want > 2", nVoted)
	}
}

func TestThreeServersOneLeaderAfterLeaderDies(t *testing.T) {
	transports := map[ServerId]*HttpTransport{}
	raftServers := map[ServerId]*RaftServer{}
	stateDirs := map[ServerId]string{}
	addrs := map[ServerId]net.Addr{}
	serverIds := []ServerId{1, 2, 3}
	for _, id := range serverIds {
		transport, err := NewHttpTransport()
		if err != nil {
			t.Fatalf("NewHttpTransport failed: %v", err)
		}
		stateDir := t.TempDir()
		raftSrv, err := NewRaftServer(id, filter(serverIds, id), stateDir)
		if err != nil {
			t.Fatalf("NewRaftServer failed: %v", err)
		}
		transport.Host(raftSrv)

		transports[id] = transport
		raftServers[id] = raftSrv
		stateDirs[id] = stateDir
		addrs[id] = transport.Addr()
	}
	// Tell all transports about all the others' addresses
	for _, transport := range transports {
		transport.peerAddrs = addrs
	}
	// Start everything
	for _, transport := range transports {
		go transport.Serve()
	}
	for _, raft := range raftServers {
		go raft.Start()
	}

	// All servers should start as Followers
	for id, raft := range raftServers {
		if role := raft.Role(); role != RaftRoleFollower {
			t.Errorf("srv[%d].Role() = %s, want RaftRoleFollower", id, role)
		}
	}

	// Two servers should be followers, one should be leader
	// Save the leader ID as we will kill it.
	wantedState := makeWantedState(1, 0, 2)
	waitForWantedState(t, raftServers, wantedState)
	var firstLeaderId ServerId
	for _, srv := range raftServers {
		if srv.Role() == RaftRoleLeader {
			firstLeaderId = srv.serverId
		}
	}

	// Kill current leader --- leave the transport for
	// reuse later as we can't reconfigure a cluster.
	raftServers[firstLeaderId].Shutdown()
	transports[firstLeaderId].Shutdown()
	delete(raftServers, firstLeaderId)
	t.Logf("KILLED THE LEADER %d", firstLeaderId)

	// Now we should have 1 leader and 1 follower
	wantedState = makeWantedState(1, 0, 1)
	waitForWantedState(t, raftServers, wantedState)
	var secondLeaderId ServerId
	for _, srv := range raftServers {
		if srv == nil {
			continue // removed the leader
		}
		if srv.Role() == RaftRoleLeader {
			secondLeaderId = srv.serverId
		}
	}

	// Create a new instance for server with ID firstLeaderId
	// This is like firstLeaderId came back online after a restart.
	var err error
	newRaftServer, err := NewRaftServer(firstLeaderId, filter(serverIds, firstLeaderId), stateDirs[firstLeaderId])
	newRaftServer.transport = transports[firstLeaderId] // reuse transport
	if err != nil {
		t.Fatalf("Error creating new server for leaderID %d", firstLeaderId)
	}
	raftServers[firstLeaderId] = newRaftServer
	go transports[firstLeaderId].Serve()
	go newRaftServer.Start()
	t.Logf("REPLACED SERVER AT %d", firstLeaderId)

	// Should be back to one leader with two followers, and the
	// leader should not have changed.
	wantedState = makeWantedState(1, 0, 2)
	waitForWantedState(t, raftServers, wantedState)
	var thirdLeaderId ServerId
	for _, srv := range raftServers {
		if srv.Role() == RaftRoleLeader {
			thirdLeaderId = srv.serverId
		}
	}
	if thirdLeaderId != secondLeaderId {
		t.Errorf("thirdLeaderId = %d, want %d", thirdLeaderId, secondLeaderId)
	}

	// Clean up
	for _, transport := range transports {
		transport.Shutdown()
	}
	for _, raft := range raftServers {
		raft.Shutdown()
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

// makeWantedState creates a checker that a state is what
// is wanted.
func makeWantedState(wantL, wantC, wantF int) func(l, c, f int, lastIteration bool) (bool, []error) {
	return func(gotL, gotC, gotF int, lastIteration bool) (bool, []error) {
		result := true
		errs := []error{}
		if gotL != wantL {
			errs = append(errs, fmt.Errorf("leaders = %d, want %d", gotL, wantL))
			result = false
		}
		if gotC != wantC {
			errs = append(errs, fmt.Errorf("candidates = %d, want %d", gotC, wantC))
			result = false
		}
		if gotF != wantF {
			errs = append(errs, fmt.Errorf("followers = %d, want %d", gotF, wantF))
			result = false
		}
		return result, errs
	}
}

// waitForWantedState waits a bounded amount of time for a given
// state to be true.
func waitForWantedState(t *testing.T, servers map[ServerId]*RaftServer, wantedState func(l int, c int, f int, lastIteration bool) (bool, []error)) {
	leaders, followers, candidates := 0, 0, 0
	for range 100 {
		leaders, followers, candidates = 0, 0, 0
		for _, srv := range servers {
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
		if ok, _ := wantedState(leaders, candidates, followers, false); ok {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if ok, errs := wantedState(leaders, candidates, followers, false); !ok {
		for _, err := range errs {
			t.Error(err)
		}
		t.Fatal("Never reached required state")
	}
}
