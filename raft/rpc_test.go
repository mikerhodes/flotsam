package raft

import (
	"net"
	"testing"
	"testing/synctest"
	"time"
)

// fakeListener is a net.Listener that blocks on Accept until closed.
// Required for synctest because real network I/O is not "durably blocked".
type fakeListener struct {
	closed chan struct{}
}

func newFakeListener() *fakeListener {
	return &fakeListener{closed: make(chan struct{})}
}

func (f *fakeListener) Accept() (net.Conn, error) {
	<-f.closed
	return nil, net.ErrClosed
}

func (f *fakeListener) Close() error {
	select {
	case <-f.closed:
	default:
		close(f.closed)
	}
	return nil
}

func (f *fakeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}
}

func TestSingleServerBecomesLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, err := NewRaftServer(1, []net.Addr{})
		if err != nil {
			t.Fatalf("NewRaftServer failed: %v", err)
		}

		listener := newFakeListener()

		go srv.Start(listener)

		// Server should start as follower
		synctest.Wait()
		if role := srv.Role(); role != RaftRoleFollower {
			t.Errorf("srv.Role() = %s, want RaftRoleCandidate", role)
		}

		// Wait for election timeout (150-300ms) plus some buffer
		time.Sleep(350 * time.Millisecond)
		synctest.Wait()

		// Server should be the leader, because there is only one
		// server, so by voting for itself it is in the majority.
		if role := srv.Role(); role != RaftRoleLeader {
			t.Errorf("srv.Role() = %s, want RaftRoleLeader", role)
		}

		// Clean up
		srv.Shutdown()
		listener.Close()
		synctest.Wait()
	})
}

func TestThreeServersOneLeaderAtStartup(t *testing.T) {
	// Generate the "template" for each raft server.
	type Peer struct {
		id       ServerId
		listener net.Listener
		addr     net.Addr
	}
	raftGroup := []Peer{}
	for id := range 3 {
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Fatalf("Couldn't open tcp listener: %v", err)
		}
		raftGroup = append(raftGroup, Peer{
			id:       ServerId(id),
			listener: l,
			addr:     l.Addr(),
		})
	}

	servers := []*RaftServer{}
	for _, peer := range raftGroup {
		peerAddrs := []net.Addr{}
		for _, cp := range raftGroup {
			if cp.id != peer.id {
				peerAddrs = append(peerAddrs, cp.addr)
			}
		}

		srv, err := NewRaftServer(peer.id, peerAddrs)
		if err != nil {
			t.Fatalf("NewRaftServer failed: %v", err)
		}
		go srv.Start(peer.listener)

		servers = append(servers, srv)
	}

	// All servers should start as Followers
	for id, srv := range servers {
		if role := srv.Role(); role != RaftRoleFollower {
			t.Errorf("srv[%d].Role() = %s, want RaftRoleFollower", id, role)
		}
	}

	// Two servers should be followers, one should be leader
	wantedState := makeWantedState(t, 1, 0, 2)
	waitForWantedState(t, servers, wantedState)

	// Clean up
	for id, srv := range servers {
		srv.Shutdown()
		raftGroup[id].listener.Close()
	}
}

func TestThreeServersOneLeaderAfterLeaderDies(t *testing.T) {
	// Generate the "template" for each raft server.
	type Peer struct {
		id       ServerId
		listener net.Listener
		addr     net.Addr
	}
	raftGroup := []Peer{}
	for id := range 3 {
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Fatalf("Couldn't open tcp listener: %v", err)
		}
		raftGroup = append(raftGroup, Peer{
			id:       ServerId(id),
			listener: l,
			addr:     l.Addr(),
		})
	}

	servers := []*RaftServer{}
	for _, peer := range raftGroup {
		peerAddrs := []net.Addr{}
		for _, cp := range raftGroup {
			if cp.id != peer.id {
				peerAddrs = append(peerAddrs, cp.addr)
			}
		}

		srv, err := NewRaftServer(peer.id, peerAddrs)
		if err != nil {
			t.Fatalf("NewRaftServer failed: %v", err)
		}
		go srv.Start(peer.listener)

		servers = append(servers, srv)
	}

	// All servers should start as Followers
	for id, srv := range servers {
		if role := srv.Role(); role != RaftRoleFollower {
			t.Errorf("srv[%d].Role() = %s, want RaftRoleFollower", id, role)
		}
	}

	// Two servers should be followers, one should be leader
	wantedState := makeWantedState(t, 1, 0, 2)
	waitForWantedState(t, servers, wantedState)
	var firstLeaderId ServerId
	for _, srv := range servers {
		if srv.Role() == RaftRoleLeader {
			firstLeaderId = srv.serverId
		}
	}

	// Kill current leader
	servers[firstLeaderId].Shutdown()
	servers[firstLeaderId] = nil

	t.Logf("KILLED THE LEADER %d", firstLeaderId)

	// Now we should have 1 leader and 1 follower
	wantedState = makeWantedState(t, 1, 0, 1)
	waitForWantedState(t, servers, wantedState)
	var secondLeaderId ServerId
	for _, srv := range servers {
		if srv == nil {
			continue // removed the leader
		}
		if srv.Role() == RaftRoleLeader {
			secondLeaderId = srv.serverId
		}
	}

	// Create a new instance for server with ID firstLeaderId
	// This is like firstLeaderId came back online after a restart.
	peerAddrs := []net.Addr{}
	for _, cp := range raftGroup {
		if cp.id != firstLeaderId {
			peerAddrs = append(peerAddrs, cp.addr)
		}
	}
	var err error
	servers[firstLeaderId], err = NewRaftServer(firstLeaderId, peerAddrs)
	if err != nil {
		t.Fatalf("Error creating new server for leaderID %d", firstLeaderId)
	}
	go servers[firstLeaderId].Start(raftGroup[firstLeaderId].listener)
	t.Logf("REPLACED SERVER AT %d", firstLeaderId)

	// Should be back to one leader with two followers, and the
	// leader should not have changed.
	wantedState = makeWantedState(t, 1, 0, 2)
	waitForWantedState(t, servers, wantedState)
	var thirdLeaderId ServerId
	for _, srv := range servers {
		if srv.Role() == RaftRoleLeader {
			thirdLeaderId = srv.serverId
		}
	}
	if thirdLeaderId != secondLeaderId {
		t.Errorf("thirdLeaderId = %d, want %d", thirdLeaderId, secondLeaderId)
	}

	// Clean up
	for id, srv := range servers {
		srv.Shutdown()
		raftGroup[id].listener.Close()
	}
}

// makeWantedState creates a checker that a state is what
// is wanted.
func makeWantedState(t *testing.T, l, c, f int) func(l, c, f int, lastIteration bool) bool {
	return func(gotL, gotC, gotF int, lastIteration bool) bool {
		result := true
		if gotL != l {
			if lastIteration {
				t.Errorf("leaders = %d, want 1", gotL)
			}
			result = false
		}
		if gotC != c {
			if lastIteration {
				t.Errorf("candidates = %d, want 0", gotC)
			}
			result = false
		}
		if gotF != f {
			if lastIteration {
				t.Errorf("followers = %d, want 2", gotF)
			}
			result = false
		}
		return result
	}
}

// waitForWantedState waits a bounded amount of time for a given
// state to be true.
func waitForWantedState(t *testing.T, servers []*RaftServer, wantedState func(l int, c int, f int, lastIteration bool) bool) {
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
		if wantedState(leaders, candidates, followers, false) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !wantedState(leaders, candidates, followers, true) {
		t.Fatal("Never reached required state")
	}
}
