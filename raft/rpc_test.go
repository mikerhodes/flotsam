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

	// Wait for election timeout (150-300ms) plus some buffer
	time.Sleep(350 * time.Millisecond)

	// Two servers should be followers, one should be leader
	leaders, followers, candidates := 0, 0, 0
	for _, srv := range servers {
		switch srv.Role() {
		case RaftRoleFollower:
			followers += 1
		case RaftRoleCandidate:
			candidates += 1
		case RaftRoleLeader:
			leaders += 1
		}
	}

	if leaders != 1 {
		t.Errorf("leaders = %d, want 1", leaders)
	}
	if followers != 2 {
		t.Errorf("followers = %d, want 2", followers)
	}
	if candidates != 0 {
		t.Errorf("candidates = %d, want 0", candidates)
	}

	// Clean up
	for id, srv := range servers {
		srv.Shutdown()
		raftGroup[id].listener.Close()
	}
}
