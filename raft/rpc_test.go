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
		srv, err := NewRaftServer(1)
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
