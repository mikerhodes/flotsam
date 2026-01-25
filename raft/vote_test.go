package raft

// vote_test.go tests:
// - server responses to RequestVote RPCs.
// - server responses to its own RequestVote RPCs.
// We don't need to lock the raft server state as there are
// no parallel goroutines active in general --- because we don't
// start the server's background ticker. The only goroutines
// started are when an election is won, when a first heartbeat
// is sent.

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestRVFollowerGrantsVote(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 1

	res := raftSrv.processRequestVote(RequestVoteReq{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if res.VoteGranted != true {
		t.Errorf("res.VoteGranted= %t, want true", res.VoteGranted)
	}
	if res.Term != 2 {
		t.Errorf("res.Term = %d, want 2", res.Term)
	}
	if term := raftSrv.state.currentTerm; term != 2 {
		t.Errorf("srv.state.currentTerm = %d, want 2", term)
	}
}

func TestRVLeaderGrantsVoteBecomesFollower(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 1
	raftSrv.state.role = RaftRoleLeader

	res := raftSrv.processRequestVote(RequestVoteReq{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if res.VoteGranted != true {
		t.Errorf("res.VoteGranted= %t, want true", res.VoteGranted)
	}
	if res.Term != 2 {
		t.Errorf("res.Term = %d, want 2", res.Term)
	}
	if term := raftSrv.state.currentTerm; term != 2 {
		t.Errorf("srv.state.currentTerm = %d, want 2", term)
	}
	if role := raftSrv.Role(); role != RaftRoleFollower {
		t.Errorf("srv.Role() = %s, want RaftRoleFollower", role)
	}
}

func TestRVFollowerGrantsOnlyOneVote(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 1

	res := raftSrv.processRequestVote(RequestVoteReq{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if res.VoteGranted != true {
		t.Errorf("res.VoteGranted= %t, want true", res.VoteGranted)
	}
	res = raftSrv.processRequestVote(RequestVoteReq{
		Term:         2,
		CandidateId:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if res.VoteGranted != false {
		t.Errorf("res.VoteGranted= %t, want false", res.VoteGranted)
	}
}

func TestRVFollowerRejectsVoteOldTerm(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 3

	res := raftSrv.processRequestVote(RequestVoteReq{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if res.VoteGranted != false {
		t.Errorf("res.VoteGranted= %t, want false", res.VoteGranted)
	}
	if res.Term != 3 {
		t.Errorf("res.Term = %d, want 3", res.Term)
	}
	if term := raftSrv.state.currentTerm; term != 3 {
		t.Errorf("srv.state.currentTerm = %d, want 3", term)
	}
}

// mockVoter is an rpcOutgoingTransport that allows sending a
// sequence of votes to the raft server.
type mockVoter struct {
	sync.Mutex
	votes       []bool
	nextVoteIdx int
}

// makeRequestVoteRequest returns a hardcoded vote.
func (r *mockVoter) makeRequestVoteRequest(
	ctx context.Context,
	peer ServerId,
	requestVote RequestVoteReq,
) (*RequestVoteRes, error) {
	r.Lock()
	defer r.Unlock()
	vote := r.votes[r.nextVoteIdx]
	r.nextVoteIdx++

	return &RequestVoteRes{
		Term:        requestVote.Term,
		VoteGranted: vote,
	}, nil
}

// makeHeartbeatRequest returns a hardcoded heartbeat response
func (r *mockVoter) makeHeartbeatRequest(
	ctx context.Context,
	peer ServerId,
	appendEntries AppendEntriesReq,
) (*AppendEntriesRes, error) {
	return &AppendEntriesRes{ // standard okay response
		Term:    appendEntries.Term,
		Success: true,
	}, nil
}

// TestBecomeLeaderInitializesNextIndexAndMatchIndex tests that when a server
// becomes leader, nextIndex and matchIndex are initialized correctly per
// Raft paper Figure 2:
// - nextIndex[]: initialized to leader's last log index + 1
// - matchIndex[]: initialized to 0
func TestBecomeLeaderInitializesNextIndexAndMatchIndex(t *testing.T) {
	peers := []ServerId{2, 3, 4}
	raftSrv, err := NewRaftServer(1, peers, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}

	raftSrv.state.log.Append([]*LogEntry{
		{Term: 1, Command: []byte{1}},
		{Term: 1, Command: []byte{2}},
		{Term: 2, Command: []byte{3}}, // index 3, nextIndex=4
	})

	raftSrv.becomeLeader()

	// nextIndex should be 4
	for _, peer := range peers {
		if got := raftSrv.state.nextIndex[peer]; got != 4 {
			t.Errorf("nextIndex[%d] = %d, want 4", peer, got)
		}
	}

	// matchIndex should be 0 for all peers
	for _, peer := range peers {
		if got := raftSrv.state.matchIndex[peer]; got != 0 {
			t.Errorf("matchIndex[%d] = %d, want 0", peer, got)
		}
	}
}

// TestBecomeLeaderInitializesNextIndexEmptyLog tests nextIndex initialization
// when the log is empty.
func TestBecomeLeaderInitializesNextIndexEmptyLog(t *testing.T) {
	peers := []ServerId{2, 3}
	raftSrv, err := NewRaftServer(1, peers, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}

	// Log is empty, so nextIndex should be 1
	raftSrv.becomeLeader()

	for _, peer := range peers {
		if got := raftSrv.state.nextIndex[peer]; got != 1 {
			t.Errorf("nextIndex[%d] = %d, want 1", peer, got)
		}
		if got := raftSrv.state.matchIndex[peer]; got != 0 {
			t.Errorf("matchIndex[%d] = %d, want 0", peer, got)
		}
	}
}

func TestServerElection(t *testing.T) {
	tests := []struct {
		name         string
		votes        []bool
		expectedRole RaftRole
	}{
		{"wins with two votes", []bool{true, true}, RaftRoleLeader},
		{"wins with one vote (self + one)", []bool{true, false}, RaftRoleLeader},
		{"wins majority of five", []bool{true, false, true, false}, RaftRoleLeader},
		{"loses with no votes", []bool{false, false}, RaftRoleCandidate},
		{"loses without majority of five", []bool{true, false, false, false}, RaftRoleCandidate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				peers := make([]ServerId, len(tt.votes))
				for i := range peers {
					peers[i] = ServerId(i)
				}

				raftSrv, err := NewRaftServer(1, peers, t.TempDir())
				if err != nil {
					t.Fatalf("NewRaftServer failed: %v", err)
				}
				raftSrv.state.electionDeadline = time.Now().Add(-1 * time.Second)
				mockR := &mockVoter{votes: tt.votes}
				raftSrv.transport = mockR

				raftSrv.maybeStartElection()

				synctest.Wait()

				if raftSrv.state.role != tt.expectedRole {
					t.Errorf("role = %s, want %s", raftSrv.state.role, tt.expectedRole)
				}
				if mockR.nextVoteIdx != len(tt.votes) {
					t.Errorf("votes requested = %d, want %d", mockR.nextVoteIdx, len(tt.votes))
				}
			})
		})
	}
}
