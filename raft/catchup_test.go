package raft

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// mockTransportCapturingAE captures AppendEntries requests for inspection.
type mockTransportCapturingAE struct {
	appendEntriesReqs []AppendEntriesReq
	// failuresRemaining is the number of times makeHeartbeatRequest will
	// return Success: false before returning Success: true.
	failuresRemaining int
}

func (m *mockTransportCapturingAE) makeRequestVoteRequest(
	ctx context.Context,
	peer ServerId,
	requestVote RequestVoteReq,
) (*RequestVoteRes, error) {
	return &RequestVoteRes{
		Term:        requestVote.Term,
		VoteGranted: true,
	}, nil
}

func (m *mockTransportCapturingAE) makeHeartbeatRequest(
	ctx context.Context,
	peer ServerId,
	appendEntries AppendEntriesReq,
) (*AppendEntriesRes, error) {
	m.appendEntriesReqs = append(m.appendEntriesReqs, appendEntries)
	m.failuresRemaining--
	return &AppendEntriesRes{
		Term:    appendEntries.Term,
		Success: m.failuresRemaining < 0,
	}, nil
}

func TestCatchUpPeerDoesNothingWhenNotLeader(t *testing.T) {
	peers := []ServerId{2}
	raftSrv, err := NewRaftServer(1, peers, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.role = RaftRoleFollower
	raftSrv.state.log.Append([]*LogEntry{
		{Term: 1, Command: []byte{1}},
	})
	mockT := &mockTransportCapturingAE{}
	raftSrv.transport = mockT

	raftSrv.catchUpPeer(peers[0])

	if len(mockT.appendEntriesReqs) != 0 {
		t.Errorf("len(mockT.appendEntriesReqs) = %d, want 0", len(mockT.appendEntriesReqs))
	}
}

func TestCatchUpPeerDoesNothingWhenPeerUpToDate(t *testing.T) {
	peers := []ServerId{2}
	raftSrv, err := NewRaftServer(1, peers, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.role = RaftRoleLeader
	raftSrv.state.log.Append([]*LogEntry{
		{Term: 1, Command: []byte{1}},
		{Term: 1, Command: []byte{2}},
	})
	raftSrv.state.nextIndex[2] = 3 // peer is up to date (log len is 2, next is 3)
	mockT := &mockTransportCapturingAE{}
	raftSrv.transport = mockT

	raftSrv.catchUpPeer(2)

	if len(mockT.appendEntriesReqs) != 0 {
		t.Errorf("len(mockT.appendEntriesReqs) = %d, want 0", len(mockT.appendEntriesReqs))
	}
}

func TestCatchUpPeerSendsCorrectAppendEntries(t *testing.T) {
	peerId := ServerId(2)
	raftSrv, err := NewRaftServer(1, []ServerId{peerId}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 2
	raftSrv.state.role = RaftRoleLeader
	leaderLog := []*LogEntry{
		{Term: 1, Command: []byte{1}}, // 1
		{Term: 2, Command: []byte{2}}, // 2
		{Term: 2, Command: []byte{3}}, // 3
	}
	raftSrv.state.log = RaftLog{leaderLog}
	mockT := &mockTransportCapturingAE{}
	raftSrv.transport = mockT

	// Set nextIndex so peer wants entries 2 and 3
	raftSrv.state.nextIndex[peerId] = 2
	expectedLog := leaderLog[1:]

	raftSrv.catchUpPeer(peerId)

	if len(mockT.appendEntriesReqs) != 1 {
		t.Fatalf("len(mockT.appendEntriesReqs) = %d, want 1", len(mockT.appendEntriesReqs))
	}

	expectedReq := AppendEntriesReq{
		Term:         2,
		LeaderId:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      expectedLog,
		LeaderCommit: 0,
	}
	if diff := cmp.Diff(expectedReq, mockT.appendEntriesReqs[0]); diff != "" {
		t.Errorf("AppendEntriesReq mismatch (-want +got):\n%s", diff)
	}
}

func TestCatchUpPeerRetriesOnFailure(t *testing.T) {
	peerId := ServerId(2)
	raftSrv, err := NewRaftServer(1, []ServerId{peerId}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 2
	raftSrv.state.role = RaftRoleLeader
	raftSrv.state.log = RaftLog{[]*LogEntry{
		{Term: 1, Command: []byte{1}},
		{Term: 2, Command: []byte{2}},
	}}
	raftSrv.state.nextIndex[peerId] = 2
	mockT := &mockTransportCapturingAE{
		failuresRemaining: 5,
	}
	raftSrv.transport = mockT

	raftSrv.catchUpPeer(peerId)

	// 5 failures + 1 success = 6 requests
	if len(mockT.appendEntriesReqs) != 6 {
		t.Errorf("len(mockT.appendEntriesReqs) = %d, want 6", len(mockT.appendEntriesReqs))
	}
}
