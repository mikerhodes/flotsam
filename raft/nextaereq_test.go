package raft

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestNextAEReqForPeer_PeerAtOne(t *testing.T) {
	// When nextIndex is 1 (peer needs all entries),
	// should return all entries with prevLogIndex=0
	raftSrv, err := NewRaftServer(1, []ServerId{2}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 2
	raftSrv.state.commitIndex = 1
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1}},
		{Term: 2, Command: []byte{2}},
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.nextIndex[2] = 1 // peer needs all entries

	req := raftSrv.nextAEReqForPeer(2)

	if req.req.PrevLogIndex != 0 {
		t.Errorf("req.PrevLogIndex = %d, want 0", req.req.PrevLogIndex)
	}
	if req.req.PrevLogTerm != 0 {
		t.Errorf("req.PrevLogTerm = %d, want 0", req.req.PrevLogTerm)
	}
	if diff := cmp.Diff(originalLog, req.req.Entries); diff != "" {
		t.Errorf("req.Entries mismatch (-want +got):\n%s", diff)
	}
	if req.newMatchIndex != 2 {
		t.Errorf("newIndex = %d, want 2", req.newMatchIndex)
	}
	if req.newNextIndex != 3 {
		t.Errorf("newIndex = %d, want 3", req.newNextIndex)
	}
}

func TestNextAEReqForPeer_EmptyLog_PeerAtOne(t *testing.T) {
	// When log is empty and nextIndex is 1 (initialized state),
	// should return empty entries and prevLogIndex=0
	raftSrv, err := NewRaftServer(1, []ServerId{2}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 5
	raftSrv.state.commitIndex = 0
	raftSrv.state.nextIndex[2] = 1

	nextReq := raftSrv.nextAEReqForPeer(2)
	req := nextReq.req

	if req.Term != 5 {
		t.Errorf("req.Term = %d, want 5", req.Term)
	}
	if req.LeaderId != 1 {
		t.Errorf("req.LeaderId = %d, want 1", req.LeaderId)
	}
	if req.PrevLogIndex != 0 {
		t.Errorf("req.PrevLogIndex = %d, want 0", req.PrevLogIndex)
	}
	if req.PrevLogTerm != 0 {
		t.Errorf("req.PrevLogTerm = %d, want 0", req.PrevLogTerm)
	}
	if req.LeaderCommit != 0 {
		t.Errorf("req.LeaderCommit = %d, want 0", req.LeaderCommit)
	}
	if diff := cmp.Diff([]*LogEntry{}, req.Entries); diff != "" {
		t.Errorf("req.Entries mismatch (-want +got):\n%s", diff)
	}
	if nextReq.newMatchIndex != 0 {
		t.Errorf("newMatchIndex = %d, want 0", nextReq.newMatchIndex)
	}
	if nextReq.newNextIndex != 1 {
		t.Errorf("newNextIndex = %d, want 1", nextReq.newNextIndex)
	}
}

func TestNextAEReqForPeer_WithEntries_PeerUpToDate(t *testing.T) {
	// When log has entries and nextIndex is at end (peer is up to date),
	// should return empty entries (heartbeat)
	raftSrv, err := NewRaftServer(1, []ServerId{2}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 3
	raftSrv.state.commitIndex = 2
	raftSrv.state.log = RaftLog{[]*LogEntry{
		{Term: 1, Command: []byte{1}},
		{Term: 2, Command: []byte{2}},
	}}
	raftSrv.state.nextIndex[2] = 3 // next index is beyond log (peer is caught up)

	nextReq := raftSrv.nextAEReqForPeer(2)
	req := nextReq.req

	if req.Term != 3 {
		t.Errorf("req.Term = %d, want 3", req.Term)
	}
	if req.PrevLogIndex != 2 {
		t.Errorf("req.PrevLogIndex = %d, want 2", req.PrevLogIndex)
	}
	if req.PrevLogTerm != 2 {
		t.Errorf("req.PrevLogTerm = %d, want 2", req.PrevLogTerm)
	}
	if req.LeaderCommit != 2 {
		t.Errorf("req.LeaderCommit = %d, want 2", req.LeaderCommit)
	}
	if diff := cmp.Diff([]*LogEntry{}, req.Entries); diff != "" {
		t.Errorf("req.Entries mismatch (-want +got):\n%s", diff)
	}
	if nextReq.newMatchIndex != 2 {
		t.Errorf("newMatchIndex = %d, want 2", nextReq.newNextIndex)
	}
	if nextReq.newNextIndex != 3 {
		t.Errorf("newNextIndex = %d, want 3", nextReq.newNextIndex)
	}
}

func TestNextAEReqForPeer_WithEntries_PeerNeedsUpdates(t *testing.T) {
	// When log has entries and nextIndex is behind (peer needs entries),
	// should return entries from nextIndex onwards
	raftSrv, err := NewRaftServer(1, []ServerId{2}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 3
	raftSrv.state.commitIndex = 2
	raftSrv.state.log = RaftLog{[]*LogEntry{
		{Term: 1, Command: []byte{1}},
		{Term: 2, Command: []byte{2}}, // index 2
		{Term: 3, Command: []byte{3}},
	}}
	raftSrv.state.nextIndex[2] = 2 // peer needs entries 2 and 3
	wantEntries := raftSrv.state.log.log[1:]

	nextReq := raftSrv.nextAEReqForPeer(2)
	req := nextReq.req

	if req.Term != 3 {
		t.Errorf("req.Term = %d, want 3", req.Term)
	}
	if req.PrevLogIndex != 1 {
		t.Errorf("req.PrevLogIndex = %d, want 1", req.PrevLogIndex)
	}
	if req.PrevLogTerm != 1 {
		t.Errorf("req.PrevLogTerm = %d, want 1", req.PrevLogTerm)
	}
	if diff := cmp.Diff(wantEntries, req.Entries); diff != "" {
		t.Errorf("req.Entries mismatch (-want +got):\n%s", diff)
	}
	if nextReq.newMatchIndex != 3 {
		t.Errorf("newMatchIndex = %d, want 3", nextReq.newMatchIndex)
	}
	if nextReq.newNextIndex != 4 {
		t.Errorf("newNextIndex = %d, want 4", nextReq.newNextIndex)
	}
}

func TestNextAEReqForPeer_PeerAtZero(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{2}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 2
	raftSrv.state.commitIndex = 1
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1}},
		{Term: 2, Command: []byte{2}},
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.nextIndex[2] = 0 // invalid state

	nextReq := raftSrv.nextAEReqForPeer(2)
	req := nextReq.req

	// nextIndex = 0 means we need to send all entries
	if req.PrevLogIndex != 0 {
		t.Errorf("req.PrevLogIndex = %d, want 0", req.PrevLogIndex)
	}
	if req.PrevLogTerm != 0 {
		t.Errorf("req.PrevLogTerm = %d, want 0", req.PrevLogTerm)
	}
	if diff := cmp.Diff(originalLog, req.Entries); diff != "" {
		t.Errorf("req.Entries mismatch (-want +got):\n%s", diff)
	}
	if nextReq.newMatchIndex != 2 {
		t.Errorf("newMatchIndex = %d, want 2", nextReq.newMatchIndex)
	}
	if nextReq.newNextIndex != 3 {
		t.Errorf("newNextIndex = %d, want 3", nextReq.newNextIndex)
	}
}
