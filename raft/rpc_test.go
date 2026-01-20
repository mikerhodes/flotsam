package raft

import (
	"testing"
)

func TestSingleServerChangesTermAppendEntries(t *testing.T) {
	// Here we don't start a server, but instead test that it
	// responds to an AE request correctly.
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
