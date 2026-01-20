package raft

// rpc_test.go puts raft servers into specific states and checks
// how the state changes and the server responds to RPC calls.
// We don't need to lock the raft server state as there are
// no parallel goroutines active in general --- because we don't
// start the server's background ticker. The only goroutines
// started are when an election is won, when a first heartbeat
// is sent.

import (
	"testing"
	"time"
)

func TestRaftServerStartsFollower(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	if role := raftSrv.Role(); role != RaftRoleFollower {
		t.Errorf("srv.Role() = %s, want RaftRoleFollower", role)
	}
}

func TestAEGreaterTermIncreasesReceiversTerm(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}

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
	if term := raftSrv.state.currentTerm; term != 5 {
		t.Errorf("srv.state.currentTerm = %d, want 5", term)
	}
}

func TestAELowerTermDoesNotChangeReceiversTerm(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 10

	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         5,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	})

	if res.Success != false {
		t.Errorf("res.Success = %t, want false", res.Success)
	}
	if res.Term != 10 {
		t.Errorf("res.Term = %d, want 10", res.Term)
	}
}

func TestAEAdvancesElectionDeadline(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.electionDeadline = time.Now().Add(-1 * time.Second)

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
	if !time.Now().Before(raftSrv.state.electionDeadline) {
		t.Errorf("raftSrv.state.electionDeadline should be in the future")
	}

}

func TestAEGreaterTermMakesReceiverFollower(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir())
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 1
	raftSrv.state.role = RaftRoleLeader

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
	if term := raftSrv.state.currentTerm; term != 5 {
		t.Errorf("srv.state.currentTerm = %d, want 5", term)
	}
	if role := raftSrv.Role(); role != RaftRoleFollower {
		t.Errorf("srv.Role() = %s, want RaftRoleFollower", role)
	}
}

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
