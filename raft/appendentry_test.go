package raft

// rpc_test.go puts raft servers into specific states and checks
// how the state changes and the server responds to RPC calls.
// We don't need to lock the raft server state as there are
// no parallel goroutines active in general --- because we don't
// start the server's background ticker. The only goroutines
// started are when an election is won, when a first heartbeat
// is sent.

import (
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// NoopStateMachine is a StateMachine implementation that does
// nothing, for use in tests that don't need a real state machine.
type NoopStateMachine struct {
}

// apply implements StateMachine
func (sm *NoopStateMachine) apply(_ []byte) {}

func TestRaftServerStartsFollower(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	if role := raftSrv.Role(); role != RaftRoleFollower {
		t.Errorf("srv.Role() = %s, want RaftRoleFollower", role)
	}
}

//
// Processing heartbeats
//

func TestAEGreaterTermIncreasesReceiversTerm(t *testing.T) {
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}

	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         5,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{},
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
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 10

	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         5,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{},
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
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.electionDeadline = time.Now().Add(-1 * time.Second)

	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         5,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{},
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
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
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
		Entries:      []*LogEntry{},
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

//
// Appending entries to log
//

func TestAELeaderAppendsFirstLogEntries(t *testing.T) {
	// Send an AE with first logs and a new term
	log := []*LogEntry{
		{Term: 1, Command: []byte{1, 2}},
		{Term: 1, Command: []byte{3, 4}},
	}

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      log,
		LeaderCommit: 0,
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if res.Term != 1 {
		t.Errorf("res.Term = %d, want 1", res.Term)
	}
	if term := raftSrv.state.currentTerm; term != 1 {
		t.Errorf("srv.state.currentTerm = %d, want 1", term)
	}
	if role := raftSrv.Role(); role != RaftRoleFollower {
		t.Errorf("srv.Role() = %s, want RaftRoleFollower", role)
	}
	if diff := cmp.Diff(log, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

func TestAEOutOfDateLeaderDoesNotAppendLogEntries(t *testing.T) {
	// "Reply false if term < currentTerm (5.1)"
	log := []*LogEntry{
		{Term: 1, Command: []byte{1, 2}},
	}

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.currentTerm = 2
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         1, // <currentTerm
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      log,
		LeaderCommit: 0,
	})

	if res.Success != false {
		t.Errorf("res.Success = %t, want false", res.Success)
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
	if diff := cmp.Diff([]*LogEntry{}, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

func TestAEMatchingprevLogIndexLogDoesAppendLogEntries(t *testing.T) {
	// "Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (5.3)"
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 2, Command: []byte{2, 1}},
		{Term: 3, Command: []byte{3, 1}},
	}
	newEntries := []*LogEntry{
		{Term: 3, Command: []byte{3, 2}},
		{Term: 3, Command: []byte{3, 3}},
		{Term: 4, Command: []byte{3, 3}},
	}
	expected := slices.Concat(originalLog, newEntries)

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = originalLog[len(originalLog)-1].Term
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 4, // 1-based
		PrevLogTerm:  3,
		Entries:      newEntries,
		LeaderCommit: 0,
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if diff := cmp.Diff(expected, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

func TestAEMismatchedprevLogIndexLogDoesntAppend(t *testing.T) {
	// "Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (5.3)"
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 2, Command: []byte{2, 1}},
		{Term: 3, Command: []byte{3, 1}}, // prevLogIndex=4
	}
	newEntries := []*LogEntry{
		{Term: 3, Command: []byte{3, 2}}, // should not be applied
	}
	expected := originalLog

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = originalLog[len(originalLog)-1].Term
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 4, // 1-based
		PrevLogTerm:  2,
		Entries:      newEntries,
		LeaderCommit: 0,
	})

	if res.Success != false {
		t.Errorf("res.Success = %t, want false", res.Success)
	}
	if diff := cmp.Diff(expected, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

func TestAEMismatchedLogAtprevLogIndexDoesntAppend(t *testing.T) {
	// "Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (5.3)"
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 2, Command: []byte{2, 1}}, // prevLogIndex=3
		{Term: 3, Command: []byte{3, 1}},
	}
	newEntries := []*LogEntry{
		{Term: 3, Command: []byte{3, 2}},
	}
	expected := originalLog

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = originalLog[len(originalLog)-1].Term
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 3, // 1-based
		PrevLogTerm:  4, // bad term, should be 2
		Entries:      newEntries,
		LeaderCommit: 0,
	})

	if res.Success != false {
		t.Errorf("res.Success = %t, want false", res.Success)
	}
	if diff := cmp.Diff(expected, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

func TestAELogAtUnknownprevLogIndexDoesntAppend(t *testing.T) {
	// "Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (5.3)"
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 2, Command: []byte{2, 1}}, // prevLogIndex=3
		{Term: 3, Command: []byte{3, 1}},
	}
	newEntries := []*LogEntry{
		{Term: 3, Command: []byte{3, 2}},
	}
	expected := originalLog

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = originalLog[len(originalLog)-1].Term
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 9, // too high, server doesn't have log to this index
		PrevLogTerm:  2, // correct term
		Entries:      newEntries,
		LeaderCommit: 0,
	})

	if res.Success != false {
		t.Errorf("res.Success = %t, want false", res.Success)
	}
	if diff := cmp.Diff(expected, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

func TestAEAppendOnlyNewEntries(t *testing.T) {
	// "Append any new entries not already in log"
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 2, Command: []byte{2, 1}}, // 3
		{Term: 3, Command: []byte{3, 1}}, // 4
	}
	newEntries := []*LogEntry{
		{Term: 2, Command: []byte{2, 1}}, // dup existing 3
		{Term: 3, Command: []byte{3, 1}}, // dup existing 4
		{Term: 3, Command: []byte{3, 2}}, // new entries start here
		{Term: 3, Command: []byte{3, 3}},
		{Term: 4, Command: []byte{4, 1}},
	}
	expected := slices.Concat(originalLog, newEntries[2:])

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = originalLog[len(originalLog)-1].Term
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      newEntries,
		LeaderCommit: 0,
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if diff := cmp.Diff(expected, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

func TestAEAppendOnlyNewEntriesNoNewEntries(t *testing.T) {
	// "Append any new entries not already in log"
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 2, Command: []byte{2, 1}}, // 3
		{Term: 3, Command: []byte{3, 1}}, // 4
	}
	newEntries := []*LogEntry{
		{Term: 2, Command: []byte{2, 1}}, // dup 3
		{Term: 3, Command: []byte{3, 1}}, // dup 4
	}
	expected := originalLog

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = originalLog[len(originalLog)-1].Term
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      newEntries,
		LeaderCommit: 0,
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if diff := cmp.Diff(expected, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

func TestAETruncateAndAddAllNewEntries(t *testing.T) {
	// "If an existing entry conflicts with a new one (same index but
	// different terms), delete the existing entry and all that follow
	// it (5.3)"
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}}, // valid entry
		{Term: 1, Command: []byte{1, 2}}, // valid entry // prevLogIndex
		{Term: 2, Command: []byte{2, 1}}, // truncate
		{Term: 3, Command: []byte{3, 1}}, // truncate
	}
	newEntries := []*LogEntry{
		{Term: 3, Command: []byte{3, 1}}, // no dup, should add all
		{Term: 3, Command: []byte{3, 2}},
		{Term: 3, Command: []byte{3, 3}},
		{Term: 3, Command: []byte{3, 4}},
		{Term: 4, Command: []byte{4, 1}},
	}
	expected := slices.Concat(originalLog[:2], newEntries)

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = originalLog[len(originalLog)-1].Term
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      newEntries,
		LeaderCommit: 0,
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if diff := cmp.Diff(expected, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

func TestAETruncateAndOnlyAddNewEntries(t *testing.T) {
	// "If an existing entry conflicts with a new one (same index but
	// different terms), delete the existing entry and all that follow
	// it (5.3)"
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}}, // prevLogIndex
		{Term: 1, Command: []byte{1, 2}}, // valid entry at 2
		{Term: 2, Command: []byte{2, 1}}, // truncate
		{Term: 3, Command: []byte{3, 1}}, // truncate
	}
	newEntries := []*LogEntry{
		{Term: 1, Command: []byte{1, 2}}, // match valid entry at 2
		{Term: 3, Command: []byte{3, 3}}, // replace truncated
		{Term: 3, Command: []byte{3, 4}}, // replace truncated
		{Term: 4, Command: []byte{4, 1}}, // replace truncated
	}
	expected := slices.Concat(originalLog[:2], newEntries[1:])

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = originalLog[len(originalLog)-1].Term
	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      newEntries,
		LeaderCommit: 0,
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if diff := cmp.Diff(expected, raftSrv.state.log.Entries()); diff != "" {
		t.Errorf("log mismatch (-want +got):\n%s", diff)
	}
}

//
// Updating commitIndex from LeaderCommit
//

func TestAELeaderCommitUpdatesCommitIndex(t *testing.T) {
	// "If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)"
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 2, Command: []byte{2, 1}},
		{Term: 3, Command: []byte{3, 1}},
	}

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = 3
	raftSrv.state.commitIndex = 0

	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 4,
		PrevLogTerm:  3,
		Entries:      []*LogEntry{},
		LeaderCommit: 2,
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if raftSrv.state.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2", raftSrv.state.commitIndex)
	}
}

func TestAELeaderCommitCappedByLogLength(t *testing.T) {
	// "If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)"
	// When leaderCommit exceeds our log length, cap at log length.
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
	}

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = 1
	raftSrv.state.commitIndex = 0

	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []*LogEntry{},
		LeaderCommit: 5, // exceeds log length of 2
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if raftSrv.state.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2 (capped at log length)", raftSrv.state.commitIndex)
	}
}

func TestAELeaderCommitNotUpdatedWhenLower(t *testing.T) {
	// "If leaderCommit > commitIndex" -- if not greater, don't update
	originalLog := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 2, Command: []byte{2, 1}},
		{Term: 3, Command: []byte{3, 1}},
		{Term: 3, Command: []byte{3, 2}},
	}

	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{originalLog}
	raftSrv.state.currentTerm = 3
	raftSrv.state.commitIndex = 3

	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 5,
		PrevLogTerm:  3,
		Entries:      []*LogEntry{},
		LeaderCommit: 2, // lower than current commitIndex of 3
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	if raftSrv.state.commitIndex != 3 {
		t.Errorf("commitIndex = %d, want 3 (unchanged)", raftSrv.state.commitIndex)
	}
}

func TestAELeaderCommitUpdatedAfterAppendingEntries(t *testing.T) {
	// Verify commitIndex reflects log length *after* new entries
	// are appended, not before.
	raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	// Start with empty log
	raftSrv.state.commitIndex = 0

	newEntries := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 1, Command: []byte{1, 3}},
	}

	res := raftSrv.processAppendEntriesRequest(AppendEntriesReq{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      newEntries,
		LeaderCommit: 3, // commit all 3 new entries
	})

	if res.Success != true {
		t.Errorf("res.Success = %t, want true", res.Success)
	}
	// Before appending entries, min(3,0) = 0; verify that's not happened
	if raftSrv.state.commitIndex != 3 {
		t.Errorf("commitIndex = %d, want 3", raftSrv.state.commitIndex)
	}
}

//
// Log persistence
//

func TestLogPersistedToStateFile(t *testing.T) {
	stateDir := t.TempDir()
	log := []*LogEntry{
		{Term: 1, Command: []byte{1, 1}},
		{Term: 1, Command: []byte{1, 2}},
		{Term: 2, Command: []byte{2, 1}},
		{Term: 3, Command: []byte{3, 1}},
	}

	raftSrv, err := NewRaftServer(1, []ServerId{}, stateDir, &NoopStateMachine{})
	if err != nil {
		t.Fatalf("NewRaftServer failed: %v", err)
	}
	raftSrv.state.log = RaftLog{log}
	raftSrv.state.currentTerm = 3

	raftSrv.persistState()

	ps, err := LoadPersistentState(stateDir)
	if err != nil {
		t.Fatalf("LoadPersistentState failed: %v", err)
	}
	if diff := cmp.Diff(log, ps.Log); diff != "" {
		t.Errorf("persisted log mismatch (-want +got):\n%s", diff)
	}
}
