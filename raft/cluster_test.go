package raft

import (
	"testing"
	"time"
)

func TestSingleServerBecomesLeader(t *testing.T) {
	harness := NewHarness(t, 1)
	harness.StartAll()

	// Wait for election timeout (150-300ms) plus some buffer
	time.Sleep(350 * time.Millisecond)

	// Server should be the leader, because there is only one
	// server, so by voting for itself it is in the majority.
	for raft := range harness.Servers() {
		role := raft.Role()
		// "all" of the one servers should be leader
		if role != RaftRoleLeader {
			t.Errorf("[%d] srv.Role() = %s, want RaftRoleLeader",
				raft.serverId, role)
		}
	}

	harness.ShutdownAll()
}

func TestThreeServersOneLeaderAtStartup(t *testing.T) {
	harness := NewHarness(t, 3)
	harness.StartAll()

	// A leader should emerge
	harness.WaitForOneLeader(t)

	harness.ShutdownAll()
}

func TestThreeServersPersistTerm(t *testing.T) {
	harness := NewHarness(t, 3)
	harness.StartAll()

	// Wait for leader
	harness.WaitForOneLeader(t)

	// Get the leader's term before shutdown
	var leaderTerm Term
	for raft := range harness.Servers() {
		if raft.Role() == RaftRoleLeader {
			leaderTerm = raft.CurrentTerm()
			break
		}
	}

	// Shutdown all servers to ensure state is persisted
	harness.ShutdownAll()

	// Check state for each server
	// They should all be on the leader term and at least
	// two should have voted during the term.
	nVoted := 0
	for id, ps := range harness.PersistentStates(t) {
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
	harness := NewHarness(t, 3)
	harness.StartAll()

	// Two servers should be followers, one should be leader
	// Save the leader ID as we will kill it.
	harness.WaitForOneLeader(t)
	var firstLeaderId ServerId
	for raft := range harness.Servers() {
		if raft.Role() == RaftRoleLeader {
			firstLeaderId = raft.serverId
			break
		}
	}

	// Kill the current leader
	harness.Shutdown(firstLeaderId)
	t.Logf("KILLED THE LEADER %d", firstLeaderId)

	// Wait for a leader to emerge
	harness.WaitForOneLeader(t)
	var secondLeaderId ServerId
	for raft := range harness.Servers() {
		if raft.Role() == RaftRoleLeader {
			secondLeaderId = raft.serverId
			break
		}
	}

	// Restart the first leader -- it should end up a follower
	err := harness.Restart(firstLeaderId)
	if err != nil {
		t.Fatalf("Error restarting server: %v", err)
	}
	t.Logf("RESTARTED SERVER AT %d", firstLeaderId)

	// Leader should not change, we shouldn't have an election
	harness.WaitForOneLeader(t)
	var thirdLeaderId ServerId
	for raft := range harness.Servers() {
		if raft.Role() == RaftRoleLeader {
			thirdLeaderId = raft.serverId
			break
		}
	}
	if thirdLeaderId != secondLeaderId {
		t.Errorf("thirdLeaderId = %d, want %d", thirdLeaderId, secondLeaderId)
	}

	// Clean up
	harness.ShutdownAll()
}
