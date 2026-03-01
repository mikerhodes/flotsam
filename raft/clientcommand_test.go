package raft

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"
)

//
// HELPERS
//

// CountingStateMachine counts the number of applies
type CountingStateMachine struct {
	count atomic.Int32
}

// apply implements StateMachine
func (sm *CountingStateMachine) apply(_ []byte) error {
	sm.count.Add(1)
	return nil
}

// countingAETransport counts the number of AppendEntries requests
// sent over the transport, otherwise returning true.
type countingAETransport struct {
	sync.Mutex
	receivedAERequests int
}

// makeRequestVoteRequest returns a hardcoded vote.
func (r *countingAETransport) makeRequestVoteRequest(
	_ context.Context, _ ServerId, requestVote requestVoteReq,
) (*requestVoteRes, error) {
	return &requestVoteRes{Term: requestVote.Term, VoteGranted: false}, nil
}

// makeHeartbeatRequest counts sent AE requests
func (r *countingAETransport) makeHeartbeatRequest(
	_ context.Context, _ ServerId, appendEntries appendEntriesReq,
) (*appendEntriesRes, error) {
	r.Lock()
	defer r.Unlock()
	r.receivedAERequests++
	return &appendEntriesRes{Term: appendEntries.Term, Success: true}, nil
}

//
// BEGIN TESTS
//

func TestClientCommandAcceptance(t *testing.T) {
	tests := []struct {
		name         string
		role         RaftRole
		wantSuccess  bool
		wantSMWrites int32
	}{
		{"candidate rejects", RaftRoleCandidate, false, 0},
		{"follower rejects", RaftRoleFollower, false, 0},
		{"leader accepts", RaftRoleLeader, true, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			countingSM := &CountingStateMachine{}
			raftSrv, err := NewRaftServer(1, []ServerId{}, t.TempDir(), countingSM)
			if err != nil {
				t.Fatalf("NewRaftServer failed: %v", err)
			}
			raftSrv.state.role = tt.role

			res := raftSrv.processClientCommand(ClientCommandReq{
				Command: []byte{1, 2, 3},
			})

			if res.Success != tt.wantSuccess {
				t.Errorf("res.Success = %t, want %t", res.Success, tt.wantSuccess)
			}
			if tt.wantSuccess != (res.Err == nil) {
				t.Errorf("res.Err = %v, wantErr %t", res.Err, !tt.wantSuccess)
			}
			if countingSM.count.Load() != tt.wantSMWrites {
				t.Errorf("counter.count = %d, want %d", countingSM.count.Load(), tt.wantSMWrites)
			}
		})
	}
}

func TestWritingSingleCommandToPeers(t *testing.T) {

	nPeers := 2

	synctest.Test(t, func(t *testing.T) {
		peers := make([]ServerId, nPeers)
		for i := range peers {
			peers[i] = ServerId(i)
		}

		mockC := &countingAETransport{}
		countingSM := &CountingStateMachine{}

		raftSrv, err := NewRaftServer(1, peers, t.TempDir(), countingSM)
		if err != nil {
			t.Fatalf("NewRaftServer failed: %v", err)
		}
		raftSrv.state.role = RaftRoleLeader
		raftSrv.state.currentTerm = 1
		raftSrv.transport = mockC

		raftSrv.processClientCommand(ClientCommandReq{
			Command: []byte{1, 2, 3},
		})

		synctest.Wait()

		// Assert leader sent RPCs to its mock peers and that it
		// has the new entry in its log and has advanced the commitIndex
		if mockC.receivedAERequests != nPeers {
			t.Errorf("mockC.receivedAERequests = %d, want %d",
				mockC.receivedAERequests, nPeers)
		}
		if diff := cmp.Diff([]*logEntry{{
			Term:    1,
			Command: []byte{1, 2, 3},
		}}, raftSrv.state.log.log); diff != "" {
			t.Errorf("log mismatch (-want +got):\n%s", diff)
		}
		if raftSrv.state.commitIndex != 1 {
			t.Errorf("commitIndex == %d, wanted 1", raftSrv.state.commitIndex)
		}
		if countingSM.count.Load() != 1 {
			t.Errorf("counter.count = %d, want %d", countingSM.count.Load(), 1)
		}
	})
}

// TestWritingManyCommands writes many commands to the raft server
// and checks the peers receive requests for them, and that we
// apply them to the state machine.
func TestWritingManyCommandsToPeers(t *testing.T) {

	nPeers := 2
	var writes int32 = 23

	synctest.Test(t, func(t *testing.T) {
		peers := make([]ServerId, nPeers)
		for i := range peers {
			peers[i] = ServerId(i)
		}

		mockC := &countingAETransport{}
		countingSM := &CountingStateMachine{}

		raftSrv, err := NewRaftServer(1, peers, t.TempDir(), countingSM)
		if err != nil {
			t.Fatalf("NewRaftServer failed: %v", err)
		}
		raftSrv.state.role = RaftRoleLeader
		raftSrv.state.currentTerm = 1
		raftSrv.transport = mockC

		for range writes {
			raftSrv.processClientCommand(ClientCommandReq{
				Command: []byte{1},
			})
			synctest.Wait()
		}

		// Assert leader sent RPCs to its mock peers and that it
		// has the new entry in its log and has advanced the commitIndex
		if mockC.receivedAERequests != nPeers*int(writes) {
			t.Errorf("mockC.receivedAERequests = %d, want %d",
				mockC.receivedAERequests, nPeers*int(writes))
		}
		if diff := cmp.Diff(
			slices.Repeat([]*logEntry{{Term: 1, Command: []byte{1}}}, int(writes)),
			raftSrv.state.log.log); diff != "" {
			t.Errorf("log mismatch (-want +got):\n%s", diff)
		}
		if raftSrv.state.commitIndex != logIndex(writes) {
			t.Errorf("commitIndex == %d, wanted %d", raftSrv.state.commitIndex, writes)
		}
		if countingSM.count.Load() != writes {
			t.Errorf("counter.count = %d, want %d", countingSM.count.Load(), writes)
		}
	})
}
