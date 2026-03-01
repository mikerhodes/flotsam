package raft

import (
	"slices"
	"testing"
)

func Test_maybeUpdatedCommitIndex(t *testing.T) {
	// genLog returns a log that ends at term t with length l
	genLog := func(t Term, l int) *raftLog {
		return &raftLog{
			log: slices.Repeat([]*logEntry{{Term: t, Command: []byte{}}}, l),
		}
	}
	// term is used for the term in all tests
	term := Term(123)

	// The commit index should only advance if:
	// 1. A majority of servers have >currentCommitIndex in their logs
	//       ie, raftLog.Len() for this server, matchIndex for peers
	// 2. commitIndex cannot go backwards.
	// 3. raftLog for this server ends with entry matching current term
	tests := []struct {
		name               string
		currentCommitIndex logIndex
		matchIndex         map[ServerId]logIndex
		raftLog            *raftLog
		wantCommitIndex    logIndex
	}{
		{
			name:               "lower majority does not advance",
			currentCommitIndex: 3,
			matchIndex:         map[ServerId]logIndex{1: 2, 2: 1},
			raftLog:            genLog(term, 5),
			wantCommitIndex:    3,
		}, {
			name:               "majority reached furthest log index advances",
			currentCommitIndex: 1,
			matchIndex:         map[ServerId]logIndex{1: 3, 2: 5},
			raftLog:            genLog(term, 5),
			wantCommitIndex:    5,
		}, {
			name:               "majority higher than current index advances",
			currentCommitIndex: 1,
			matchIndex:         map[ServerId]logIndex{1: 3, 2: 4},
			raftLog:            genLog(term, 5), // higher than durable elsewhere
			wantCommitIndex:    4,
		}, {
			name:               "lower N than current index does not advance",
			currentCommitIndex: 4,
			matchIndex:         map[ServerId]logIndex{1: 3, 2: 3}, // 3 < 4
			raftLog:            genLog(term, 5),
			wantCommitIndex:    4,
		}, {
			name:               "old term for log[N] does not advance",
			currentCommitIndex: 1,
			matchIndex:         map[ServerId]logIndex{1: 5, 2: 5},
			raftLog:            genLog(term-12, 5), // local log with older term
			wantCommitIndex:    1,
		}, {
			name:               "newer term for log[N] does not advance",
			currentCommitIndex: 1,
			matchIndex:         map[ServerId]logIndex{1: 5, 2: 5},
			raftLog:            genLog(term+12, 5), // local log with newer term
			wantCommitIndex:    1,
		}, {
			// Raft should guarantee this never happens
			name:               "other servers beyond log len does not advance",
			currentCommitIndex: 1,
			matchIndex:         map[ServerId]logIndex{1: 100, 2: 100},
			raftLog:            genLog(term, 5), // much shorter than peer logs
			wantCommitIndex:    1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateUpdatedCommitIndex(
				0, term, tt.currentCommitIndex, tt.matchIndex, tt.raftLog,
			)
			if got != tt.wantCommitIndex {
				t.Errorf("maybeUpdatedCommitIndex() = %v, want %v", got, tt.wantCommitIndex)
			}
		})
	}
}
