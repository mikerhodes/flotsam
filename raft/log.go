package raft

import "fmt"

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    Term
	Command []byte
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", e.Term, e.Command)
}

// RaftLog wraps a slice of log entries with 1-based indexing to match the
// Raft paper's conventions.
type RaftLog struct {
	log []*LogEntry
}

// Get returns the entry at idx, or (nil, false) if idx is out of bounds.
func (rl *RaftLog) Get(idx LogIndex) (*LogEntry, bool) {
	if idx < 1 || int(idx) > len(rl.log) {
		return nil, false
	}

	return rl.log[idx-1], true
}

// SliceFrom returns all entries from idx to the end of the log.
// Returns the entire log if idx < 1, or an empty slice if idx is past the end.
func (rl *RaftLog) SliceFrom(idx LogIndex) []*LogEntry {
	if idx < 1 {
		return rl.log
	}
	if int(idx) > len(rl.log) {
		return []*LogEntry{}
	}
	return rl.log[idx-1:]
}

// Slice returns entries in the half-open interval [s, e).
// Out-of-bounds indices are clamped to valid ranges.
func (rl RaftLog) Slice(s LogIndex, e LogIndex) []*LogEntry {
	if e < 1 {
		return []*LogEntry{}
	}
	if s > e {
		return []*LogEntry{}
	}
	if int(s) > len(rl.log) {
		return []*LogEntry{}
	}

	// clamp start and end
	s = max(s, 1)
	e = min(e, rl.LastLogIndex()+1) // +1 as slice is half-open

	// Only convert to zero-based now
	return rl.log[s-1 : e-1]
}

// TruncateFrom removes all entries from idx onwards, keeping entries [1, idx).
func (rl *RaftLog) TruncateFrom(idx LogIndex) {
	if idx < 1 {
		return
	}
	if int(idx) > len(rl.log) {
		return
	}

	rl.log = rl.log[:idx-1]
}

// Append adds entries to the end of the log.
func (rl *RaftLog) Append(entries []*LogEntry) {
	rl.log = append(rl.log, entries...)
}

// Replace overwrites the entire log with entries.
func (rl *RaftLog) Replace(entries []*LogEntry) {
	rl.log = entries
}

// Entries returns the underlying slice of log entries.
func (rl *RaftLog) Entries() []*LogEntry {
	return rl.log
}

// Empty returns true if the log contains no entries.
func (rl *RaftLog) Empty() bool {
	return len(rl.log) == 0
}

// Len returns the number of entries in the log.
func (rl *RaftLog) Len() int {
	return len(rl.log)
}

// LastLogIndex returns the index of the last entry, or 0 if empty.
func (rl *RaftLog) LastLogIndex() LogIndex {
	return LogIndex(rl.Len())
}

// AheadOf returns true if this log is more up-to-date than a log with the
// given last entry term and index. Used for the election restriction (5.4.1):
// a candidate's log must be at least as up-to-date to receive a vote.
func (rl *RaftLog) AheadOf(term Term, idx LogIndex) bool {
	if rl.Empty() {
		return false
	}

	entry, _ := rl.Get(LogIndex(rl.Len()))
	if entry.Term == term {
		return idx < LogIndex(rl.Len())
	}
	return term < entry.Term
}

// prevLogMatches returns true if the entry at prevIndex has term prevTerm.
// Also returns true if prevIndex is 0 and the log is empty (initial state).
func (rl *RaftLog) prevLogMatches(prevIndex LogIndex, prevTerm Term) bool {
	if prevIndex == 0 && rl.Empty() {
		return true
	}
	entry, found := rl.Get(prevIndex)
	return found && entry.Term == prevTerm
}

// findConflict scans newEntries against the log starting at startIndex.
// Returns the index of the first conflicting entry (different term), or
// (0, false) if no conflict is found or we reach the end of our log.
func (rl *RaftLog) findConflict(
	startIndex LogIndex,
	newEntries []*LogEntry,
) (LogIndex, bool) {
	for i, newEntry := range newEntries {
		logIndex := startIndex + LogIndex(i)
		entry, found := rl.Get(logIndex)
		if !found { // reached the end of our log
			return 0, false
		}
		if entry.Term != newEntry.Term { // found conflicting entry
			return logIndex, true
		}
	}
	return 0, false
}
