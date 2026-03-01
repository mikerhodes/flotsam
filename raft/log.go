package raft

import "fmt"

// logEntry represents a single entry in the Raft log.
type logEntry struct {
	Term    Term
	Command []byte
}

func (e *logEntry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", e.Term, e.Command)
}

// raftLog wraps a slice of log entries with 1-based indexing to match the
// Raft paper's conventions.
type raftLog struct {
	log []*logEntry
}

// get returns the entry at idx, or (nil, false) if idx is out of bounds.
func (rl *raftLog) get(idx logIndex) (*logEntry, bool) {
	if idx < 1 || int(idx) > len(rl.log) {
		return nil, false
	}

	return rl.log[idx-1], true
}

// sliceFrom returns all entries from idx to the end of the log.
// Returns the entire log if idx < 1, or an empty slice if idx is past the end.
func (rl *raftLog) sliceFrom(idx logIndex) []*logEntry {
	if idx < 1 {
		return rl.log
	}
	if int(idx) > len(rl.log) {
		return []*logEntry{}
	}
	return rl.log[idx-1:]
}

// slice returns entries in the half-open interval [s, e).
// Out-of-bounds indices are clamped to valid ranges.
func (rl raftLog) slice(s logIndex, e logIndex) []*logEntry {
	if e < 1 {
		return []*logEntry{}
	}
	if s > e {
		return []*logEntry{}
	}
	if int(s) > len(rl.log) {
		return []*logEntry{}
	}

	// clamp start and end
	s = max(s, 1)
	e = min(e, rl.lastLogIndex()+1) // +1 as slice is half-open

	// Only convert to zero-based now
	return rl.log[s-1 : e-1]
}

// truncateFrom removes all entries from idx onwards, keeping entries [1, idx).
func (rl *raftLog) truncateFrom(idx logIndex) {
	if idx < 1 {
		return
	}
	if int(idx) > len(rl.log) {
		return
	}

	rl.log = rl.log[:idx-1]
}

// append adds entries to the end of the log.
func (rl *raftLog) append(entries []*logEntry) {
	rl.log = append(rl.log, entries...)
}

// replace overwrites the entire log with entries.
func (rl *raftLog) replace(entries []*logEntry) {
	rl.log = entries
}

// entries returns the underlying slice of log entries.
func (rl *raftLog) entries() []*logEntry {
	return rl.log
}

// empty returns true if the log contains no entries.
func (rl *raftLog) empty() bool {
	return len(rl.log) == 0
}

// len returns the number of entries in the log.
func (rl *raftLog) len() int {
	return len(rl.log)
}

// lastLogIndex returns the index of the last entry, or 0 if empty.
func (rl *raftLog) lastLogIndex() logIndex {
	return logIndex(rl.len())
}

// aheadOf returns true if this log is more up-to-date than a log with the
// given last entry term and index. Used for the election restriction (5.4.1):
// a candidate's log must be at least as up-to-date to receive a vote.
func (rl *raftLog) aheadOf(term Term, idx logIndex) bool {
	if rl.empty() {
		return false
	}

	entry, _ := rl.get(logIndex(rl.len()))
	if entry.Term == term {
		return idx < logIndex(rl.len())
	}
	return term < entry.Term
}

// prevLogMatches returns true if the entry at prevIndex has term prevTerm.
// Also returns true if prevIndex is 0 and the log is empty (initial state).
func (rl *raftLog) prevLogMatches(prevIndex logIndex, prevTerm Term) bool {
	if prevIndex == 0 && rl.empty() {
		return true
	}
	entry, found := rl.get(prevIndex)
	return found && entry.Term == prevTerm
}

// findConflict scans newEntries against the log starting at startIndex.
// Returns the index of the first conflicting entry (different term), or
// (0, false) if no conflict is found or we reach the end of our log.
func (rl *raftLog) findConflict(
	startIndex logIndex,
	newEntries []*logEntry,
) (logIndex, bool) {
	for i, newEntry := range newEntries {
		logIndex := startIndex + logIndex(i)
		entry, found := rl.get(logIndex)
		if !found { // reached the end of our log
			return 0, false
		}
		if entry.Term != newEntry.Term { // found conflicting entry
			return logIndex, true
		}
	}
	return 0, false
}
