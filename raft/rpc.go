package raft

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Term int64
type ServerId int64
type LogIndex int64

// rpcOutgoingTransport is the interface implemented by the
// external transport. At a peer, these messages are
// processed by an rpcResponder.
type rpcOutgoingTransport interface {

	// makeRequestVoteRequest makes a request to peer asking for its vote
	// in a leader election, returning the peer's vote.
	makeRequestVoteRequest(
		ctx context.Context,
		peer ServerId,
		requestVote RequestVoteReq,
	) (*RequestVoteRes, error)

	// makeHeartbeatRequest makes a heartbeat request to a peer, returning
	// the result.
	makeHeartbeatRequest(
		ctx context.Context,
		peer ServerId,
		appendEntries AppendEntriesReq,
	) (*AppendEntriesRes, error)
}

// noVote is a sentinel server ID for state.votedFor, representing
// no candidate has been voted on this term.
const noVote = -1

// ElectionResult represents whether this node won an election.
// True if it did, false if it didn't.
type ElectionResult bool

type RaftRole int

const (
	RaftRoleFollower  RaftRole = 1
	RaftRoleCandidate RaftRole = 2
	RaftRoleLeader    RaftRole = 3
)

func (r RaftRole) String() string {
	switch r {
	case RaftRoleFollower:
		return "RaftRoleFollower"
	case RaftRoleCandidate:
		return "RaftRoleCandidate"
	case RaftRoleLeader:
		return "RaftRoleLeader"
	default:
		return fmt.Sprintf("RaftRole(%d)", r)
	}
}

// Election deadlines set to now + (electionTimeoutBase + rand(electionPerturbation))
const electionTimeoutBase = 150 * time.Millisecond
const electionPerturbation = 150 * time.Millisecond
const heartbeatDuration = electionPerturbation / 2

type LogEntry struct {
	Term    Term
	Command []byte
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", e.Term, e.Command)
}

// RaftLog is a log indexed from 1 rather than 0
// to allow us to more closely match the paper.
type RaftLog struct {
	log []*LogEntry
}

func (rl *RaftLog) Get(idx LogIndex) (*LogEntry, bool) {
	if idx < 1 {
		return nil, false
	}
	if int(idx) > len(rl.log) {
		return nil, false
	}

	return rl.log[idx-1], true
}

func (rl *RaftLog) SliceFrom(idx LogIndex) []*LogEntry {
	if idx < 1 {
		return rl.log
	}
	if int(idx) > len(rl.log) {
		return []*LogEntry{}
	}
	return rl.log[idx-1:]
}

func (rl *RaftLog) Truncate(idx LogIndex) {
	if idx < 1 {
		return
	}
	if int(idx) > len(rl.log) {
		return
	}

	rl.log = rl.log[:idx-1]
}

func (rl *RaftLog) Append(entries []*LogEntry) {
	rl.log = append(rl.log, entries...)
}
func (rl *RaftLog) Replace(entries []*LogEntry) {
	rl.log = entries
}
func (rl *RaftLog) Entries() []*LogEntry {
	return rl.log
}
func (rl *RaftLog) Empty() bool {
	return len(rl.log) == 0
}
func (rl *RaftLog) Len() int {
	return len(rl.log)
}

// LastLogIndex returns the last log index.
func (l *RaftLog) LastLogIndex() LogIndex {
	return LogIndex(l.Len())
}

// AheadOf returns true if this log is ahead of the passed term and index.
// State lock must be held when calling this method
func (l *RaftLog) AheadOf(term Term, idx LogIndex) bool {
	if l.Empty() {
		return false
	}

	entry, _ := l.Get(LogIndex(l.Len()))

	if entry.Term == term {
		return idx < LogIndex(l.Len())
	} else {
		return term < entry.Term
	}
}

type AppendEntriesReq struct {
	Term         Term
	LeaderId     ServerId
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []*LogEntry
	LeaderCommit LogIndex
}
type AppendEntriesRes struct {
	Term    Term
	Success bool
}

type RequestVoteReq struct {
	Term         Term
	CandidateId  ServerId
	LastLogIndex LogIndex
	LastLogTerm  Term
}
type RequestVoteRes struct {
	Term        Term
	VoteGranted bool
}

// ClientCommandReq and ClientCommandRes are used to receive
// and respond to client commands.
type ClientCommandReq struct {
	Command []byte
}
type ClientCommandRes struct {
	Success bool
	Err     error
}

type state struct {
	sync.Mutex

	// Raft role
	role RaftRole

	// Persistent state
	currentTerm Term
	votedFor    ServerId
	log         RaftLog

	// Volatile state
	commitIndex LogIndex
	lastApplied LogIndex

	// Leader volatile state
	nextIndex  map[ServerId]LogIndex
	matchIndex map[ServerId]LogIndex

	electionDeadline time.Time
	nextHeartbeat    time.Time

	peers []ServerId
}

// persistentState loads and saves raft persistent state
type persistentState struct {
	CurrentTerm Term
	VotedFor    ServerId
	Log         []*LogEntry
}

func LoadPersistentState(stateDir string) (persistentState, error) {
	ps := persistentState{}
	data, err := os.ReadFile(filepath.Join(stateDir, "raft.json"))
	if err != nil {
		return ps, fmt.Errorf("loading state: %w", err)
	}
	err = json.Unmarshal(data, &ps)
	if err != nil {
		return ps, fmt.Errorf("loading state: %w", err)
	}
	return ps, nil
}

func (ps *persistentState) Save(stateDir string) error {
	data, err := json.Marshal(ps)
	if err != nil {
		return fmt.Errorf("saving state: %w", err)
	}

	// This isn't super safe (we should write + move)
	err = os.WriteFile(filepath.Join(stateDir, "raft.json"), data, 0666)
	if err != nil {
		return fmt.Errorf("saving state: %w", err)
	}

	return nil
}

type RaftServer struct {
	state *state

	// stateDir is the storage location for persistent state
	stateDir string

	// serverId is read-only server ID
	serverId ServerId

	// transport must be assigned before calling Start
	transport rpcOutgoingTransport

	// stop closes when we should stop
	stop chan struct{}
	// exited closes when raft ticker loop exits
	exited chan struct{}
}

func NewRaftServer(serverId ServerId, peers []ServerId, stateDir string) (*RaftServer, error) {
	if serverId < 0 {
		return nil, errors.New("serverId must be >0")
	}
	electionDeadline := newElectionDeadline()
	nextHeartbeat := nextHeartbeat()
	return &RaftServer{
		state: &state{
			role:             RaftRoleFollower,
			currentTerm:      0,
			votedFor:         noVote,
			log:              RaftLog{[]*LogEntry{}},
			commitIndex:      0,
			lastApplied:      0,
			nextIndex:        map[ServerId]LogIndex{},
			matchIndex:       map[ServerId]LogIndex{},
			peers:            peers,
			electionDeadline: electionDeadline,
			nextHeartbeat:    nextHeartbeat,
		},
		stateDir: stateDir,
		serverId: serverId,
		stop:     make(chan struct{}),
		exited:   make(chan struct{}),
	}, nil
}

func nextHeartbeat() time.Time {
	nextHeartbeat := time.Now().Add(50 * time.Millisecond)
	return nextHeartbeat
}

func newElectionDeadline() time.Time {
	perturbMs, _ := rand.Int(rand.Reader, big.NewInt(electionPerturbation.Milliseconds()))
	deadline := time.Now().Add(electionTimeoutBase)
	deadline = deadline.Add(time.Duration(perturbMs.Int64()) * time.Millisecond)
	return deadline
}

// Start starts the Raft server, and blocks until it is shutdown.
func (r *RaftServer) Start() {
	// Load persistent state
	if ps, err := LoadPersistentState(r.stateDir); err == nil {
		r.state.Lock()
		r.state.currentTerm = ps.CurrentTerm
		r.state.votedFor = ps.VotedFor
		r.state.log.Replace(ps.Log)
		r.state.Unlock()
	}

	// We take actions for every tick. Requests from other servers
	// are processed in callbacks from the transport.
	const checkInterval = 25 * time.Millisecond
	t := time.NewTicker(checkInterval)
	for {
		select {
		case <-r.stop:
			close(r.exited)
			return
		case <-t.C:
			r.maybeStartElection()
			r.maybeSendHeartbeats()
		}
	}
}

func (r *RaftServer) Shutdown() {
	close(r.stop)
	<-r.exited
}

// persistState saves the raft persistent state to disk.
// Call assumes state lock is held.
func (r *RaftServer) persistState() {
	ps := persistentState{
		CurrentTerm: r.state.currentTerm,
		VotedFor:    r.state.votedFor,
		Log:         r.state.log.Entries(),
	}
	err := ps.Save(r.stateDir)
	if err != nil {
		panic(fmt.Sprintf("Error saving state; panic: %v", err))
	}
}

// Role returns the current Raft role of this server.
func (r *RaftServer) Role() RaftRole {
	r.state.Lock()
	defer r.state.Unlock()
	return r.state.role
}

// CurrentTerm returns the current term of this server.
func (r *RaftServer) CurrentTerm() Term {
	r.state.Lock()
	defer r.state.Unlock()
	return r.state.currentTerm
}

// becomeFollower makes us a follower and advances to term.
// Caller must hold state lock.
func (r *RaftServer) becomeFollower(term Term) {
	r.state.currentTerm = term
	r.state.role = RaftRoleFollower
	r.state.votedFor = noVote
	r.state.electionDeadline = newElectionDeadline()
	log.Printf("[%d] became follower, term=%d", r.serverId, r.state.currentTerm)
}

// becomeLeader makes us a leader for the current term
// Caller must hold state lock.
func (r *RaftServer) becomeLeader() {
	r.state.role = RaftRoleLeader
	r.state.nextIndex = map[ServerId]LogIndex{}
	r.state.matchIndex = map[ServerId]LogIndex{}
	for _, peer := range r.state.peers {
		r.state.nextIndex[peer] = LogIndex(r.state.log.Len() + 1)
		r.state.matchIndex[peer] = 0
	}
	r.state.nextHeartbeat = nextHeartbeat()
	log.Printf("[%d] became leader, term=%d", r.serverId, r.state.currentTerm)
}

func (r *RaftServer) processAppendEntriesRequest(appendEntries AppendEntriesReq) *AppendEntriesRes {
	r.state.Lock()
	defer r.state.Unlock()
	defer r.persistState()

	ct := r.state.currentTerm

	// Out of date request
	if appendEntries.Term < ct {
		return &AppendEntriesRes{Term: ct, Success: false}
	}

	// Request is for current term, bump election deadline
	r.state.electionDeadline = newElectionDeadline()

	// If we receive an AppendEntries with Term >= currentTerm,
	// we are definitely not the leader for this term.
	if appendEntries.Term > ct {
		r.becomeFollower(appendEntries.Term)
	}
	if appendEntries.Term == ct {
		// While raft guarantees we can't have two leaders for
		// a term, if we are a candidate we should become
		// a follower if someone else won.
		if r.state.role != RaftRoleFollower {
			r.becomeFollower(appendEntries.Term)
		}
	}

	ct = r.state.currentTerm

	// If there are entries, entry at prevLogIndex must match term
	// prevLogTerm (5.3)
	if !(appendEntries.PrevLogIndex == 0 && r.state.log.Empty()) {
		entry, found := r.state.log.Get(appendEntries.PrevLogIndex)
		if !found {
			log.Printf("prevLogIndex not found, idx=%d, len(log)=%d",
				appendEntries.PrevLogIndex, len(r.state.log.Entries()))
			return &AppendEntriesRes{Term: ct, Success: false}
		}
		if entry.Term != appendEntries.PrevLogTerm {
			log.Printf("prevLogTerm mismatch, prevLogTerm=%d, entry.Term=%d",
				appendEntries.PrevLogTerm, entry.Term)
			return &AppendEntriesRes{Term: ct, Success: false}
		}
	}

	// Truncate if we find a mismatched entry (5.3)
	// Raft guarantees that an index+term pair will
	// always contain the same command. To enforce this,
	// we have to drop "conflicting" log entries when
	// we get a set of new entries that properly extends
	// our log from prevLogIndex. We find the first
	// entry (if any) that doesn't match, and drop that
	// entry and all following ones.
	// It'll look something like this (number is term):
	//   log      new entries
	//    5            (=prevLogIndex/=prevLogTerm)
	//    5          5 (same term - okay)
	//    5          5 (same term - okay)
	//    5          6 (mismatching term, discard our log)
	//    5          6
	//    5            (we could even have a longer local log)
	logStartIdx := appendEntries.PrevLogIndex + 1
	log.Printf("Starting truncate and dedup at logStartIdx=%d", logStartIdx)
	log.Printf("Log: %v", r.state.log.Entries())
	appendEntriesIdx := 0
	for {
		logIndex := logStartIdx + LogIndex(appendEntriesIdx)
		entry, found := r.state.log.Get(logIndex)
		if !found { // reached end of our log
			log.Printf("Truncate reached end of log at logIndex=%d", logIndex)
			break
		}
		if appendEntriesIdx >= len(appendEntries.Entries) {
			log.Printf(
				"Truncate reached end of Entries appendEntriesIdx=%d",
				appendEntriesIdx)
			break
		}
		if entry.Term != appendEntries.Entries[appendEntriesIdx].Term {
			log.Printf("Truncating from %d", logIndex)
			r.state.log.Truncate(logIndex)
			break
		}
		appendEntriesIdx++
	}

	// Append any new entries not already in the log. First,
	// skip entries from req that are already in our log.
	// After truncation, we are guaranteed that the new
	// entries either go at the end of our current log,
	// or that they have matching entries overlapping.
	// We just need to figure out the length of the
	// overlap. It'll look something like this (number is
	// log index, assume terms match due to truncation):
	//   log      new entries
	//    5            (=prevLogIndex)
	//    6          6 (=prevLogIndex + 1) // discard new
	//    7          7                     // discard new
	//               8                     // append
	//               9                     // append
	overlap := r.state.log.Len() - int(appendEntries.PrevLogIndex)
	newEntries := appendEntries.Entries[overlap:]
	log.Printf("newEntries=%s", newEntries)
	r.state.log.Append(newEntries)

	if appendEntries.LeaderCommit > r.state.commitIndex {
		r.state.commitIndex = min(
			appendEntries.LeaderCommit, r.state.log.LastLogIndex(),
		)
	}

	return &AppendEntriesRes{Term: r.state.currentTerm, Success: true}
}

// processClientCommand adds command to log, and blocks until a
// majority of peers have the command replicated to their log.
func (r *RaftServer) processClientCommand(
	command ClientCommandReq,
) *ClientCommandRes {
	// Add entry to our log while holding the lock.
	// Then try to apply to all followers.
	r.state.Lock()

	if r.state.role != RaftRoleLeader {
		r.state.Unlock()
		return &ClientCommandRes{
			Success: false,
			Err:     errors.New("Client commands only accepted by leader"),
		}
	}

	newEntries := []*LogEntry{{
		Term:    r.state.currentTerm,
		Command: command.Command,
	}}
	r.state.log.Append(newEntries)

	peers := slices.Clone(r.state.peers)

	r.persistState()
	r.state.Unlock()

	// If we have peers, we need to wait for a majority to
	// accept before returning.
	if len(peers) > 0 {
		success := atomic.Int32{}
		successRequired := len(peers)/2 + 1
		done := make(chan struct{})
		for _, peer := range peers {
			go func() {
				r.catchUpPeer(peer)

				log.Printf("[%d] Successfully replicated to %d", r.serverId, peer)
				if int(success.Add(1)) >= successRequired {
					close(done)
				}
			}()
		}

		<-done
	}

	return &ClientCommandRes{
		Success: true,
		Err:     nil,
	}
}

// catchUpPeer tries to catch up a peer, blocking until the peer
// is caught up.
// Do not call while holding state lock.
func (r *RaftServer) catchUpPeer(
	peer ServerId,
) {

	for {
		r.state.Lock()
		if r.state.role != RaftRoleLeader {
			// no longer leader
			r.state.Unlock()
			return
		}
		if r.state.log.LastLogIndex() < r.state.nextIndex[peer] {
			// Caught up
			r.state.Unlock()
			return
		}
		r.state.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), heartbeatDuration)
		currentAEReq, newNextIndex := r.nextAEReqForPeer(peer)
		res, err := r.transport.makeHeartbeatRequest(
			ctx, peer, currentAEReq,
		)
		cancel()
		if err != nil {
			// Assume err means network error, don't decrement nextIndex
			continue
		}
		if !res.Success {
			// Assume !Success means log inconsistency - decrement nextIndex (5.3)
			r.state.Lock()
			r.state.nextIndex[peer] = max(0, r.state.nextIndex[peer]-1)
			r.state.Unlock()
			continue
		}

		// Successfully caught up, break loop
		r.state.Lock()
		r.state.nextIndex[peer] = newNextIndex
		r.state.matchIndex[peer] = newNextIndex
		r.state.Unlock()
	}
}

// nextAEReqForPeer generates an AppendEntries request for the peer
// that uses the nextIndex state to send the appropriate log entries.
// Also returns the logIndex to advance peer to if request succeeds.
// Do not call while holding the state lock.
func (r *RaftServer) nextAEReqForPeer(peer ServerId) (AppendEntriesReq, LogIndex) {
	r.state.Lock()
	defer r.state.Unlock()

	peerNextIndex := r.state.nextIndex[peer]
	prevLogIndex := max(0, peerNextIndex-1)
	prevLogTerm := Term(0)
	if entry, found := r.state.log.Get(prevLogIndex); found {
		prevLogTerm = entry.Term
	}
	appendEntriesReq := AppendEntriesReq{
		Term:         r.state.currentTerm,
		LeaderId:     r.serverId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      r.state.log.SliceFrom(peerNextIndex),
		LeaderCommit: r.state.commitIndex,
	}

	// If successful, next index for peer should be just beyond current log
	return appendEntriesReq, LogIndex(r.state.log.Len() + 1)
}

func (r *RaftServer) processRequestVote(requestVote RequestVoteReq) *RequestVoteRes {
	r.state.Lock()
	defer r.state.Unlock()
	defer r.persistState()

	ct := r.state.currentTerm

	// Out of date request
	if requestVote.Term < ct {
		log.Printf("[%d] processRequestVote LOWER_TERM electionTerm=%d candidate=%d", r.serverId, requestVote.Term, requestVote.CandidateId)
		return &RequestVoteRes{Term: ct, VoteGranted: false}
	}

	// If we are the leader for this term, we need to update the calling
	// candidate with that fact.
	if r.state.role == RaftRoleLeader && requestVote.Term == ct {
		log.Printf("[%d] processRequestVote I_AM_LEADER electionTerm=%d candidate=%d", r.serverId, requestVote.Term, requestVote.CandidateId)
		return &RequestVoteRes{Term: ct, VoteGranted: false}
	}

	// Vote is for a newer term, move ourselves to that term and
	// become a follower before processing. (section 5.1)
	if requestVote.Term > ct {
		r.becomeFollower(requestVote.Term)
	}
	ct = r.state.currentTerm

	// Only grant one vote per term (section 5.2)
	if r.state.votedFor != noVote && r.state.votedFor != requestVote.CandidateId {
		log.Printf("[%d] processRequestVote ALREADY_VOTED_SOMEONE_ELSE electionTerm=%d candidate=%d", r.serverId, requestVote.Term, requestVote.CandidateId)
		return &RequestVoteRes{Term: ct, VoteGranted: false}
	}

	// Section 5.4.1 safety property - election restriction
	// A candidate should not win an election unless its log contains
	// all committed entries.
	if r.state.log.AheadOf(requestVote.LastLogTerm, requestVote.LastLogIndex) {
		log.Printf(
			"[%d] processRequestVote TOO_OLD_LOG_TERM LastLogTerm=%d",
			r.serverId, requestVote.LastLogTerm)
		return &RequestVoteRes{Term: ct, VoteGranted: false}
	}

	log.Printf("[%d] processRequestVote GRANTING_VOTE electionTerm=%d candidate=%d", r.serverId, requestVote.Term, requestVote.CandidateId)
	r.state.votedFor = requestVote.CandidateId
	return &RequestVoteRes{Term: ct, VoteGranted: true}
}

// maybeStartElection checks whether we are in the right state
// to start an election --- not the leader, with an elapsed
// election deadline --- and if so starts an election in a new
// goroutine, so this method returns before the election is
// complete.
func (r *RaftServer) maybeStartElection() {
	r.state.Lock()
	defer r.state.Unlock()
	defer r.persistState()

	if r.state.role == RaftRoleLeader {
		return
	}
	if time.Now().Before(r.state.electionDeadline) {
		return
	}

	// Transition to candidate
	r.state.role = RaftRoleCandidate
	r.state.currentTerm += 1
	r.state.votedFor = r.serverId
	log.Printf("%d became candidate, term=%d", r.serverId, r.state.currentTerm)

	peers := slices.Clone(r.state.peers)
	electionTerm := r.state.currentTerm

	newDeadline := newElectionDeadline()
	go func() {
		ctx, cancel := context.WithDeadline(context.Background(), newDeadline)
		defer cancel()
		r.runElection(ctx, electionTerm, peers)
	}()
	r.state.electionDeadline = newDeadline
}

// runElection runs a single election. We exit this function either as
// follower, leader or still as a candidate. In the latter state, we
// run another election.
func (r *RaftServer) runElection(ctx context.Context, electionTerm Term, peers []ServerId) {
	log.Printf("[%d] runElection term=%d", r.serverId, electionTerm)

	lastLogTerm := Term(0)
	entry, found := r.state.log.Get(r.state.log.LastLogIndex())
	if found {
		lastLogTerm = entry.Term
	}

	voteReq := RequestVoteReq{
		Term:         electionTerm,
		CandidateId:  r.serverId,
		LastLogIndex: r.state.log.LastLogIndex(),
		LastLogTerm:  lastLogTerm,
	}
	won := r.collectVotes(ctx, electionTerm, peers, voteReq)

	r.state.Lock()
	defer r.state.Unlock()
	defer r.persistState()

	// No longer a candidate
	if r.state.role != RaftRoleCandidate {
		return
	}

	// If time has moved on, discard the result of the election
	if r.state.currentTerm > electionTerm {
		return
	}

	// A bug, this should never happen, panic
	if r.state.currentTerm < electionTerm {
		panic("Term went backwards, this is a bug")
	}

	// Section 5.2 Once a candidate wins an election, it becomes leader.
	// It then sends heartbeat messages to all of the other servers to
	// establish its authority and prevent new elections.
	if won {
		r.becomeLeader()
		go r.sendLeaderHeartbeats()
	}
}

// collectVotes sends peers vote requests, and returns true if this
// node received a majority of votes.
func (r *RaftServer) collectVotes(ctx context.Context, electionTerm Term, peers []ServerId, reqVoteReq RequestVoteReq) ElectionResult {
	votes := make(chan bool, len(peers))
	votesForMe := 1 // vote for self
	votesRequired := len(peers)/2 + 1

	// Send RequestVote RPCs, ensuring timeout using ctx.
	for _, peer := range peers {
		go func() {
			peerResult, err := r.transport.makeRequestVoteRequest(ctx, peer, reqVoteReq)
			if err != nil {
				log.Printf("Error requesting vote from %d; assuming false vote: %v", peer, err)
				votes <- false
				return
			}

			r.state.Lock()
			defer r.state.Unlock()
			defer r.persistState()

			log.Printf(
				"[%d] collectVotes received vote electionTerm=%d, peerResult=%v",
				r.serverId, electionTerm, peerResult)

			if peerResult.Term > electionTerm {
				// Time has moved on and a new leader has arisen. Accept
				// this and become a follower.
				r.becomeFollower(peerResult.Term)
				votes <- false
			} else if peerResult.Term < electionTerm {
				// A result from the past, discard it.
				votes <- false
			} else {
				// Peer is still in the same term, use its result.
				votes <- peerResult.VoteGranted
			}
		}()
	}

	// Wait for enough responses to declare victory, or timeout,
	// or we receive all responses but not enough to win.
	for range len(peers) {
		v := <-votes
		log.Printf(
			"[%d] collectVotes received vote electionTerm=%d, vote=%t",
			r.serverId, electionTerm, v)
		if v {
			votesForMe += 1
		}
		if votesForMe >= votesRequired {
			break
		}
	}

	// Checking this after the votes are received ensures that
	// we become leader in the (unusual) case that there are
	// zero peers.
	return votesForMe >= votesRequired
}

// maybeSendHeartbeats checks whether we are in the right state to
// sent heartbeats --- we are the leader, and the next heartbeat
// time has passed --- and starts a goroutine to send the heartbeats,
// meaning that this method will return before heartbeats are complete.
func (r *RaftServer) maybeSendHeartbeats() {
	r.state.Lock()
	defer r.state.Unlock()

	if r.state.role != RaftRoleLeader {
		return
	}
	if time.Now().Before(r.state.nextHeartbeat) {
		return
	}

	go r.sendLeaderHeartbeats()
	r.state.nextHeartbeat = nextHeartbeat()
}

// sendLeaderHeartbeats sends a heartbeat to all peers, blocking on
// until complete.
func (r *RaftServer) sendLeaderHeartbeats() {
	r.state.Lock()
	appendEntriesReq := AppendEntriesReq{
		Term:         r.state.currentTerm,
		LeaderId:     r.serverId,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{},
		LeaderCommit: 0,
	}
	r.state.Unlock()

	// ctx sets timeout for all requests
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatDuration)
	defer cancel()
	wg := sync.WaitGroup{}
	for _, peer := range r.state.peers {
		wg.Go(func() {
			result, err := r.transport.makeHeartbeatRequest(ctx, peer, appendEntriesReq)
			if err != nil {
				log.Printf("Error sending heartbeat to %v: %v", peer, err)
				return
			}

			// Make any state updates based on the response.
			r.state.Lock()
			defer r.state.Unlock()
			defer r.persistState()

			// Time has moved on, and another leader has likely
			// arisen. Accept this, and become a follower. (section 5.1)
			if result.Term > r.state.currentTerm {
				r.becomeFollower(result.Term)
			}
		})
	}
	wg.Wait()
}
