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

// StateMachine
type StateMachine interface {
	// apply applies command to this state machine
	apply(command []byte) error
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

	// stateMachine is the external state machine
	stateMachine StateMachine

	// transport must be assigned before calling Start
	transport rpcOutgoingTransport

	// inShutdown is true when server is shutting down,
	// and prevents new calls from starting.
	inShutdown atomic.Bool

	// inFlightReqs stores the number of inflight requests.
	inFlightReqs atomic.Int64

	// stop closes when we should stop
	stop chan struct{}
	// exited closes when raft ticker loop exits
	exited chan struct{}
}

func NewRaftServer(serverId ServerId, peers []ServerId, stateDir string, stateMachine StateMachine) (*RaftServer, error) {
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
		stateDir:     stateDir,
		serverId:     serverId,
		stateMachine: stateMachine,
		stop:         make(chan struct{}),
		exited:       make(chan struct{}),
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
	if r.inShutdown.Load() {
		return
	}

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
			if r.inShutdown.Load() {
				close(r.exited)
				return
			}
			r.maybeStartElection()
			r.maybeSendHeartbeats()
		}
	}
}

// Shutdown gracefully shuts down the server.
// New process* requests will be rejected.
// Shutdown will block until inflight requests are complete.
func (r *RaftServer) Shutdown() {
	// Prevent new process* calls from starting.
	r.inShutdown.Store(true)

	// Wait for Start loop to exit. This means no new
	// requests will be made.
	close(r.stop)
	<-r.exited

	// Finally, wait for all in-flight requests to complete
	t := time.NewTicker(2 * time.Millisecond) // expect short requests
	defer t.Stop()
	for range t.C {
		if r.inFlightReqs.Load() == 0 {
			return
		}
	}
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
		panic(fmt.Sprintf("failed to persist raft state: %v", err))
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
	if r.inShutdown.Load() {
		return nil
	}

	r.state.Lock()
	defer r.state.Unlock()
	defer r.persistState()

	// Out of date request
	if appendEntries.Term < r.state.currentTerm {
		return &AppendEntriesRes{Term: r.state.currentTerm, Success: false}
	}

	// Request is for current term, bump election deadline.
	r.state.electionDeadline = newElectionDeadline()

	// We also know that we are a follower if we receive
	// an AppendEntries request for the current term or
	// later one.
	if appendEntries.Term > r.state.currentTerm ||
		r.state.role != RaftRoleFollower {
		r.becomeFollower(appendEntries.Term)
	}

	// If there are entries, entry at prevLogIndex must match term
	// prevLogTerm (5.3)
	if !r.state.log.prevLogMatches(appendEntries.PrevLogIndex, appendEntries.PrevLogTerm) {
		return &AppendEntriesRes{Term: r.state.currentTerm, Success: false}
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
	if idx, hasConflict := r.state.log.findConflict(
		logStartIdx,
		appendEntries.Entries,
	); hasConflict {
		log.Printf("Truncating from %d", idx)
		r.state.log.TruncateFrom(idx)
	}

	// Append any new entries not already in the log. First,
	// skip entries from req that are already in our log.
	// After truncation, we are guaranteed that the new
	// entries either go at the end of our current log,
	// or that they have matching entries overlapping.
	// We just need to figure out the length of the
	// overlap to see what we already have. It'll look
	// something like this (number is log index, assume
	// terms match due to truncation):
	//   log      new entries
	//    5            (=prevLogIndex)
	//    6          6 (=prevLogIndex + 1) // discard new
	//    7          7                     // discard new
	//               8                     // append
	//               9                     // append
	alreadyHave := r.state.log.Len() - int(appendEntries.PrevLogIndex)
	newEntries := appendEntries.Entries[alreadyHave:]
	r.state.log.Append(newEntries)

	if appendEntries.LeaderCommit > r.state.commitIndex {
		r.state.commitIndex = min(
			appendEntries.LeaderCommit, r.state.log.LastLogIndex(),
		)
		r.catchUpStateMachine()
	}

	return &AppendEntriesRes{Term: r.state.currentTerm, Success: true}
}

// catchUpStateMachine applies log entries from lastApplied
// to commitIndex, advancing lastApplied after each successful apply.
func (r *RaftServer) catchUpStateMachine() {
	// Check this invariant first, makes following loop simpler
	if r.state.commitIndex > LogIndex(r.state.log.Len()) {
		panic(fmt.Sprintf(
			"invariant violation, commitIndex=%d > len(log)=%d",
			r.state.commitIndex, r.state.log.Len(),
		))
	}

	for r.state.lastApplied < r.state.commitIndex {
		entry, _ := r.state.log.Get(r.state.lastApplied + 1)
		if err := r.stateMachine.apply(entry.Command); err != nil {
			panic(fmt.Sprintf("failed to apply log entry %d: %v", r.state.lastApplied+1, err))
		}
		r.state.lastApplied++
	}
}

// processClientCommand adds command to log, and blocks until a
// majority of peers have the command replicated to their log.
func (r *RaftServer) processClientCommand(
	command ClientCommandReq,
) *ClientCommandRes {
	if r.inShutdown.Load() {
		return nil
	}

	commandIndex, err := r.appendCommandIfLeader(command.Command)
	if err != nil {
		return &ClientCommandRes{
			Success: false,
			Err:     err,
		}
	}

	// Start peers catching up --- this will kick off async,
	// so we need to then wait for lastApplied to catch up
	// with the LogIndex for the command being added.
	r.startPeerCatchUp()
	r.waitForLastAppliedToCatchUp(commandIndex)

	return &ClientCommandRes{
		Success: true,
		Err:     nil,
	}
}

// appendCommandIfLeader appends the command to the log if we are the leader,
// otherwise returns an error.
// State lock should _not_ be held when calling this method.
func (r *RaftServer) appendCommandIfLeader(command []byte) (LogIndex, error) {
	r.state.Lock()
	defer r.state.Unlock()
	defer r.persistState()

	if r.state.role != RaftRoleLeader {
		return 0, errors.New("client commands only accepted by leader")
	}
	r.state.log.Append([]*LogEntry{{
		Term:    r.state.currentTerm,
		Command: command,
	}})
	commandIndex := r.state.log.LastLogIndex()
	return commandIndex, nil
}

// waitForLastAppliedToCatchUp returns when r.state.lastApplied is at
// least as large as wanted.
// Do not call while holding state lock.
func (r *RaftServer) waitForLastAppliedToCatchUp(wanted LogIndex) {
	// We should use something like a sync.Cond
	// rather than a for + sleep, but for now okay.
	r.state.Lock()
	la := r.state.lastApplied
	r.state.Unlock()
	for la < wanted {
		time.Sleep(1 * time.Millisecond)

		r.state.Lock()
		la = r.state.lastApplied
		r.state.Unlock()
	}
}

// startPeerCatchUp kicks of async catch up for each peer.
// Also handles case where there are no peers.
// Do not call while holding state lock.
func (r *RaftServer) startPeerCatchUp() {
	r.state.Lock()
	defer r.state.Unlock()

	// If we have peers, catch up each in parallel. Otherwise, if we
	// are the only server, catch up ourselves and return.
	if len(r.state.peers) > 0 {
		for _, peer := range r.state.peers {
			go r.catchUpPeer(peer)
		}
	} else {
		r.maybeAdvanceCommitIndex()
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
			// No longer leader, so don't send more updates
			r.state.Unlock()
			return
		}
		if r.state.log.LastLogIndex() < r.state.nextIndex[peer] {
			// Caught up this peer
			r.state.Unlock()
			return
		}
		r.state.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), heartbeatDuration)
		nextReq := r.nextAEReqForPeer(peer)
		res, err := r.transport.makeHeartbeatRequest(
			ctx, peer, nextReq.req,
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
		r.state.nextIndex[peer] = nextReq.newNextIndex
		r.state.matchIndex[peer] = nextReq.newMatchIndex
		r.maybeAdvanceCommitIndex()
		r.state.Unlock()
	}
}

// maybeAdvanceCommitIndex recalculates the commit index based on
// peer match indexes, and applies any newly committed entries to
// the state machine.
// Caller must hold state lock.
func (r *RaftServer) maybeAdvanceCommitIndex() {
	r.state.commitIndex = calculateUpdatedCommitIndex(
		r.serverId,
		r.state.currentTerm,
		r.state.commitIndex,
		r.state.matchIndex,
		&r.state.log,
	)
	if r.state.commitIndex > r.state.lastApplied {
		r.catchUpStateMachine()
	}
}

// calculateUpdatedCommitIndex will advance the commit index if enough
// peers are beyond the current index.
// The state lock must be held when calling this function.
func calculateUpdatedCommitIndex(
	serverId ServerId,
	currentTerm Term,
	currentCommitIndex LogIndex,
	matchIndex map[ServerId]LogIndex,
	raftLog *RaftLog,
) LogIndex {
	// from the paper: If there exists an N such that N > commitIndex, a
	// majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).

	log.Printf("[%d] maybeAdvanceCommitIndex %v %d %v", serverId, matchIndex, currentCommitIndex, raftLog.log)

	// Collect our leader last log index alongside the peer matchIndex
	// so we have all servers' known durable log indexes.
	// Find the highest N in that array such that a majority is
	// above it, that N is the commitIndex.
	commitIndexes := []LogIndex{raftLog.LastLogIndex()}
	for _, idx := range matchIndex {
		commitIndexes = append(commitIndexes, idx)
	}
	slices.Sort(commitIndexes)
	slices.Reverse(commitIndexes)
	// Now we have, eg, [5,4,4,2,1], so 4 is majority for N
	// Observation: N will always equal the mid value
	n := commitIndexes[len(commitIndexes)/2]

	// Check whether N > commitIndex
	if n <= currentCommitIndex {
		return currentCommitIndex
	}
	// Check the term on that log[N]
	entry, found := raftLog.Get(n)
	if !found {
		log.Printf(
			"[%d] maybeAdvanceCommitIndex - should be impossible for N>len(log)",
			serverId)
		return currentCommitIndex
	}
	if entry.Term != currentTerm {
		return currentCommitIndex
	}
	// Bump commit index to N
	return n
}

// nextAEReqForPeer is a container for the result of RaftServer.nextAEReqForPeer
type nextAEReqForPeer struct {
	req           AppendEntriesReq
	newNextIndex  LogIndex
	newMatchIndex LogIndex
}

// nextAEReqForPeer generates an AppendEntries request for the peer
// that uses the nextIndex state to send the appropriate log entries.
// Returns the request, new nextIndex and matchIndex if req succeeds.
// Do not call while holding the state lock.
func (r *RaftServer) nextAEReqForPeer(peer ServerId) nextAEReqForPeer {
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
	newMatchIndex := prevLogIndex + LogIndex(len(appendEntriesReq.Entries))
	return nextAEReqForPeer{
		req:           appendEntriesReq,
		newNextIndex:  newMatchIndex + 1,
		newMatchIndex: newMatchIndex,
	}
}

func (r *RaftServer) processRequestVote(requestVote RequestVoteReq) *RequestVoteRes {
	if r.inShutdown.Load() {
		return nil
	}

	r.state.Lock()
	defer r.state.Unlock()
	defer r.persistState()

	// Always convert to follower if we receive an RPC with a newer term (5.1)
	// This also advances our term to match the request's and clears our vote.
	if requestVote.Term > r.state.currentTerm {
		r.becomeFollower(requestVote.Term)
	}

	// Reject stale requests (5.1)
	if requestVote.Term < r.state.currentTerm {
		log.Printf("[%d] RequestVote: STALE_TERM candidate=%d term=%d", r.serverId, requestVote.CandidateId, requestVote.Term)
		return &RequestVoteRes{Term: r.state.currentTerm, VoteGranted: false}
	}

	// We already won the election for this term, don't grant another candidate a vote (5.2)
	if r.state.role == RaftRoleLeader && requestVote.Term == r.state.currentTerm {
		log.Printf("[%d] RequestVote: I_AM_LEADER candidate=%d term=%d", r.serverId, requestVote.CandidateId, requestVote.Term)
		return &RequestVoteRes{Term: r.state.currentTerm, VoteGranted: false}
	}

	// Only grant one vote per term (section 5.2)
	if r.state.votedFor != noVote && r.state.votedFor != requestVote.CandidateId {
		log.Printf("[%d] RequestVote: ALREADY_VOTED candidate=%d term=%d", r.serverId, requestVote.CandidateId, requestVote.Term)
		return &RequestVoteRes{Term: r.state.currentTerm, VoteGranted: false}
	}

	// Election restriction: candidate's log must be at least as up-to-date (section 5.4.1)
	if r.state.log.AheadOf(requestVote.LastLogTerm, requestVote.LastLogIndex) {
		log.Printf("[%d] RequestVote: LOG_BEHIND candidate=%d term=%d", r.serverId, requestVote.CandidateId, requestVote.Term)
		return &RequestVoteRes{Term: r.state.currentTerm, VoteGranted: false}
	}

	// Grant vote
	log.Printf("[%d] RequestVote: GRANTED candidate=%d term=%d", r.serverId, requestVote.CandidateId, requestVote.Term)
	r.state.votedFor = requestVote.CandidateId
	return &RequestVoteRes{Term: r.state.currentTerm, VoteGranted: true}
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
	r.state.currentTerm++
	r.state.votedFor = r.serverId
	log.Printf("%d became candidate, term=%d", r.serverId, r.state.currentTerm)

	// Gather state needed for the election
	peers := slices.Clone(r.state.peers)
	electionTerm := r.state.currentTerm
	lastLogTerm := Term(0)
	entry, found := r.state.log.Get(r.state.log.LastLogIndex())
	if found {
		lastLogTerm = entry.Term
	}

	newDeadline := newElectionDeadline()
	go func() {
		ctx, cancel := context.WithDeadline(context.Background(), newDeadline)
		defer cancel()
		r.runElection(ctx, electionTerm, lastLogTerm, peers)
	}()
	r.state.electionDeadline = newDeadline
}

// runElection runs a single election. We exit this function either as
// follower, leader or still as a candidate. In the latter state, we
// run another election.
func (r *RaftServer) runElection(ctx context.Context, electionTerm Term, lastLogTerm Term, peers []ServerId) {
	log.Printf("[%d] runElection term=%d", r.serverId, electionTerm)

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
			r.inFlightReqs.Add(1)
			defer r.inFlightReqs.Add(-1)

			peerResult, err := r.transport.makeRequestVoteRequest(ctx, peer, reqVoteReq)
			if err != nil {
				log.Printf("Error requesting vote from %d; assuming false vote: %v", peer, err)
				votes <- false
				return
			}

			r.state.Lock()
			defer r.state.Unlock()
			defer r.persistState()

			switch {
			case peerResult.Term > electionTerm:
				// Time has moved on and a new leader has arisen. Accept
				// this and become a follower. (5.1)
				r.becomeFollower(peerResult.Term)
				votes <- false
			case peerResult.Term < electionTerm:
				// A result from the past, discard it.
				votes <- false
			default:
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
			r.inFlightReqs.Add(1)
			defer r.inFlightReqs.Add(-1)

			result, err := r.transport.makeHeartbeatRequest(ctx, peer, appendEntriesReq)
			if err != nil {
				log.Printf("Error sending heartbeat to %v: %v", peer, err)
				return
			}

			// Make any state updates based on the response.
			r.state.Lock()
			defer r.state.Unlock()
			defer r.persistState()

			// Time has moved on, and another leader has
			// arisen. Accept this, and become a follower. (section 5.1)
			if result.Term > r.state.currentTerm {
				r.becomeFollower(result.Term)
			}
		})
	}
	wg.Wait()
}
