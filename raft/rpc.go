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

type LogEntry struct{}

type AppendEntriesReq struct {
	Term         Term
	LeaderId     ServerId
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
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

type state struct {
	sync.Mutex

	// Raft role
	role RaftRole

	// Persistent state
	currentTerm Term
	votedFor    ServerId
	log         []LogEntry

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
	Log         []LogEntry
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
			log:              []LogEntry{},
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
		r.state.log = ps.Log
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
		Log:         r.state.log,
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
	log.Printf("%d became follower, term=%d", r.serverId, r.state.currentTerm)
}

// becomeLeader makes us a leader for the current term
// Caller must hold state lock.
func (r *RaftServer) becomeLeader() {
	r.state.role = RaftRoleLeader
	r.state.votedFor = noVote
	r.state.nextHeartbeat = nextHeartbeat()
	log.Printf("%d became leader, term=%d", r.serverId, r.state.currentTerm)
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
	if appendEntries.Term >= ct {
		r.becomeFollower(appendEntries.Term)
	}
	return &AppendEntriesRes{Term: ct, Success: true}
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

	// Section 5.4 safety property
	// TODO this only makes sense when we are processing the log
	// if requestVote.LastLogTerm < ct {
	// 	log.Printf("[%d] processRequestVote LOWER_LAST_LOG_TERM electionTerm=%d", r.serverId, requestVote.Term)
	// 	return &RequestVoteRes{Term: ct, VoteGranted: false}
	// }
	// if requestVote.LastLogIndex < r.state.lastApplied {
	// 	log.Printf("[%d] processRequestVote LOWER_LAST_LOG_INDEX electionTerm=%d", r.serverId, requestVote.Term)
	// 	return &RequestVoteRes{Term: ct, VoteGranted: false}
	// }

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

	voteReq := RequestVoteReq{
		Term:         electionTerm,
		CandidateId:  r.serverId,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	log.Printf("[%d] runElection request=%+v", r.serverId, voteReq)
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

	// Send RequestVote RPCs, ensuring timeout using ctx
	for _, peer := range peers {
		go func() {
			peerResult, err := r.transport.makeRequestVoteRequest(ctx, peer, reqVoteReq)
			if err != nil {
				log.Printf("Error requesting vote from %d; assuming false vote: %v", peer, err)
				votes <- false
				return
			}

			// Respond to result, which might indicate that time
			// has moved forward.
			r.state.Lock()
			defer r.state.Unlock()
			defer r.persistState()

			log.Printf("[%d] collectVotes received vote electionTerm=%d, peerResult=%v", r.serverId, r.state.currentTerm, peerResult)

			// Time has moved on and a new leader has arisen. Accept
			// this and become a follower.
			if peerResult.Term > electionTerm {
				r.becomeFollower(peerResult.Term)
				votes <- false
				return
			}

			// A result from the past, discard it.
			if peerResult.Term < electionTerm {
				votes <- false
				return
			}

			// Peer is still in the same term, use its result.
			votes <- peerResult.VoteGranted
		}()
	}

	// Wait for enough responses to declare victory, or timeout,
	// or we receive all responses but not enough to win.
	for range len(peers) {
		select {
		case v := <-votes:
			log.Printf("[%d] collectVotes received vote electionTerm=%d, vote=%t", r.serverId, electionTerm, v)
			if v {
				votesForMe += 1
			}
			// Exit if we got the required number of votes
			if votesForMe >= votesRequired {
				log.Printf("[%d] collectVotes WON electionTerm=%d", r.serverId, electionTerm)
				return true
			}
		case <-ctx.Done(): // too few responses before timed out
			log.Printf("[%d] collectVotes TIMEOUT electionTerm=%d", r.serverId, electionTerm)
			return false
		}
	}

	// If we win the election, we will have returned during for loop,
	// unless we are the only server.
	if votesForMe >= votesRequired {
		log.Printf("[%d] collectVotes WON electionTerm=%d", r.serverId, electionTerm)
		return true
	} else {
		log.Printf("[%d] collectVotes NOT_ENOUGH_VOTES electionTerm=%d", r.serverId, electionTerm)
		return false
	}
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
		Entries:      []LogEntry{},
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
