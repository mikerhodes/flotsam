package raft

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"
)

type Term int64
type ServerId int64
type LogIndex int64

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

	peerAddrs []net.Addr
}

type RaftServer struct {
	state *state

	// serverId is read-only server ID
	serverId ServerId

	// stop closes when we should stop
	stop chan struct{}
}

func NewRaftServer(serverId ServerId, peerAddrs []net.Addr) (*RaftServer, error) {
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
			peerAddrs:        slices.Clone(peerAddrs),
			electionDeadline: electionDeadline,
			nextHeartbeat:    nextHeartbeat,
		},
		serverId: serverId,
		stop:     make(chan struct{}),
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
func (r *RaftServer) Start(l net.Listener) {
	// Start our HTTP server for other servers
	mux := http.NewServeMux()
	mux.HandleFunc("/append_entries", r.appendEntriesRPC)
	mux.HandleFunc("/request_vote", r.requestVoteRPC)
	srv := &http.Server{
		Handler:        mux,
		ReadTimeout:    1 * time.Second,
		WriteTimeout:   1 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	wg := sync.WaitGroup{}

	// Handle events based on state
	wg.Go(func() {
		if err := srv.Serve(l); err != nil {
			log.Printf("Error serving HTTP: %v", err)
		}
	})

	// Election timeout and heartbeats
	wg.Go(func() {
		const checkInterval = 25 * time.Millisecond

		t := time.NewTicker(checkInterval)
		for {
			select {
			case <-r.stop:
				return
			case <-t.C:
				r.maybeStartElection()
				r.maybeSendHeartbeats()
			}
		}
	})

	// Handle shutdown
	wg.Go(func() {
		<-r.stop
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Printf("HTTP server Shutdown: %v", err)
		}
	})

	wg.Wait()
}

func (r *RaftServer) Shutdown() {
	close(r.stop)
}

// Role returns the current Raft role of this server.
func (r *RaftServer) Role() RaftRole {
	r.state.Lock()
	defer r.state.Unlock()
	return r.state.role
}

func (r *RaftServer) appendEntriesRPC(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	data, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	appendEntries := AppendEntriesReq{}
	err = json.Unmarshal(data, &appendEntries)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	response := r.processAppendEntriesRequest(&appendEntries)

	data, err = json.Marshal(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func (r *RaftServer) processAppendEntriesRequest(appendEntries *AppendEntriesReq) *AppendEntriesRes {
	r.state.Lock()
	defer r.state.Unlock()

	ct := r.state.currentTerm

	// Out of date request
	if appendEntries.Term < r.state.currentTerm {
		return &AppendEntriesRes{Term: ct, Success: false}
	}

	// Request is for current term, bump election deadline
	r.state.electionDeadline = newElectionDeadline()

	// If we receive an AppendEntries with Term >= currentTerm,
	// we are definitely not the leader for this term.
	if appendEntries.Term >= r.state.currentTerm {
		r.state.role = RaftRoleFollower
	}
	// If we have moved to a new term, clear our vote state
	if appendEntries.Term > r.state.currentTerm {
		r.state.votedFor = noVote
	}
	r.state.currentTerm = appendEntries.Term
	return &AppendEntriesRes{Term: ct, Success: true}
}

func (r *RaftServer) requestVoteRPC(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	data, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// log.Printf("[%d] requestVoteRPC body=%s", r.serverId, data)
	requestVote := RequestVoteReq{}
	err = json.Unmarshal(data, &requestVote)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// log.Printf("[%d] requestVoteRPC request=%+v", r.serverId, requestVote)
	response := r.processRequestVote(&requestVote)

	data, err = json.Marshal(&response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func (r *RaftServer) processRequestVote(requestVote *RequestVoteReq) *RequestVoteRes {
	r.state.Lock()
	defer r.state.Unlock()

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
		r.state.role = RaftRoleFollower
		r.state.votedFor = noVote
	}
	r.state.currentTerm = requestVote.Term
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

	newDeadline := newElectionDeadline()
	go func() {
		ctx, cancel := context.WithDeadline(context.Background(), newDeadline)
		defer cancel()
		r.runElection(ctx)
	}()
	r.state.electionDeadline = newDeadline
}

// runElection runs a single election. We exit this function either as
// follower, leader or still as a candidate. In the latter state, we
// run another election.
func (r *RaftServer) runElection(ctx context.Context) {
	r.state.Lock()
	peers := slices.Clone(r.state.peerAddrs)
	electionTerm := r.state.currentTerm
	log.Printf("[%d] runElection term=%d", r.serverId, electionTerm)
	r.state.Unlock()

	voteReq := RequestVoteReq{
		Term:         electionTerm,
		CandidateId:  r.serverId,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	log.Printf("[%d] runElection request=%+v", r.serverId, voteReq)
	reqVoteBody, err := json.Marshal(voteReq)
	if err != nil {
		panic("Should never have un-marshal-able request vote body")
	}
	won := r.collectVotes(ctx, peers, reqVoteBody)

	r.state.Lock()
	defer r.state.Unlock()

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
		r.state.role = RaftRoleLeader
		r.state.nextHeartbeat = nextHeartbeat()
		go r.sendLeaderHeartbeats()
	}
}

// collectVotes sends peers vote requests, and returns true if this
// node received a majority of votes.
func (r *RaftServer) collectVotes(ctx context.Context, peers []net.Addr, reqVoteBody []byte) ElectionResult {
	votes := make(chan bool, len(peers))
	votesForMe := 1 // vote for self
	votesRequired := len(peers)/2 + 1

	// Send RequestVote RPCs, ensuring timeout using ctx
	for _, peer := range peers {
		go func() {
			peerResult, err := r.makeRequestVoteRequest(ctx, peer, reqVoteBody)
			if err != nil {
				log.Printf("Error requesting vote from %s; assuming false vote: %v", peer, err)
				votes <- false
				return
			}

			// Respond to result, which might indicate that time
			// has moved forward.
			r.state.Lock()
			defer r.state.Unlock()

			log.Printf("[%d] collectVotes received vote electionTerm=%d, peerResult=%v", r.serverId, r.state.currentTerm, peerResult)

			// Time has moved on and a new leader has arisen. Accept
			// this and become a follower.
			if peerResult.Term > r.state.currentTerm {
				r.state.currentTerm = peerResult.Term
				r.state.role = RaftRoleFollower
				r.state.votedFor = noVote
				votes <- false
				return
			}

			// A result from the past, discard it.
			if peerResult.Term < r.state.currentTerm {
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
			log.Printf("[%d] collectVotes received vote electionTerm=%d, vote=%t", r.serverId, r.state.currentTerm, v)
			if v {
				votesForMe += 1
			}
			// Exit if we got the required number of votes
			if votesForMe >= votesRequired {
				log.Printf("[%d] collectVotes WON electionTerm=%d", r.serverId, r.state.currentTerm)
				return true
			}
		case <-ctx.Done(): // too few responses before timed out
			log.Printf("[%d] collectVotes TIMEOUT electionTerm=%d", r.serverId, r.state.currentTerm)
			return false
		}
	}

	// If we win the election, we will have returned during for loop,
	// unless we are the only server.
	if votesForMe >= votesRequired {
		log.Printf("[%d] collectVotes WON electionTerm=%d", r.serverId, r.state.currentTerm)
		return true
	} else {
		log.Printf("[%d] collectVotes NOTENOUGHVOTES electionTerm=%d", r.serverId, r.state.currentTerm)
		return false
	}
}

// makeRequestVoteRequest makes a request to peer asking for its vote
// in a leader election, returning the peer's vote.
func (r *RaftServer) makeRequestVoteRequest(ctx context.Context, peer net.Addr, reqVoteBody []byte) (*RequestVoteRes, error) {
	// log.Printf("[%d] makeRequestVoteRequest body=%s", r.serverId, reqVoteBody)
	req, _ := http.NewRequestWithContext(
		ctx,
		"POST",
		fmt.Sprintf("http://%s/request_vote", peer.String()),
		bytes.NewReader(reqVoteBody),
	)
	req.Header.Add("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	result := &RequestVoteRes{}
	err = json.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}
	return result, nil
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
	data, err := json.Marshal(&AppendEntriesReq{
		Term:         r.state.currentTerm,
		LeaderId:     r.serverId,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	})
	if err != nil {
		panic("Should never have un-marshal-able heartbeat body")
	}
	r.state.Unlock()

	// ctx sets timeout for all requests
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatDuration)
	defer cancel()
	wg := sync.WaitGroup{}
	for _, peer := range r.state.peerAddrs {
		wg.Go(func() {
			result, err := r.makeHeartbeatRequest(ctx, peer, data)
			if err != nil {
				log.Printf("Error sending heartbeat to %v: %v", peer, err)
				return
			}

			// Make any state updates based on the response.
			r.state.Lock()
			defer r.state.Unlock()

			// Time has moved on, and another leader has likely
			// arisen. Accept this, and become a follower. (section 5.1)
			if result.Term > r.state.currentTerm {
				r.state.role = RaftRoleFollower
				r.state.votedFor = noVote
			}
		})
	}
	wg.Wait()
}

// makeHeartbeatRequest makes a heartbeat request to a peer, returning
// the result.
func (r *RaftServer) makeHeartbeatRequest(
	ctx context.Context,
	peer net.Addr,
	bodyData []byte,
) (*AppendEntriesRes, error) {
	log.Printf("[%d] makeHeartBeatRequest body=%s", r.serverId, bodyData)
	req, _ := http.NewRequestWithContext(
		ctx,
		"POST",
		fmt.Sprintf("http://%s/append_entries", peer.String()),
		bytes.NewReader(bodyData),
	)
	req.Header.Add("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	result := &AppendEntriesRes{}
	err = json.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
