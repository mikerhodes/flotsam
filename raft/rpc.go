package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

type Term int64
type ServerId int64
type LogIndex int64

// ElectionResult represents whether this node won an election.
// True if it did, false if it didn't.
type ElectionResult bool

type RaftState int

const (
	RaftStateFollower  = 1
	RaftStateCandidate = 2
	RaftStateLeader    = 3
)

// electionTimeout is the duration after which we call an election
// if no RPCs have been received.
const electionTimeout = 1 * time.Second
const heartbeatDuration = electionTimeout / 2

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

type RaftServer struct {
	// Raft state
	state RaftState

	// Persistent state
	currentTerm Term
	votedFor    *ServerId
	log         []LogEntry
	serverId    ServerId

	// Volatile state
	commitIndex LogIndex
	lastApplied LogIndex

	// Leader volatile state
	nextIndex  map[ServerId]LogIndex
	matchIndex map[ServerId]LogIndex

	// Other state
	stop             chan struct{}
	appendEntries    chan *AppendEntriesReq
	requestVote      chan *RequestVoteReq
	peers            []net.Addr
	electionDeadline time.Time
}

func NewRaftServer() *RaftServer {
	return &RaftServer{
		state:            RaftStateFollower,
		currentTerm:      0,
		votedFor:         nil,
		log:              []LogEntry{},
		serverId:         0,
		commitIndex:      0,
		lastApplied:      0,
		nextIndex:        map[ServerId]LogIndex{},
		matchIndex:       map[ServerId]LogIndex{},
		stop:             make(chan struct{}),
		appendEntries:    make(chan *AppendEntriesReq),
		requestVote:      make(chan *RequestVoteReq),
		peers:            []net.Addr{},
		electionDeadline: time.Now().Add(electionTimeout),
	}
}

func (r *RaftServer) Start(l net.Listener) {
	// Start our HTTP server for other servers
	mux := http.NewServeMux()
	mux.HandleFunc("/append_entries", func(w http.ResponseWriter, req *http.Request) {
		req.Body.Close()
		data, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		appendEntries := AppendEntriesReq{}
		err = json.Unmarshal(data, &r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.appendEntries <- &appendEntries
		response := AppendEntriesRes{
			Term:    r.currentTerm,
			Success: true,
		}
		data, err = json.Marshal(&response)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(data)
	})
	mux.HandleFunc("/request_vote", func(w http.ResponseWriter, req *http.Request) {
		req.Body.Close()
		data, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		requestVote := RequestVoteReq{}
		err = json.Unmarshal(data, &r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		r.requestVote <- &requestVote

		// TODO do I do this if I'm a candidate? Do we need to do this only for
		// follower state?
		// TODO clearly there is some protection needed for state!
		// At least currentTerm and votedFor are concurrently accessed
		response := RequestVoteRes{
			Term: r.currentTerm,
		}

		// handle vote
		if requestVote.Term < r.currentTerm {
			// Request is coming from candidate in out of date term
			response.VoteGranted = false
		} else if r.votedFor != nil && r.votedFor != &requestVote.CandidateId {
			// We already voted this term for a different candidate
			response.VoteGranted = false
		} else if requestVote.LastLogTerm < r.currentTerm {
			// Our log is newer than the candidate's
			response.VoteGranted = false
		} else if requestVote.LastLogIndex < r.lastApplied {
			// Our log is newer than the candidate's
			response.VoteGranted = false
		} else {
			// All conditions passed
			response.VoteGranted = true
		}

		data, err = json.Marshal(&response)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(data)
	})
	srv := &http.Server{
		Handler:        mux,
		ReadTimeout:    1 * time.Second,
		WriteTimeout:   1 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	go func() {
		if err := srv.Serve(l); err != nil {
			log.Fatal("Error serving HTTP")
		}
	}()
	go func() {
		<-r.stop
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Printf("HTTP server Shutdown: %v", err)
		}
	}()

	// Start the Raft state machine
	r.state = RaftStateFollower
	go func() {
		for {
			// Check whether we should stop
			select {
			case <-r.stop:
				return
			default:
				// continue
			}

			// Execute this state
			switch r.state {
			case RaftStateFollower:
				r.runFollowerState()
			case RaftStateCandidate:
				r.runCandidateState()
			case RaftStateLeader:
				r.runLeaderState()
			}
		}
	}()
}

func (r *RaftServer) Stop() {
	close(r.stop)
}

func (r *RaftServer) runFollowerState() {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case appendEntry := <-r.appendEntries:
			// assume heartbeat for now but reset election timer
			r.currentTerm = appendEntry.Term
			r.electionDeadline = time.Now().Add(electionTimeout)
		case requestVote := <-r.requestVote:
			// assume heartbeat for now but reset election timer
			r.currentTerm = requestVote.Term
			r.electionDeadline = time.Now().Add(electionTimeout)
		case <-ticker.C:
			if time.Now().After(r.electionDeadline) {
				r.currentTerm += 1
				r.state = RaftStateCandidate
				return
			}
		case <-r.stop:
			return
		}
	}
}

func (r *RaftServer) runCandidateState() {
	// Loop until:
	// - We win the election and become the leader
	// - We learn of another leader via AppendEntry RPC
	for {
		r.stepCandidateState()
		if r.state != RaftStateCandidate {
			return
		}
	}
}

func (r *RaftServer) stepCandidateState() {
	// ctx sets the timeout for the election
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatDuration)
	defer cancel()

	result := make(chan ElectionResult)
	go func() {
		// For now we just go straight into another election.
		time.Sleep(100 * time.Millisecond) // TODO we shouldn't really wait here, should wait on electionDeadline
		result <- r.runElection(ctx)
	}()

	// Run the election.
	// If we discover a new leader has already been chosen,
	// become a follower and return.
	// If we win, set our state to leader. Return.
	// If we don't win, remain a candidate. Return.
	// If the election times out, return and wait for next election.
	for {
		select {
		case appendEntry := <-r.appendEntries:
			// If we get AppendEntry during with term >= currentTerm,
			// another candidate won the election.
			// Back to state follower, discard election (will cancel context)
			if appendEntry.Term > r.currentTerm {
				r.state = RaftStateFollower
				r.currentTerm = appendEntry.Term
				r.electionDeadline = time.Now().Add(electionTimeout)
				return
			}
		case won := <-result:
			if won {
				r.state = RaftStateLeader
			}
			return
		case <-ctx.Done(): // election timed out
			return
		}
	}
}

// runElection runs a single election. We exit this function either as
// follower, leader or still as a candidate. In the latter state, we
// run another election.
func (r *RaftServer) runElection(ctx context.Context) ElectionResult {

	votes := make(chan bool, len(r.peers))
	votesForMe := 1 // vote for self
	votesRequired := len(r.peers)/2 + 1

	// Send RequestVote RPCs, ensuring timeout using ctx
	for _, peer := range r.peers {
		go func() {
			peerResult, err := r.requestVoteFromPeer(ctx, peer)
			if err != nil {
				log.Printf("Error requesting vote from %s; assuming false vote: %v", peer, err)
				votes <- false
			} else {
				votes <- peerResult
			}

		}()
	}

	// Wait for enough responses to declare victory, or timeout,
	// or we receive all responses but not enough to win.
	for range len(r.peers) {
		select {
		case v := <-votes:
			if v {
				votesForMe += 1
			}
			// Exit if we got the required number of votes
			if votesForMe >= votesRequired {
				return true
			}
		case <-ctx.Done(): // too few responses before timed out
			return false
		}
	}

	// If we win the election, we will have returned during for loop
	return false
}

// requestVoteFromPeer makes a HTTP request to peer asking for its vote in a leader election
func (r *RaftServer) requestVoteFromPeer(ctx context.Context, peer net.Addr) (bool, error) {
	req, _ := http.NewRequestWithContext(
		ctx,
		"POST",
		fmt.Sprintf("http://%s/request_vote", peer.String()),
		strings.NewReader("{}"),
	)
	req.Header.Add("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return false, err
	}
	result := &RequestVoteRes{}
	err = json.Unmarshal(data, result)
	if err != nil {
		return false, err
	}
	if result.VoteGranted && r.currentTerm == result.Term { // TODO is this right: currentTerm >= r.Term?
		return true, nil
	} else {
		return false, nil
	}
}

func (r *RaftServer) runLeaderState() {
	// Send heartbeats to all peers to retain leadership
	t := time.NewTicker(heartbeatDuration)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			r.leaderHeartbeat()
		case appendEntry := <-r.appendEntries:
			r.electionDeadline.Add(electionTimeout)
			if r.currentTerm < appendEntry.Term {
				r.state = RaftStateFollower // Do we need to append entries to log?
				r.currentTerm = appendEntry.Term
				return
			}
		case requestVote := <-r.requestVote:
			r.electionDeadline.Add(electionTimeout)
			if r.currentTerm < requestVote.Term {
				r.state = RaftStateFollower
				r.currentTerm = requestVote.Term
				return
			}
		case <-r.stop:
			return
		}
	}
}

// leaderHeartbeat sends a heartbeat to all peers
func (r *RaftServer) leaderHeartbeat() {
	// ctx sets timeout for all requests
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatDuration/2)
	defer cancel()

	for _, peer := range r.peers {
		go func() {
			req, _ := http.NewRequestWithContext(
				ctx,
				"POST",
				fmt.Sprintf("http://%s/append_entries", peer.String()),
				strings.NewReader("{}"),
			)
			req.Header.Add("Content-Type", "application/json")
			// Fire and forget; will timeout
			if _, err := http.DefaultClient.Do(req); err != nil {
				log.Printf("Error sending heartbeat to %s", peer)
			}
		}()
	}
}
