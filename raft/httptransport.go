package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// rpcResponder is the interface implemented by the Raft consensus module.
type rpcResponder interface {
	processRequestVote(requestVote requestVoteReq) *requestVoteRes
	processAppendEntriesRequest(appendEntries appendEntriesReq) *appendEntriesRes
}

// HttpTransport is a HTTP server and client transport
// for Raft. responder and peerAddrs must be set before
// calling Serve.
type HttpTransport struct {
	srv       *http.Server
	listener  net.Listener
	responder rpcResponder
	peerAddrs map[ServerId]net.Addr
}

func NewHttpTransport() (*HttpTransport, error) {
	t := HttpTransport{}
	mux := http.NewServeMux()
	mux.HandleFunc("/append_entries", t.appendEntriesRPCHandler)
	mux.HandleFunc("/request_vote", t.requestVoteRPCHandler)
	srv := &http.Server{
		Handler:        mux,
		ReadTimeout:    1 * time.Second,
		WriteTimeout:   1 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	t.srv = srv

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	t.listener = listener

	return &t, nil
}

// Host sets up this transport as a host for a raft node.
// It must be called at most once.
func (t *HttpTransport) Host(raft *RaftServer) {
	t.responder = raft
	raft.transport = t
}

func (t *HttpTransport) Addr() net.Addr {
	return t.listener.Addr()
}

func (t *HttpTransport) Serve() error {
	if err := t.srv.Serve(t.listener); err != nil {
		return fmt.Errorf("error serving HTTP: %v", err)
	}
	return nil
}

func (t *HttpTransport) Shutdown() error {
	if err := t.srv.Shutdown(context.Background()); err != nil {
		return fmt.Errorf("HTTP server Shutdown: %v", err)
	}
	if err := t.listener.Close(); err != nil {
		return fmt.Errorf("closing listener: %v", err)
	}
	return nil
}

func (t *HttpTransport) appendEntriesRPCHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	data, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	appendEntries := appendEntriesReq{}
	err = json.Unmarshal(data, &appendEntries)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	response := t.responder.processAppendEntriesRequest(appendEntries)

	data, err = json.Marshal(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func (t *HttpTransport) requestVoteRPCHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	data, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// log.Printf("[%d] requestVoteRPC body=%s", r.serverId, data)
	requestVote := requestVoteReq{}
	err = json.Unmarshal(data, &requestVote)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// log.Printf("[%d] requestVoteRPC request=%+v", r.serverId, requestVote)
	response := t.responder.processRequestVote(requestVote)

	data, err = json.Marshal(&response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

// makeRequestVoteRequest implements rpcOutgoingTransport.
func (t *HttpTransport) makeRequestVoteRequest(
	ctx context.Context,
	dest ServerId,
	requestVote requestVoteReq,
) (*requestVoteRes, error) {
	addr := t.peerAddrs[dest]
	reqData, err := json.Marshal(requestVote)
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequestWithContext(
		ctx,
		"POST",
		fmt.Sprintf("http://%s/request_vote", addr.String()),
		bytes.NewReader(reqData),
	)
	req.Header.Add("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	resData, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	result := &requestVoteRes{}
	err = json.Unmarshal(resData, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// makeHeartbeatRequest implements rpcOutgoingTransport.
func (t *HttpTransport) makeHeartbeatRequest(
	ctx context.Context,
	dest ServerId,
	appendEntriesReq appendEntriesReq,
) (*appendEntriesRes, error) {
	addr := t.peerAddrs[dest]
	reqData, err := json.Marshal(appendEntriesReq)
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequestWithContext(
		ctx,
		"POST",
		fmt.Sprintf("http://%s/append_entries", addr.String()),
		bytes.NewReader(reqData),
	)
	req.Header.Add("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	resData, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	result := &appendEntriesRes{}
	err = json.Unmarshal(resData, result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
