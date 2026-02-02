# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Flotsam is a Go implementation of the Raft consensus protocol. The implementation follows the Raft paper, with section references in code comments (e.g., "section 5.1", "section 5.2").

## Build and Test Commands

```bash
go build ./...     # Build all packages
go test ./...      # Run all tests
go test ./raft     # Run tests for the raft package
go test -run TestName ./raft  # Run a specific test
```

## Architecture

The Raft implementation is in the `raft/` package:

- **rpc.go**: Core Raft consensus logic
- **httpserver.go**: HTTP transport layer for inter-node RPC

### Core Types (rpc.go)

- **RaftServer**: Main server struct that manages state and runs the event loop
- **state**: Thread-safe state container holding Raft persistent and volatile state
- **RaftLog**: Log wrapper with 1-based indexing (to match Raft paper)
- **StateMachine**: Interface for applying committed log entries to external state
- **Roles**: Follower, Candidate, Leader (RaftRole enum)

### Transport Layer (httpserver.go)

- **HttpTransport**: HTTP server and client for Raft RPC
- **rpcOutgoingTransport**: Interface for outgoing RPCs (vote requests, heartbeats)
- **rpcResponder**: Interface for incoming RPC handlers
- Endpoints: `/append_entries`, `/request_vote`

### Key Flows

- **Event Loop**: Ticker-based loop (25ms) in `Start()` checks election deadlines and heartbeat timing
- **Election**: `maybeStartElection()` → `runElection()` → `collectVotes()`
- **Heartbeats**: Leader sends via `maybeSendHeartbeats()` → `sendLeaderHeartbeats()`
- **Log Replication**: `processClientCommand()` → `catchUpPeer()` for each peer
- **State Machine**: `catchUpStateMachine()` applies committed entries from lastApplied to commitIndex

### Timing Constants

- Election timeout: 150-300ms (base + random perturbation)
- Heartbeat interval: 75ms
- Check interval: 25ms

## Test Organization

Tests are organized by feature area:

- **appendentry_test.go**: AppendEntries RPC handling, log replication, state machine application
- **vote_test.go**: RequestVote RPC, election logic
- **clientcommand_test.go**: Client command processing
- **catchup_test.go**: Peer log catch-up during replication
- **nextaereq_test.go**: AppendEntries request generation for peers
- **cluster_test.go**: Multi-server integration tests
- **testharness.go**: Test utilities for creating multi-server clusters
