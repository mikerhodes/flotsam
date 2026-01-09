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

The entire Raft implementation lives in `raft/rpc.go`:

- **RaftServer**: Main server struct that manages state and runs the event loop
- **state**: Thread-safe state container holding Raft persistent and volatile state
- **Roles**: Follower, Candidate, Leader (RaftRole enum)

### Key Components

- **HTTP RPC Layer**: Uses `/append_entries` and `/request_vote` endpoints for inter-node communication
- **Event Loop**: Ticker-based loop (25ms) in `Start()` checks election deadlines and heartbeat timing
- **Election**: `maybeStartElection()` → `runElection()` → `collectVotes()` flow
- **Heartbeats**: Leader sends via `maybeSendHeartbeats()` → `sendLeaderHeartbeats()`

### Timing Constants

- Election timeout: 150-300ms (base + random perturbation)
- Heartbeat interval: 75ms
- Check interval: 25ms
