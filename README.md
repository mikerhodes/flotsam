# Flotsam

Flotsam is an implementation of Raft that I am writing to learn Raft.

Progress:

- [x] Leader election
- [x] Log replication
- [x] State machine apply
- [ ] Log compaction
- [ ] Cluster reconfiguration

It's well unit tested.

## AI

All the production code is human-written. Most of the tests are too.
Claude just filled in some of the gaps after I'd established the
patterns. I think that's a decent way to write something like this,
particularly when it's a learning experience.

Saying that, CLAUDE.md is a reasonable guide to the code if you are
interested.

I also used Claude to help refine methods --- flag up opportunities to improve readability and so on.
