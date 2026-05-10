Design Document: Chandy-Lamport Snapshot Algorithm

1. Goals and Non-Goals

- Record a global state: each server's token count plus in-transit token messages.
- Correct snapshots must be consistent cuts and conserve total tokens.
- Out of scope: failures, lost/duplicated/corrupt messages, non-FIFO channels, partitions, recovery, membership changes, or behavior outside the simulator model.

2. Protocol Assumptions

- Reliable delivery: every message arrives once and intact, so tokens are not lost or duplicated.
- FIFO directed channels: markers separate earlier messages from later messages on each link.
- Finite delay and sufficient connectivity: every server eventually receives all needed markers.
- Markers do not affect the underlying token-passing computation.

3. Marker Semantics

- A marker is the snapshot boundary for one channel.
- On local start, a server records its tokens and sends `MarkerMessage{snapshotId}` on all outbound links before further normal sends.
- On the first marker for a snapshot, it records tokens, treats that source channel as empty, records all other inbound channels, and forwards markers.
- On later markers for that snapshot, it stops recording that source channel.
- FIFO makes this a barrier: pre-marker messages are pre-snapshot; post-marker messages are post-snapshot.

4. Channel State Recording

- Channel state is messages sent before the sender's snapshot but received after the receiver's snapshot.
- A server records an inbound channel from its local snapshot until that channel's marker arrives.
- This captures exactly cut-crossing messages: absent from the receiver's local state but sent before the sender's marker.

5. Consistent Cut

- A consistent cut includes causal dependencies: a recorded receive implies the send is before the cut.
- The algorithm records each server once and uses FIFO markers to identify channel messages crossing the cut.

6. Concurrent Snapshots

- Concurrent snapshots are keyed by `snapshotId`; marker handling and recorded data are per id.
- Per snapshot, each server tracks recorded tokens, recorded messages, open/closed inbound channels, and completion.
- A first marker for an unknown id must start that snapshot immediately; delaying can move the local cut forward and lose in-flight messages.

7. Invariants

- Conservation: server tokens plus channel tokens equal the system total.
- Cut: post-snapshot messages are never recorded as in-flight.
- Termination: reliable FIFO delivery and connectivity eventually produce all inbound markers.

8. Limitations

- Failed processes or links can block completion.
- Non-FIFO channels can corrupt the channel boundary.
- Partitions or unreachable servers prevent a complete snapshot.
