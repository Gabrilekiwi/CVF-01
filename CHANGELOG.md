# Changelog

## 0.3.1 — 2026-08-03

Corrective Phase 3 hardening release. It supersedes `v0.3.0` without rewriting that published
tag. The release contract includes fixed-data and source-tree gates, a final distribution build,
an isolated clean-wheel smoke test, hosted CI, merge, tag, and GitHub Release publication.

- Route live collection, standard replay, and fixed-data acceptance through one
  `FeatureRuntime` and one receive-time feature timeline.
- Add bounded receive-time reordering, fail-closed late-event handling, quiet-period
  wall-clock ticks, and exact boundary tests.
- Persist exact post-dedup normalized events and stream-health transitions beside raw
  public payloads so connector generation, deduplication, and health semantics can be
  replayed.
- Add an exclusive `IN_PROGRESS`/`CLEAN_END` collection manifest, terminal-watermark
  reconciliation, strict journal/raw source modes, and an interruption sentinel for raw
  compaction.
- Make standard clean-journal replay fail closed before creating output when capture and current
  code version, package-source SHA-256, strategy version, or settings SHA-256 differ, while
  preserving explicit raw recovery and evidence-free legacy `auto` replay.
- Make single-venue IDs bind all semantic inputs, configuration, and code version while
  excluding nondeterministic normalization wall time.
- Replace repeated whole-window source canonicalization with the versioned, mergeable
  `sha256-sum-xor-v1` multiset commitment; add incremental price-window aggregates and explicit
  book synchronization epochs while preserving exact observable feature values.
- Bind cross-venue IDs to the count and SHA-256 of the complete prior paired-spread history,
  and select stream health as-of each decision boundary.
- Treat zero-variance Z-scores and absent source age as unavailable instead of fabricated
  zeroes; reset derived histories on book-generation changes and restart book warmup on an
  observed same-generation synchronization loss/recovery epoch.
- Make feature persistence restart-idempotent with a rebuildable SQLite sidecar; propagate
  writer failures and reject malformed, empty, mixed, or unknown-schema feature trees.
- Guard each feature root with an exclusive cross-process ownership lock so concurrent processes
  cannot write Parquet or rebuild the SQLite sidecar against the same destination.
- Close collector/event-bus cancellation races and make event-bus, writer, feature-runtime, and
  collector close/rebuild lifecycles complete under repeated cancellation; add reservation/write
  ordering and crash-stale WAL cleanup.
- Move raw ordering and UUID/content audit to bounded disk-backed scratch state and close
  Parquet/SQLite resources on early exit.
- Replace fabricated safety counters with a fail-closed runtime component inventory and
  observed output-event types.
- Remove Phase 4 scoring, signal-threshold, execution, risk, and exit settings from the
  active Phase 3 configuration.

The authoritative 2026-07-31 corrective fixed-data acceptance under
`data/processed/phase3-acceptance/fixed-30m-v0.3.1-final-2` processed 2,331,346 raw records
twice, produced 32,490 snapshots per run (21,660 single-venue and 10,830 cross-venue), matched
logical digest
`09ebc2e9039ad04705d7bae65452c84507458f7a064d6c274205019396e38ba2`,
reported zero no-lookahead/forbidden-output violations, and ran at 1598.962/1588.812 raw
records/s and 1.237678×/1.229822× event time across 2925.389 seconds of fixed-replay
observation. It binds package-source SHA-256
`5e05912737c52a21d9d075d301bee90ad00026deafba085c65da9ea87c7e7d12`.
All 275 tests pass; Ruff, strict mypy, and `pip check` also pass.

The complete `fixed-30m-v0.3.1-final` run remains historical evidence bound to the older
package-source SHA-256
`0324bb5110eff55205298c61b788dd1ae0cab58490c1f1844b6746f2a9d5b5db`; it is not evidence for
the current package. The still earlier `fixed-30m-v0.3.1` directory is an interrupted diagnostic,
not completed acceptance evidence. The continuous six-hour public-feed soak remains a pending
operational acceptance; fixed replay duration is not presented as a substitute.
Exact live-journal equivalence from a matching-lineage `CLEAN_END` capture also remains pending,
and a sustained fully warm, healthy, aligned cross-venue path has not been demonstrated. Every
one of the 32,490 snapshots in each authoritative fixed replay carried at least one structured
unavailable reason.

Previous wheel/sdist candidate hashes are retired. The release distributions were rebuilt after
the README reached its stable release state, then installed with dependencies into a fresh venv.
That environment reported package/module version `0.3.1`, passed `pip check`, loaded `cvf` from
its own `site-packages`, exposed every release CLI, and ran offline `cvf --once` with both
connectors reporting `network_attempted=false`:

- wheel SHA-256:
  `496954e4682b10a3f1ab0dbf276d7cf695e9e0231cd1027e4840f863a0a34cd9`;
- sdist SHA-256:
  `f489b15b041a243c303d632ef216536e93f1f12e10886059f190de9aad998aaf`.

## 0.3.0

Initial published Phase 3C/3D release with cross-venue features, feature Parquet
persistence, fixed-data acceptance, and release artifacts. The independent post-release
audit found the correctness and evidence gaps addressed by `0.3.1`.
