# RoxChaos

External Docker chaos harness for the durable RoxIA, RoxAPI, and RoxTune workflow path.

The harness captures the two workflows currently configured for organization `CD06`, rebuilds them without preserving database IDs, and lets the real Solid Queue recurrence start both workflows in parallel. It runs the RoxInference and RoxTrain `TimeScheduler` entrypoints, kills RoxInference, RoxAPI, and Solid Queue during inference, then verifies durable recovery and the following scheduler handoff to RoxTrain.

## Safety

- The stack uses a Compose project in the `roxchaos` namespace, localhost-only ports `3180` and `8180`, dedicated Redis volumes, and databases whose names must start with `roxchaos_`.
- Database reset is refused if either configured database name does not start with `roxchaos_`.
- Existing `roxia`, `roxapi`, and `roxtune` containers and volumes are not stopped or removed.
- The captured manifest contains three real local input rows per workflow by default. Their six workflow identities must be globally distinct. The manifest, generated inputs, and detailed reports use mode `0600` and are ignored by Git.
- Teardown removes the dedicated MySQL databases, Compose resources, and generated input files. It refuses paths outside `RoxChaos/runtime` or directories without the RoxChaos ownership marker.
- The real RoxTrain scheduler and subprocess run against an empty training queue. No training job is submitted because training has no dry-run implementation.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
cp .env.example .env
```

The default database settings target the MariaDB exposed on `host.docker.internal:3307`. Change only the credentials in `.env`; keep dedicated database names beginning with `roxchaos_`.

## Capture

The current RoxIA `web` container must be running. Capture the organization, all rule prompts/examples, both workflow task chains, and three input rows per workflow:

```bash
.venv/bin/roxchaos capture
```

The source files default to:

```text
../RoxIA/documents/extract_4_TAB.output.csv
../RoxIA/documents/AUDIT_extract_TEST_TAB.output.csv
```

Override `ROXCHAOS_NORMAL_INPUT` or `ROXCHAOS_AUDIT_INPUT` if those files move.
Set `ROXCHAOS_INPUT_ROWS` to use more rows; the minimum is two.

## Run

```bash
.venv/bin/roxchaos run
```

The command builds the current service branches, resets only the dedicated databases, loads the declarative manifest, and runs the pytest scenario. Set `ROXCHAOS_KEEP_STACK=1` to preserve failed containers for inspection. Set `ROXCHAOS_SKIP_BUILD=1` only when the required images have already been rebuilt.

Generated reports:

```text
reports/junit.xml
reports/recovery.json
reports/recovery.md
```

Remove a preserved stack with:

```bash
.venv/bin/roxchaos down
```

## Recovery assertions

- Both workflow runs finish with `completed`.
- The `scheduled_tasks_scan` recurring execution invokes `ScheduledTasksJob`, which creates exactly one scheduled run and one `AsyncTaskJob` delivery per workflow and scheduled period.
- Both parent schedulers publish fresh heartbeats. RoxTrain exhibits the expected heartbeat gap while its real subprocess runs; RoxInference keeps refreshing its heartbeat during its subprocess.
- The shared lock begins absent, becomes present for the inference turn, then transitions `present -> absent -> present` across the following training turn.
- Both real Redis Stream consumer groups are observed, while the RoxTrain queue remains empty.
- Each workflow commits exactly 3 analyses, 60 durable items, 27 external jobs, and 61 successful task logs for the current three-row configuration.
- The selected in-flight job is pending and has no result immediately after the worker crash.
- No duplicate workflow item, successful task log, reference tag, or document log is committed.
- RoxAPI exposes one successful result per stable job identity.
- The complete RoxAPI inventory exactly matches the external jobs recorded by RoxIA.
- Every idempotency key still maps to its original job ID.
- The inference consumer group has no pending messages and the stream is empty.

Reports include source commit SHAs, dirty-worktree flags, image metadata, tool versions, and the manifest SHA-256. Input values and inference payloads are omitted from reports.

The worker may begin an inference attempt more than once after a crash. The tested guarantee is one fenced terminal result and one committed business side effect, not exactly-once model invocation.
