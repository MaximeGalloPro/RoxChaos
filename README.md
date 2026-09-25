# RoxChaos

External Docker chaos harness for the durable RoxIA, RoxAPI, and RoxTune workflow path.

The harness captures the two workflows currently configured for organization `CD06`, rebuilds them without preserving database IDs, starts one input for each workflow in parallel, kills the inference worker, RoxAPI, and Solid Queue, then verifies recovery after restart.

## Safety

- The stack uses a Compose project in the `roxchaos` namespace, localhost-only ports `3180` and `8180`, dedicated Redis volumes, and databases whose names must start with `roxchaos_`.
- Database reset is refused if either configured database name does not start with `roxchaos_`.
- Existing `roxia`, `roxapi`, and `roxtune` containers and volumes are not stopped or removed.
- The captured manifest contains one real local input row per workflow. The manifest, generated inputs, and detailed reports use mode `0600` and are ignored by Git.
- Teardown removes the dedicated MySQL databases, Compose resources, and generated input files. It refuses paths outside `RoxChaos/runtime` or directories without the RoxChaos ownership marker.
- `roxtrain-worker` is intentionally excluded because training has no dry-run implementation.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
cp .env.example .env
```

The default database settings target the MariaDB exposed on `host.docker.internal:3307`. Change only the credentials in `.env`; keep dedicated database names beginning with `roxchaos_`.

## Capture

The current RoxIA `web` container must be running. Capture the organization, all rule prompts/examples, both workflow task chains, and one input row per workflow:

```bash
.venv/bin/roxchaos capture
```

The source files default to:

```text
../RoxIA/documents/extract_4_TAB.output.csv
../RoxIA/documents/AUDIT_extract_TEST_TAB.output.csv
```

Override `ROXCHAOS_NORMAL_INPUT` or `ROXCHAOS_AUDIT_INPUT` if those files move.

## Run

```bash
.venv/bin/roxchaos run
```

The command builds the three current branches, resets only the dedicated databases, loads the declarative manifest, and runs the pytest scenario. Set `ROXCHAOS_KEEP_STACK=1` to preserve failed containers for inspection.

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
- Each workflow commits exactly one `Analysis`.
- Each workflow commits exactly 20 durable items, 9 external jobs, and 21 successful task logs for the captured configuration.
- The selected in-flight job is pending and has no result immediately after the worker crash.
- No duplicate workflow item, successful task log, reference tag, or document log is committed.
- RoxAPI exposes one successful result per stable job identity.
- The complete RoxAPI inventory exactly matches the external jobs recorded by RoxIA.
- Every idempotency key still maps to its original job ID.
- The inference consumer group has no pending messages and the stream is empty.

Reports include source commit SHAs, dirty-worktree flags, image metadata, tool versions, and the manifest SHA-256. Input values and inference payloads are omitted from reports.

The worker may begin an inference attempt more than once after a crash. The tested guarantee is one fenced terminal result and one committed business side effect, not exactly-once model invocation.
