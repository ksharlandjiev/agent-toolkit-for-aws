# The step-by-step upgrade (version-by-version)

The core upgrade-and-validate procedure every approach uses. It works on one
environment at a time: the new environment (New-environment upgrade), the
test copy then your current environment (Rehearsed in-place upgrade), or
your current environment (Direct in-place upgrade). Walks an ordered upgrade
path; version numbers are never hardcoded.

## Inputs (supplied by the calling approach)

- **Environment name** — the environment this engine mutates.
- **Upgrade path** — ordered version jumps from the plan, each with `from`,
  `to`, `python_change`, `crosses_major`.
- **Starting condition**:
  - **newly created** (validate at the starting version, then upgrade
    through the remaining version jumps) — the environment was created by
    the approach at version jump 1's `to`.
  - **upgraded in place** (upgrade through every version jump, starting at
    the first) — the environment currently sits at version jump 1's `from`.

## Per-version-jump confirmation

Every `aws mwaa update-environment` (every version jump) requires its own
explicit user confirmation. State rollback options at each in-place version
jump: downgrade from 3.x to 2.11.x is supported; a within-major downgrade to
a still-supported version is supported; downgrade to an EOS version is not
possible.

## Validate / scan-logs / fix loop (run after an env reaches a version V)

1. Deploy the current DAG set to the environment; wait for parsing.
1a. Unpause all deployed DAGs. Newly synced DAGs register as paused by default
   on MWAA (unless `core.dags_are_paused_at_creation` is `False`).
   **New-environment upgrade:** while a DAG is unpaused on both the new
   environment and your current environment, its schedule runs on both — a
   non-idempotent DAG executes twice, and DAGs that drive named AWS resources
   (Glue jobs, crawlers, EMR applications, S3 output prefixes) collide unless
   those names are namespaced per environment. Before unpausing, confirm with
   the user that duplicate scheduled runs are acceptable for the validation
   window; otherwise keep the DAGs paused and let the testing skill's manual
   triggers drive validation (unpause just before each test trigger and pause
   again afterwards). Phase 8 switchover then owns the final unpause.
   For each DAG in the deployed set:

   ```
   aws mwaa invoke-rest-api --name <env> --method PATCH \
     --path /dags/<dag_id> --body '{"is_paused":false}'
   sleep 0.2
   ```

   (`invoke-rest-api` has a 10-second timeout and a 6 MB response cap
   ([docs](https://docs.aws.amazon.com/mwaa/latest/userguide/access-mwaa-apache-airflow-rest-api.html));
   it is also throttled per environment, so the sleep spaces calls out — prefer
   exponential backoff on a throttling error over assuming a fixed rate.)

   **64-char `--path` caveat (applies to every per-DAG pause/unpause in this
   skill — step 1a here, the AF2 pause loops below, and the batched switchover in
   SKILL.md):** the per-DAG path `/dags/<dag_id>` can exceed `invoke-rest-api`'s
   64-char `--path` limit for long `dag_id`s, and the PATCH is then rejected.
   For an over-length `dag_id`, pause/unpause via the Airflow CLI
   (`aws mwaa create-cli-token` -> `airflow dags pause|unpause <dag_id>`), which
   has no path-length limit; on AF3 the bulk collection form
   `--path /dags --query-parameters '{"dag_id_pattern":"<dag_id>"}'` also avoids
   the per-DAG path.
2. Check `has_import_errors` is false for all DAGs
   (`invoke-rest-api --path /dags`).
3. **Scan for deprecations** when the NEXT version jump crosses into a new
   major version (this version is the required stepping-stone before the
   crossing). The authoritative sources are the Ruff AIR scan
   (`ruff check --preview --select AIR .`) and the Phase 6 Docker parse with
   warnings enabled ([docker-validation.md](docker-validation.md)). A CloudWatch
   search of scheduler and task logs for `RemovedInAirflow3Warning` /
   `RemovedInAirflow{N}Warning` / `DeprecationWarning` is supplementary only: on
   MWAA it returns platform-internal warnings (metrics timer units, `fork()`),
   and user-DAG deprecations such as `days_ago` or `schedule_interval` do not
   appear there. Never record "no warnings in logs" as evidence that the DAGs
   are clean. Fix using
   [airflow2-to-3-quick-reference.md](airflow2-to-3-quick-reference.md) and
   [airflow2-to-3-checklist.md](airflow2-to-3-checklist.md); re-deploy until the
   Ruff scan and the Docker parse are clean.
4. **Python-line re-validation:** if the just-reached version jump was
   flagged `python_change` (it crossed a Python-line boundary, e.g. 3.11 to
   3.12), run a dedicated requirements re-validation against the new
   Python-line constraints file and flag compiled-dependency conflicts
   before continuing. This is the generalized reason the intermediate step
   exists (to move Python safely and isolate compiled-dependency risk); it
   applies whether or not the version jump also crosses a major version.
   Also confirm `GET /importErrors` is empty: an unchanged DAG that imports a
   standard-library module removed in the new Python line (for 3.12: `imp`,
   `asynchat`, `asyncore`, `smtpd`) disappears from `/dags` at this point.
   Hand any such error to `debugging-mwaa-workflow` (drift) and fix through
   `authoring-mwaa-workflow`.
5. **Same-major version jump with no crossing ahead:** rely on `GET
   /importErrors` and the Phase 6 Docker parse; a CloudWatch scan for
   `DeprecationWarning` is best-effort only (see step 3 — MWAA logs do not
   carry user-DAG warnings).

## Steps

### Newly created

- **Validate at the starting version (version jump 1's `to`):** set
  `current_jump: 1`, `jump_status: validating`, then run the validate /
  scan-logs / fix loop (so a resume during this window is unambiguous). If
  there is no version jump 2, version jump 1's `to` is the target — validate
  and go to Finish.
- **For each remaining version jump j = 2..N:** set requirements to
  `jump_j.to`'s constraint URL (apply provider changes from the quick
  reference if `crosses_major`); **pause all DAGs** before upgrading to
  prevent the scheduler from firing runs during the transition:

  **AF3 (bulk):**

  ```
  aws mwaa invoke-rest-api --name <env> --method PATCH \
    --path /dags --query-parameters '{"dag_id_pattern":"~"}' \
    --body '{"is_paused":true}'
  ```

  `"~"` is Airflow's shorthand regex for "match all DAGs".

  **AF2 (per-DAG loop, paginated):**

  ```
  # /dags returns up to 100 DAGs per call (default limit) — page through ALL of
  # them so every DAG is paused, not just the first 100 (see discovery-preflight.md)
  offset=0
  while :; do
    dag_ids=$(aws mwaa invoke-rest-api --name <env> --method GET \
      --path /dags --query-parameters "{\"limit\":\"100\",\"offset\":\"$offset\"}" \
      --query 'RestApiResponse.dags[].dag_id' --output text)
    [ -z "$dag_ids" ] && break
    n=0
    for dag_id in $dag_ids; do   # dag_ids are space-free identifiers
      aws mwaa invoke-rest-api --name <env> --method PATCH \
        --path "/dags/$dag_id" --body '{"is_paused":true}'
      sleep 0.2  # invoke-rest-api is throttled per environment
      n=$((n + 1))
    done
    [ "$n" -lt 100 ] && break
    offset=$((offset + 100))
  done
  ```

  Set plan `jump_status: upgrading`; present
  `update-environment --airflow-version <jump_j.to>` for confirmation; execute;
  poll to AVAILABLE (report every 5 minutes; stop on UPDATE_FAILED/UNAVAILABLE);
  set `jump_status: validating`; run the validate / scan-logs / fix loop
  (which unpauses DAGs in step 1a after validation confirms the new version
  is healthy); mark the version jump complete and save progress (version
  jump Status, `## Where we are`, `Last updated`) BEFORE presenting the next
  version jump; advance `current_jump`.

### Upgraded in place

- **For each version jump j = 1..N:** set requirements to `jump_j.to`'s
  constraint URL (provider changes if `crosses_major`); **pause all DAGs**
  before upgrading to prevent the scheduler from firing runs during the
  transition:

  **AF3 (bulk):**

  ```
  aws mwaa invoke-rest-api --name <env> --method PATCH \
    --path /dags --query-parameters '{"dag_id_pattern":"~"}' \
    --body '{"is_paused":true}'
  ```

  `"~"` is Airflow's shorthand regex for "match all DAGs".

  **AF2 (per-DAG loop, paginated):**

  ```
  # /dags returns up to 100 DAGs per call (default limit) — page through ALL of
  # them so every DAG is paused, not just the first 100 (see discovery-preflight.md)
  offset=0
  while :; do
    dag_ids=$(aws mwaa invoke-rest-api --name <env> --method GET \
      --path /dags --query-parameters "{\"limit\":\"100\",\"offset\":\"$offset\"}" \
      --query 'RestApiResponse.dags[].dag_id' --output text)
    [ -z "$dag_ids" ] && break
    n=0
    for dag_id in $dag_ids; do   # dag_ids are space-free identifiers
      aws mwaa invoke-rest-api --name <env> --method PATCH \
        --path "/dags/$dag_id" --body '{"is_paused":true}'
      sleep 0.2  # invoke-rest-api is throttled per environment
      n=$((n + 1))
    done
    [ "$n" -lt 100 ] && break
    offset=$((offset + 100))
  done
  ```

  Set `jump_status: upgrading`; present
  `update-environment --airflow-version <jump_j.to>` for confirmation; execute;
  poll to AVAILABLE; set `jump_status: validating`; run the validate /
  scan-logs / fix loop (which unpauses DAGs in step 1a after validation;
  scan logs when version jump j+1 is `crosses_major`); mark the version jump
  complete and save progress (version jump Status, `## Where we are`, `Last
  updated`) BEFORE presenting the next version jump; advance `current_jump`.

Example in-place command (values from the current version jump, never
hardcoded):

```
aws mwaa update-environment --name <env> \
  --airflow-version <jump.to> \
  --requirements-s3-path <requirements-path-for-jump.to>
```

Poll:

```
aws mwaa get-environment --name <env> --query "Environment.Status"
```

### Finish (after the last version jump)

- Verify health:

  ```
  # Airflow 2.x (REST API v1)
  aws mwaa invoke-rest-api --name <env> --method GET --path /health
  # Airflow 3.x (REST API v2) — /health does not exist and returns RestApiClientException
  aws mwaa invoke-rest-api --name <env> --method GET --path /monitor/health
  ```

  Expected: metadatabase=healthy, scheduler=healthy. Use the path that matches
  the environment's Airflow version AFTER the jump (an upgrade to 3.x switches
  the health path).
- Confirm `has_import_errors` is false for every DAG.
- On failure, hand off to `debugging-mwaa-workflow` with context
  ("upgrade to `<target>` on `<env>`; DAGs `<list>` have import errors"), then
  re-verify.
- Return control to the calling approach. The engine's scope is parse
  validation (health + no import errors). Approaches that use the new
  environment or the test copy for validation (New-environment upgrade,
  Rehearsed in-place upgrade) are responsible for invoking execution
  validation (`testing-mwaa-workflow`) as a required checkpoint before
  proceeding to the live upgrade or switchover. Parse success alone does
  not prove artifacts will execute correctly at the target version.

## Re-entry

The engine writes `working_on` (the environment), `current_jump` (integer
index into the upgrade path), and `jump_status` (`upgrading` while an update
is in flight, `validating` during the post-update loop). Phase 0 resumes at
that environment + version jump + status. Each write happens before the next
action, per saving progress in
[plan-materialization.md](plan-materialization.md), so the recorded position
never trails reality by more than one in-flight action. For the Rehearsed
in-place upgrade (two environments), `working_on` selects the test copy from
your current environment.
