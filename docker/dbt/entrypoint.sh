#!/usr/bin/env bash
# Entrypoint of the dbt image. On a retry, replays only what failed.
set -uo pipefail

STATE="gs://${DBT_STATE_BUCKET}/${DAG_ID}/${RUN_ID}"
SELECT="${DBT_SELECT:-}"

if gsutil -q stat "${STATE}/run_results.json"; then
    mkdir -p /tmp/last_run
    gsutil -q cp "${STATE}/manifest.json" "${STATE}/run_results.json" /tmp/last_run/

    # a failed model skips its children, hence the +
    retry_select="result:error+"
    # comma = intersection: never step outside this task's own scope
    [ -n "$SELECT" ] && retry_select="${SELECT},${retry_select}"

    dbt run --select "$retry_select" --state /tmp/last_run
else
    if [ -n "$SELECT" ]; then
        dbt run --select "$SELECT"
    else
        dbt run
    fi
fi
rc=$?

# the next attempt reads these, so upload them on failure too
gsutil -q cp target/manifest.json target/run_results.json "${STATE}/" || true
exit $rc
