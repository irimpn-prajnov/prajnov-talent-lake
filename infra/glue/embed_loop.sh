#!/usr/bin/env bash
# embed_loop.sh — retriggers prajnov-shared-embed-upsert until no candidates remain
# Usage: bash embed_loop.sh [in-progress-run-id]

PROFILE="201727487159_AdministratorAccess"
REGION="ap-south-1"
JOB="prajnov-shared-embed-upsert"
POLL=60

wait_for_run() {
  local rid=$1
  echo "  Watching $rid ..."
  while true; do
    STATE=$(aws glue get-job-run --job-name "$JOB" --run-id "$rid" \
      --profile "$PROFILE" --region "$REGION" \
      --query 'JobRun.{s:JobRunState,t:ExecutionTime}' --output text 2>/dev/null)

    DB=$(ssh ec2-user@13.202.247.12 "python3 -c \"
import psycopg2
conn = psycopg2.connect(host='ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com', dbname='recruiter', user='prajnov', password='Prajn0v-Recruiter-2026!')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FILTER (WHERE embedding_titan IS NOT NULL), COUNT(*) FILTER (WHERE embedding_titan IS NULL) FROM candidates')
r = cur.fetchone()
print(str(r[0]) + '|' + str(r[1]))
conn.close()
\"" 2>/dev/null)

    EMBEDDED=$(echo "$DB" | cut -d'|' -f1)
    PENDING=$(echo "$DB" | cut -d'|' -f2)

    echo "  $(date '+%H:%M:%S')  [$STATE]  embedded=$EMBEDDED  pending=$PENDING"

    echo "$STATE" | grep -qE "SUCCEEDED|TIMEOUT" && return 0
    echo "$STATE" | grep -qE "FAILED|STOPPED|ERROR" && echo "  Run $rid ended with $STATE — aborting." && exit 1
    sleep $POLL
  done
}

# Wait for already-running job if passed as arg
if [[ $# -ge 1 ]]; then
  echo "=== Waiting for in-progress run: $1 ==="
  wait_for_run "$1"
fi

run_num=1
while true; do
  echo ""
  echo "=== Triggering run #$run_num ==="
  RID=$(aws glue start-job-run --job-name "$JOB" \
    --profile "$PROFILE" --region "$REGION" \
    --query 'JobRunId' --output text)
  echo "  Run ID: $RID"

  wait_for_run "$RID"
  echo "  Run #$run_num SUCCEEDED."

  # Check remaining via logs
  REMAINING=$(aws logs get-log-events \
    --log-group-name /aws-glue/python-jobs/output \
    --log-stream-name "$RID" \
    --profile "$PROFILE" --region "$REGION" \
    --query 'events[*].message' --output text 2>/dev/null \
    | grep "Candidates to embed" | grep -oE '[0-9]+' | head -1)

  echo "  Candidates reported in run: ${REMAINING:-unknown}"

  if [[ "$REMAINING" == "0" ]]; then
    echo "=== All candidates embedded after $run_num run(s). Done. ==="
    break
  fi

  run_num=$((run_num + 1))
  sleep 10
done
