#!/usr/bin/env bash
# Report on the latest prod run of each scheduled job (prod-triage workflow).
#
#   prod_runs.sh report <file>   write failed runs and their task errors to <file>;
#                                print "failed=<n>" and "transient=<true|false>"
#   prod_runs.sh repair          repair each failed run once (failed tasks and the ones
#                                after them), wait, and print "fixed=<true|false>"
#
# Needs the Databricks CLI signed in to prod (DATABRICKS_HOST, _CLIENT_ID, _CLIENT_SECRET).
set -euo pipefail

JOBS=("energy-daily" "energy-forecast-archive" "energy-weekly-weather")
WINDOW_HOURS=26
# Errors worth one automatic retry: the source, not our code. 28 Nov 2026 was the first
# (Open-Meteo returned broken JSON, then hung).
TRANSIENT='HTTP ?(429|5[0-9][0-9])|Too Many Requests|Service Unavailable|Bad Gateway|Gateway Time-?out|Read timed out|ConnectTimeout|ConnectionError|Connection (reset|aborted|refused)|RemoteDisconnected|IncompleteRead|ChunkedEncodingError|JSONDecodeError|Expecting .* delimiter|Unterminated string|TIMEDOUT'

failed_runs() {
  # One line per job whose latest run, started in the window, didn't succeed: "<job> <run_id>"
  local since=$(( ($(date +%s) - WINDOW_HOURS * 3600) * 1000 ))
  local jobs; jobs=$(databricks jobs list -o json)
  for name in "${JOBS[@]}"; do
    local id; id=$(jq -r --arg n "$name" '.[] | select(.settings.name == $n) | .job_id' <<<"$jobs")
    [ -n "$id" ] || continue
    databricks jobs list-runs --job-id "$id" --completed-only --limit 1 -o json |
      jq -r --arg n "$name" --argjson since "$since" \
        '.[] | select(.start_time >= $since and .state.result_state != "SUCCESS")
             | "\($n) \(.run_id)"'
  done
}

report() {
  local out=$1 n=0 transient=true
  : >"$out"
  while read -r name run_id; do
    [ -n "$run_id" ] || continue
    n=$((n + 1))
    local run; run=$(databricks jobs get-run "$run_id" -o json)
    {
      echo "## $name: $(jq -r '.state.result_state' <<<"$run")"
      echo
      echo "Run $run_id, started $(jq -r '.start_time / 1000 | todate' <<<"$run"): $(jq -r '.run_page_url' <<<"$run")"
      echo
    } >>"$out"
    # Every attempt of every task that didn't succeed, oldest first.
    while read -r task_run task_key state; do
      local output; output=$(databricks jobs get-run-output "$task_run" -o json 2>/dev/null || echo '{}')
      local text; text=$(jq -r '[.error // "", (.logs // "" | split("\n") | .[-40:] | join("\n"))] | join("\n")' <<<"$output")
      {
        echo "### $task_key ($state), task run $task_run"
        echo
        echo '```'
        echo "$text" | tail -n 50
        echo '```'
        echo
      } >>"$out"
      grep -Eq "$TRANSIENT" <<<"$state $text" || transient=false
    done < <(jq -r '.tasks[] | select(.state.result_state != null and .state.result_state != "SUCCESS"
                                       and .state.result_state != "EXCLUDED")
                     | "\(.run_id) \(.task_key) \(.state.result_state)"' <<<"$run")
  done < <(failed_runs)
  [ "$n" -gt 0 ] || transient=false
  echo "failed=$n"
  echo "transient=$transient"
}

repair() {
  local fixed=true
  while read -r name run_id; do
    [ -n "$run_id" ] || continue
    local run; run=$(databricks jobs get-run "$run_id" -o json)
    if [ "$(jq '.repair_history // [] | length' <<<"$run")" -gt 1 ]; then
      echo "$name run $run_id was already repaired once; leaving it" >&2
      fixed=false
      continue
    fi
    local tasks; tasks=$(jq <<<"$run" -c '[.tasks[] | select(.state.result_state != null and .state.result_state != "SUCCESS"
                                and .state.result_state != "EXCLUDED") | .task_key] | unique')
    echo "Repairing $name run $run_id: $tasks" >&2
    databricks jobs repair-run --json "{\"run_id\": $run_id, \"rerun_tasks\": $tasks, \"rerun_dependent_tasks\": true}" \
      --timeout 45m >/dev/null || true
    local state; state=$(databricks jobs get-run "$run_id" -o json | jq -r '.state.result_state')
    echo "$name run $run_id after repair: $state" >&2
    [ "$state" = "SUCCESS" ] || fixed=false
  done < <(failed_runs)
  echo "fixed=$fixed"
}

case "${1:-}" in
  report) report "$2" ;;
  repair) repair ;;
  *) echo "usage: $0 report <file> | repair" >&2; exit 2 ;;
esac
