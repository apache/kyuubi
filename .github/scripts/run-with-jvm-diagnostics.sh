#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Diagnostic wrapper for Linux CI; does not change Maven/test arguments.
set -uo pipefail
diagnostics_dir=${KYUUBI_DIAGNOSTICS_DIR:-${RUNNER_TEMP:-/tmp}/kyuubi-ci-diagnostics}
quiet_seconds=${KYUUBI_DIAGNOSTICS_QUIET_SECONDS:-180}
time_limit=${KYUUBI_DIAGNOSTICS_TIME_LIMIT:-5h}
mkdir -p "$diagnostics_dir"
output_file="$diagnostics_dir/console.log"
touch "$output_file"

capture() {
  local stamp pid mainclass sample
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  for sample in 1 2 3; do
    jps -l > "$diagnostics_dir/$stamp-$sample-processes.txt"
    while read -r pid mainclass; do
      case "$mainclass" in
        *Jps|*JCmd) continue ;;
      esac
      timeout 15s jcmd "$pid" Thread.print -l \
        > "$diagnostics_dir/$stamp-$sample-$pid.txt" 2>&1 || true
    done < "$diagnostics_dir/$stamp-$sample-processes.txt"
    if (( sample < 3 )); then sleep 5; fi
  done
}

timeout --signal=TERM --kill-after=2m "$time_limit" "$@" \
  > >(tee -a "$output_file") 2>&1 &
command_pid=$!
(
  sleeper_pid=
  trap 'if [[ -n "$sleeper_pid" ]]; then kill "$sleeper_pid" 2>/dev/null || true; fi; exit 0' TERM
  pause() {
    sleep "$1" &
    sleeper_pid=$!
    wait "$sleeper_pid"
    sleeper_pid=
  }
  while kill -0 "$command_pid" 2>/dev/null; do
    pause 30
    now=$(date +%s)
    modified=$(stat -c %Y "$output_file")
    if (( now - modified >= quiet_seconds )); then
      capture
      # Space repeated snapshots while preserving the original console output.
      pause 30
    fi
  done
) &
watchdog_pid=$!
wait "$command_pid"
result=$?
kill "$watchdog_pid" 2>/dev/null || true
wait "$watchdog_pid" 2>/dev/null || true
exit "$result"
