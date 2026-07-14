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

# Resolves the *_HOME variable for each requested engine from the archives
# extracted under /opt by the download-archive action, and appends them to
# $GITHUB_ENV. Each home is matched by its unique directory prefix rather than
# derived from the archive name, since the extracted top-level directory does
# not always match (e.g. Flink drops the -bin-scala suffix).
#
# Usage: expose-engine-homes.sh <engines>
#   <engines>  comma-separated engine list, e.g. "spark,hive,flink" or "spark"

set -euo pipefail

expose_home() {
  local engine="$1" var pattern
  case "$engine" in
    spark) var=SPARK_HOME; pattern='/opt/spark-*' ;;
    hive)  var=HIVE_HOME;  pattern='/opt/apache-hive-*' ;;
    flink) var=FLINK_HOME; pattern='/opt/flink-*' ;;
    *) echo "Unknown engine: $engine" >&2; exit 1 ;;
  esac
  local dirs=($pattern)
  if [[ ${#dirs[@]} -ne 1 || ! -d "${dirs[0]}" ]]; then
    echo "Expected exactly one directory matching '$pattern', found: ${dirs[*]}" >&2
    exit 1
  fi
  echo "$var=${dirs[0]}" | tee -a "$GITHUB_ENV"
}

IFS=',' read -r -a engines <<< "${1:?Usage: $0 <engines>}"
for engine in "${engines[@]}"; do
  expose_home "$engine"
done
