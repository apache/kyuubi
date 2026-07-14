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

# Resolves the download URL (${mirror}/${name}${query}) for each requested
# engine's binary archive from the Maven build properties and writes them to
# $GITHUB_OUTPUT as <engine>-url, to be consumed by the download-archive action.
#
# Usage: resolve-engine-archive-urls.sh <engines> [maven-args...]
#   <engines>    comma-separated engine list, e.g. "spark,hive,flink" or "flink"
#   maven-args   forwarded to build/mvn so the caller passes the same profiles
#                and -D overrides as the build (e.g. -Pspark-3.5 -Pscala-2.13)

set -euo pipefail

IFS=',' read -r -a engines <<< "${1:?Usage: $0 <engines> [maven-args...]}"
shift

for engine in "${engines[@]}"; do
  mirror=$(./build/mvn -N -q help:evaluate -Dexpression="${engine}.archive.mirror" -DforceStdout "$@")
  name=$(./build/mvn -N -q help:evaluate -Dexpression="${engine}.archive.name" -DforceStdout "$@")
  query=$(./build/mvn -N -q help:evaluate -Dexpression="${engine}.archive.query" -DforceStdout "$@")
  url="${mirror}/${name}${query}"
  echo "${engine}: ${url}"
  echo "${engine}-url=${url}" >> "$GITHUB_OUTPUT"
done
