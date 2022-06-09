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
set -o pipefail
set -e

LAST_RELEASE_TAG=$1
CURRENT_RELEASE_TAG=$2

KYUUBI_DIR="$(cd "$(dirname "$0")"/../..; pwd)"
CHANGELOG_DIR=$KYUUBI_DIR/docs/changelog
RELEASE_VERSION="$(echo $CURRENT_RELEASE_TAG | cut -d "-" -f 1,2)"  ## use cut to remove -rc symbol
CHANGELOG_PATH=$CHANGELOG_DIR/"$RELEASE_VERSION.md"
KYUUBI_GITHUB_COMMIT_URL="https://github.com/apache/incubator-kyuubi/commit/"

function usage {
  set +x
  echo "./generate_changelog.sh - Tool for generate changelog for Kyuubi"
  echo ""
  echo "Usage:"
  echo "+--------------------------------------------------------------------+"
  echo "| ./generate_changelog.sh <LAST_RELEASE_TAG> <CURRENT_RELEASE_TAG>   |"
  echo "+--------------------------------------------------------------------+"
  echo "LAST_RELEASE_TAG:     -  last release tag of kyuubi e.g. v1.5.0-incubating"
  echo "CURRENT_RELEASE_TAG:  -  current release tag of kyuubi e.g. v1.5.1-incubating-rc0"
  echo ""
}

source "$KYUUBI_DIR/build/util.sh"

if [ "${LAST_RELEASE_TAG}" = "-h" ]; then
  usage
  exit 0
fi

## Add title for changelog doc.
echo "## Changelog for Apache Kyuubi(Incubating) $RELEASE_VERSION" > $CHANGELOG_PATH
echo "" >> $CHANGELOG_PATH
## Append well-formatted git log to changelog file.
git log --pretty="[%s]($KYUUBI_GITHUB_COMMIT_URL%h)  " $LAST_RELEASE_TAG..$CURRENT_RELEASE_TAG | grep -v "\[RELEASE\]" >> $CHANGELOG_PATH
