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

RELEASE_DIR="$(cd "$(dirname "$0")"/..; pwd)"

######### Please modify the variables ##########
# release version, e.g. 1.7.1
RELEASE_VERSION=${RELEASE_VERSION:-""}
# release candidate number, e.g. 2
RELEASE_RC_NO=${RELEASE_RC_NO:-""}
# previous release candidate number, e.g. 1, could be empty if it is the first vote
PREV_RELEASE_RC_NO=${PREV_RELEASE_RC_NO:-""}
# previous release version, e.g. 1.7.0, this is used to generate change log
PREV_RELEASE_VERSION=${PREV_RELEASE_VERSION:-""}
# staging repository number, check it under https://repository.apache.org/content/repositories
REPO_NO=${REPO_NO:-""}
################################################

if [[ -z $RELEASE_VERSION ]]; then
  echo "Please input RELEASE_VERSION, e.g. 1.7.1"
  exit 1
fi
if [[ -z $RELEASE_RC_NO ]]; then
  echo "Please input RELEASE_RC_NO, e.g. 2"
  exit 1
fi
if [[ -z $PREV_RELEASE_VERSION ]]; then
  echo "Please input PREV_RELEASE_VERSION which is used to generate change log, e.g. 1.7.0"
  exit 1
fi
if [[ -z $REPO_NO ]]; then
  echo "Please input staging repository number, check it under https://repository.apache.org/content/repositories "
  exit 1
fi

RELEASE_RC_TAG=v${RELEASE_VERSION}-rc${RELEASE_RC_NO}
GIT_COMMIT_HASH=$(git rev-list -n 1 $RELEASE_RC_TAG)

echo "Release version: v${RELEASE_VERSION}"
echo "Release candidate number: RC${RELEASE_RC_NO}"
echo "Previous release candidate number: RC${PREV_RELEASE_RC_NO}"
echo "Staging repository number: ${REPO_NO}"
echo "Release candidate tag: ${RELEASE_RC_TAG}"
echo "Release candidate tag commit hash: ${GIT_COMMIT_HASH}"

if [[ ! -z "$PREV_RELEASE_RC_NO" ]]; then
  PREV_RELEASE_RC_TAG=v${RELEASE_VERSION}-rc${PREV_RELEASE_RC_NO}
  CHANGE_FROM_PRE_COMMIT="
The commit list since the previous RC:
https://github.com/apache/kyuubi/compare/${PREV_RELEASE_RC_TAG}...${RELEASE_RC_TAG}
"
fi

RELEASE_TEMP_DIR=${RELEASE_DIR}/tmp
mkdir -p ${RELEASE_TEMP_DIR}
DEV_VOTE=${RELEASE_TEMP_DIR}/${RELEASE_RC_TAG}_dev_vote.temp

cat >${DEV_VOTE}<<EOF
Title: [VOTE] Release Apache Kyuubi v${RELEASE_VERSION} RC${RELEASE_RC_NO}

Content:
Hello Apache Kyuubi PMC and Community,

Please vote on releasing the following candidate as
Apache Kyuubi v${RELEASE_VERSION}.

The VOTE will remain open for at least 72 hours.

[ ] +1 Release this package as Apache Kyuubi v${RELEASE_VERSION}
[ ] +0
[ ] -1 Do not release this package because ...

To learn more about Apache Kyuubi, please see
https://kyuubi.apache.org/

The tag to be voted on is ${RELEASE_RC_TAG} (commit ${GIT_COMMIT_HASH}):
https://github.com/apache/kyuubi/tree/${RELEASE_RC_TAG}

The release files, including signatures, digests, etc. can be found at:
https://dist.apache.org/repos/dist/dev/kyuubi/${RELEASE_RC_TAG}/

Signatures used for Kyuubi RCs can be found in this file:
https://downloads.apache.org/kyuubi/KEYS

The staging repository for this release can be found at:
https://repository.apache.org/content/repositories/orgapachekyuubi-${REPO_NO}/
${CHANGE_FROM_PRE_COMMIT}
The commit list since the latest released version:
https://github.com/apache/kyuubi/compare/v${PREV_RELEASE_VERSION}...${RELEASE_RC_TAG}

Thanks,
On behalf of Apache Kyuubi community
EOF

echo "please use dev@kyuubi.apache.com
see vote content in $DEV_VOTE
please check all the links and ensure they are available"
