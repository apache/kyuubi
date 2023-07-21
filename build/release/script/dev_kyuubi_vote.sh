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
# release version, e.g. v1.7.0
release_version=${release_version:-""}
# release candidate number, e.g. rc2
release_rc_no=${release_rc_no:-""}
# previous release candidate number, e.g. rc1, could be empty if it is the first vote
prev_release_rc_no=${prev_release_rc_no:-""}
# previous release version, e.g. v1.7.0, this is used to generate change log
prev_release_version=${prev_release_version:-""}
# staging repository number, check it under https://repository.apache.org/content/repositories
repo_no=${repo_no:-""}
################################################

if [[ -z $release_version ]]; then
  echo "Please input release version"
  exit 1
fi
if [[ -z $release_rc_no ]]; then
  echo "Please input release rc number"
  exit 1
fi
if [[ -z $prev_release_version ]]; then
  echo "Please input prev release version which is used to generate change log"
  exit 1
fi
if [[ -z $repo_no ]]; then
  echo "Please input staging repository number, check it under https://repository.apache.org/content/repositories "
  exit 1
fi

release_rc_tag=${release_version}-${release_rc_no}
git_commit_hash=$(git rev-list -n 1 $release_rc_tag)

echo "Release version: ${release_version}"
echo "Release candidate number: ${release_rc_no}"
echo "Previous release candidate number: ${prev_release_rc_no}"
echo "Staging repository number: ${repo_no}"
echo "Release candidate tag: ${release_rc_tag}"
echo "Release candidate tag commit hash: ${git_commit_hash}"

if [[ ! -z "$prev_release_rc_no" ]]; then
  prev_release_rc_tag=${release_version}-${prev_release_rc_no}
  change_from_pre_commit="
The commit list since the ${prev_release_rc_no}:
https://github.com/apache/kyuubi/compare/${prev_release_rc_tag}...${release_rc_tag}
"
fi

RELEASE_TEMP_DIR=${RELEASE_DIR}/tmp
mkdir -p ${RELEASE_TEMP_DIR}
DEV_VOTE=${RELEASE_TEMP_DIR}/${release_rc_tag}_dev_vote.temp

cat >${DEV_VOTE}<<EOF
Title: [VOTE] Release Apache Kyuubi ${release_version} ${release_rc_no}

Content:
Hello Apache Kyuubi PMC and Community,

Please vote on releasing the following candidate as
Apache Kyuubi version ${release_version}.

The VOTE will remain open for at least 72 hours.

[ ] +1 Release this package as Apache Kyuubi ${release_version}
[ ] +0
[ ] -1 Do not release this package because ...

To learn more about Apache Kyuubi, please see
https://kyuubi.apache.org/

The tag to be voted on is ${release_rc_tag} (commit ${git_commit_hash}):
https://github.com/apache/kyuubi/tree/${release_rc_tag}

The release files, including signatures, digests, etc. can be found at:
https://dist.apache.org/repos/dist/dev/kyuubi/${release_rc_tag}/

Signatures used for Kyuubi RCs can be found in this file:
https://downloads.apache.org/kyuubi/KEYS

The staging repository for this release can be found at:
https://repository.apache.org/content/repositories/orgapachekyuubi-${repo_no}/
${change_from_pre_commit}
The release note is available in:
https://github.com/apache/kyuubi/compare/${prev_release_version}...${release_rc_tag}

Thanks,
On behalf of Apache Kyuubi community
EOF

echo "please use dev@kyuubi.apache.com
see vote content in $DEV_VOTE
please check all the links and ensure they are available"
