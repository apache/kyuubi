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
set -x

SKIP_GPG=${SKIP_GPG:-false}
RELEASE_VERSION=${RELEASE_VERSION:?"RELEASE_VERSION is required"}

exit_with_usage() {
  local NAME=$(basename $0)
  cat << EOF
Usage: $NAME <source|binary>

Top level targets are:
  source: Create source release tarball
  binary: Create binary release tarball

All other inputs are environment variables:

RELEASE_VERSION - Release version, must match pom.xml (e.g. 1.3.0-incubating)
SKIP_GPG        - (optional) Default false
EOF
  exit 1
}

KYUUBI_DIR="$(cd "$(dirname "$0")"/../..; pwd)"
RELEASE_DIR="${KYUUBI_DIR}/work/release"

SHASUM="sha512sum"
if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
fi

package_source() {
  SRC_TGZ_FILE="kyuubi-${RELEASE_VERSION}-source.tgz"
  SRC_TGZ="${RELEASE_DIR}/${SRC_TGZ_FILE}"

  mkdir -p "${RELEASE_DIR}"
  rm -f "${SRC_TGZ}*"

  echo "Creating source release tarball ${SRC_TGZ_FILE}"

  git archive --prefix="kyuubi-${RELEASE_VERSION}-source/" -o "${SRC_TGZ}" HEAD

  if [ "$SKIP_GPG" == "false" ] ; then
    gpg --armor --detach-sig "${SRC_TGZ}"
  fi
  (cd "${RELEASE_DIR}" && $SHASUM "${SRC_TGZ_FILE}" > "${SRC_TGZ_FILE}.sha512")
}

package_binary() {
  BIN_TGZ_FILE="kyuubi-${RELEASE_VERSION}-bin.tgz"
  BIN_TGZ="${RELEASE_DIR}/${BIN_TGZ_FILE}"

  mkdir -p "${RELEASE_DIR}"
  rm -f "${BIN_TGZ}*"

  echo "Creating binary release tarball ${BIN_TGZ_FILE}"

  ${KYUUBI_DIR}/build/dist --tgz --spark-provided -Pkyuubi-extension-spark-3-1

  cp "${BIN_TGZ_FILE}" "${RELEASE_DIR}"

  if [ "$SKIP_GPG" == "false" ] ; then
    gpg --armor --detach-sig "${BIN_TGZ}"
  fi
  (cd "${RELEASE_DIR}" && $SHASUM "${BIN_TGZ_FILE}" > "${BIN_TGZ_FILE}.sha512")
}

if [[ "$1" == "source" ]]; then
  package_source
  exit 0
fi

if [[ "$1" == "binary" ]]; then
  package_binary
  exit 0
fi

exit_with_usage
