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

ASF_USERNAME=${ASF_USERNAME:?"ASF_USERNAME is required"}
ASF_PASSWORD=${ASF_PASSWORD:?"ASF_PASSWORD is required"}
RELEASE_VERSION=${RELEASE_VERSION:?"RELEASE_VERSION is required, e.g. 1.7.0"}
RELEASE_RC_NO=${RELEASE_RC_NO:?"RELEASE_RC_NO is required, e.g. 0"}

exit_with_usage() {
  local NAME=$(basename $0)
  cat << EOF
Usage: $NAME <publish|finalize>

Top level targets are:
  publish: Publish tarballs to SVN staging repository and jars to Nexus staging repository
  finalize: Finalize the release after an RC passes vote

All other inputs are environment variables

RELEASE_VERSION - Release version, must match pom.xml and not be SNAPSHOT (e.g. 1.7.0)
RELEASE_RC_NO   - Release RC number, (e.g. 0)

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account
EOF
  exit 1
}

if [[ ${RELEASE_VERSION} =~ .*-SNAPSHOT ]]; then
  echo "Can not release a SNAPSHOT version: ${RELEASE_VERSION}"
  exit_with_usage
  exit 1
fi

if [ -n "${JAVA_HOME}" ]; then
  JAVA="${JAVA_HOME}/bin/java"
elif [ "$(command -v java)" ]; then
  JAVA="java"
else
  echo "JAVA_HOME is not set" >&2
  exit 1
fi

JAVA_VERSION=$($JAVA -version 2>&1 | awk -F '"' '/version/ {print $2}')
if [[ $JAVA_VERSION != 1.8.* ]]; then
  echo "Unexpected Java version: $JAVA_VERSION. Java 8 is required for release."
  exit 1
fi

RELEASE_TAG="v${RELEASE_VERSION}-rc${RELEASE_RC_NO}"

SVN_STAGING_REPO="https://dist.apache.org/repos/dist/dev/kyuubi"
SVN_RELEASE_REPO="https://dist.apache.org/repos/dist/release/kyuubi"

KYUUBI_DIR="$(cd "$(dirname "$0")"/../..; pwd)"
RELEASE_DIR="${KYUUBI_DIR}/work/release"
SVN_STAGING_DIR="${KYUUBI_DIR}/work/svn-dev"
SVN_RELEASE_DIR="${KYUUBI_DIR}/work/svn-release"

package() {
  SKIP_GPG="false" RELEASE_VERSION="${RELEASE_VERSION}" $KYUUBI_DIR/build/release/create-package.sh source
  SKIP_GPG="false" RELEASE_VERSION="${RELEASE_VERSION}" $KYUUBI_DIR/build/release/create-package.sh binary
}

upload_svn_staging() {
  rm -rf "${SVN_STAGING_DIR}"
  svn checkout --depth=empty "${SVN_STAGING_REPO}" "${SVN_STAGING_DIR}"
  mkdir -p "${SVN_STAGING_DIR}/${RELEASE_TAG}"
  rm -f "${SVN_STAGING_DIR}/${RELEASE_TAG}/*"

  SRC_TGZ_FILE="apache-kyuubi-${RELEASE_VERSION}-source.tgz"
  BIN_TGZ_FILE="apache-kyuubi-${RELEASE_VERSION}-bin.tgz"

  echo "Copying release tarballs"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}"        "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}.asc"    "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}.asc"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}.sha512" "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}.sha512"
  cp "${RELEASE_DIR}/${BIN_TGZ_FILE}"        "${SVN_STAGING_DIR}/${RELEASE_TAG}/${BIN_TGZ_FILE}"
  cp "${RELEASE_DIR}/${BIN_TGZ_FILE}.asc"    "${SVN_STAGING_DIR}/${RELEASE_TAG}/${BIN_TGZ_FILE}.asc"
  cp "${RELEASE_DIR}/${BIN_TGZ_FILE}.sha512" "${SVN_STAGING_DIR}/${RELEASE_TAG}/${BIN_TGZ_FILE}.sha512"

  svn add "${SVN_STAGING_DIR}/${RELEASE_TAG}"

  echo "Uploading release tarballs to ${SVN_STAGING_REPO}/${RELEASE_TAG}"
  (
    cd "${SVN_STAGING_DIR}" && \
    svn commit --username "${ASF_USERNAME}" --password "${ASF_PASSWORD}" --message "Apache Kyuubi ${RELEASE_TAG}"
  )
  echo "Kyuubi tarballs uploaded"
}

upload_nexus_staging() {
  # Spark Extension Plugin for Spark 3.3 and Scala 2.12
  ${KYUUBI_DIR}/build/mvn clean deploy -DskipTests -Papache-release,flink-provided,spark-provided,hive-provided,spark-3.3 \
    -s "${KYUUBI_DIR}/build/release/asf-settings.xml" \
    -pl extensions/spark/kyuubi-extension-spark-3-3 -am

  # Spark Extension Plugin for Spark 3.4 and Scala 2.12
  ${KYUUBI_DIR}/build/mvn clean deploy -DskipTests -Papache-release,flink-provided,spark-provided,hive-provided,spark-3.4 \
    -s "${KYUUBI_DIR}/build/release/asf-settings.xml" \
    -pl extensions/spark/kyuubi-extension-spark-3-4 -am

  # Spark Extension Plugin for Spark 4.0 and Scala 2.13
  ${KYUUBI_DIR}/build/mvn clean deploy -DskipTests -Papache-release,flink-provided,spark-provided,hive-provided,spark-4.0,scala-2.13 \
    -s "${KYUUBI_DIR}/build/release/asf-settings.xml" \
    -pl extensions/spark/kyuubi-extension-spark-4-0 -am

  # Spark Extension Plugin for Spark 4.1 and Scala 2.13
  ${KYUUBI_DIR}/build/mvn clean deploy -DskipTests -Papache-release,flink-provided,spark-provided,hive-provided,spark-4.1,scala-2.13 \
    -s "${KYUUBI_DIR}/build/release/asf-settings.xml" \
    -pl extensions/spark/kyuubi-extension-spark-4-1 -am

  # Spark Hive/TPC-DS/TPC-H Connector built with default Spark version (3.5) and Scala 2.13
  ${KYUUBI_DIR}/build/mvn clean deploy -DskipTests -Papache-release,flink-provided,spark-provided,hive-provided,spark-3.5,scala-2.13 \
    -s "${KYUUBI_DIR}/build/release/asf-settings.xml" \
    -pl extensions/spark/kyuubi-spark-connector-hive,extensions/spark/kyuubi-spark-connector-tpcds,extensions/spark/kyuubi-spark-connector-tpch -am

  # All modules including Spark Extension Plugin and Connectors built with default Spark version (3.5) and default Scala version (2.12)
  ${KYUUBI_DIR}/build/mvn clean deploy -DskipTests -Papache-release,flink-provided,spark-provided,hive-provided,spark-3.5 \
    -s "${KYUUBI_DIR}/build/release/asf-settings.xml"
}

finalize_svn() {
  echo "Moving Kyuubi tarballs to the release directory"
  svn mv --username "${ASF_USERNAME}" --password "${ASF_PASSWORD}" --no-auth-cache \
     --message "Apache Kyuubi ${RELEASE_VERSION}" \
     "${SVN_STAGING_REPO}/${RELEASE_TAG}" "${SVN_RELEASE_REPO}/kyuubi-${RELEASE_VERSION}"
  echo "Kyuubi tarballs moved"
}

if [[ "$1" == "publish" ]]; then
  package
  upload_svn_staging
  upload_nexus_staging
  exit 0
fi

if [[ "$1" == "finalize" ]]; then
  echo "THIS STEP IS IRREVERSIBLE! Make sure the vote has passed and you pick the right RC to finalize."
  read -p "You must be a PMC member to run this step. Continue? [y/N] " ANSWER
  if [ "$ANSWER" != "y" ]; then
    echo "Exiting."
    exit 1
  fi

  finalize_svn
  exit 0
fi

exit_with_usage
