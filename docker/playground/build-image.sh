#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Set Mirror:
# APACHE_MIRROR=mirrors.cloud.tencent.com/apache
# MAVEN_MIRROR=mirrors.cloud.tencent.com/maven

set -e

APACHE_MIRROR=${APACHE_MIRROR:-https://dlcdn.apache.org}
MAVEN_MIRROR=${MAVEN_MIRROR:-https://maven-central-asia.storage-download.googleapis.com/maven2}
BUILD_CMD="docker build"

if [ $BUILDX ]; then
  echo "Using buildx to build cross-platform images"
  BUILD_CMD="docker buildx build --platform=linux/amd64,linux/arm64 --push"
fi

SELF_DIR="$(cd "$(dirname "$0")"; pwd)"

source "${SELF_DIR}/.env"

${BUILD_CMD} \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg KYUUBI_VERSION=${KYUUBI_VERSION} \
  --file "${SELF_DIR}/image/kyuubi-playground-base.Dockerfile" \
  --tag nekyuubi/kyuubi-playground-base:${KYUUBI_VERSION} \
  "${SELF_DIR}/image" $@

${BUILD_CMD} \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg KYUUBI_VERSION=${KYUUBI_VERSION} \
  --build-arg AWS_JAVA_SDK_VERSION=${AWS_JAVA_SDK_VERSION} \
  --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
  --file "${SELF_DIR}/image/kyuubi-playground-hadoop.Dockerfile" \
  --tag nekyuubi/kyuubi-playground-hadoop:${KYUUBI_VERSION} \
  "${SELF_DIR}/image" $@

${BUILD_CMD} \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg KYUUBI_VERSION=${KYUUBI_VERSION} \
  --build-arg HIVE_VERSION=${HIVE_VERSION} \
  --file "${SELF_DIR}/image/kyuubi-playground-metastore.Dockerfile" \
  --tag nekyuubi/kyuubi-playground-metastore:${KYUUBI_VERSION} \
  "${SELF_DIR}/image" $@

${BUILD_CMD} \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg KYUUBI_VERSION=${KYUUBI_VERSION} \
  --build-arg AWS_JAVA_SDK_VERSION=${AWS_JAVA_SDK_VERSION} \
  --build-arg CLICKHOUSE_JDBC_VERSION=${CLICKHOUSE_JDBC_VERSION} \
  --build-arg SPARK_HADOOP_VERSION=${SPARK_HADOOP_VERSION} \
  --build-arg ICEBERG_VERSION=${ICEBERG_VERSION} \
  --build-arg POSTGRES_JDBC_VERSION=${POSTGRES_JDBC_VERSION} \
  --build-arg SCALA_BINARY_VERSION=${SCALA_BINARY_VERSION} \
  --build-arg SPARK_VERSION=${SPARK_VERSION} \
  --build-arg SPARK_BINARY_VERSION=${SPARK_BINARY_VERSION} \
  --file "${SELF_DIR}/image/kyuubi-playground-spark.Dockerfile" \
  --tag nekyuubi/kyuubi-playground-spark:${KYUUBI_VERSION} \
  "${SELF_DIR}/image" $@

${BUILD_CMD} \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg KYUUBI_VERSION=${KYUUBI_VERSION} \
  --build-arg AWS_JAVA_SDK_VERSION=${AWS_JAVA_SDK_VERSION} \
  --build-arg KYUUBI_HADOOP_VERSION=${KYUUBI_HADOOP_VERSION} \
  --file "${SELF_DIR}/image/kyuubi-playground-kyuubi.Dockerfile" \
  --tag nekyuubi/kyuubi-playground-kyuubi:${KYUUBI_VERSION} \
  "${SELF_DIR}/image" $@
