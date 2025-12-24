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
# release version, e.g. 1.7.0
RELEASE_VERSION=${RELEASE_VERSION:-""}
################################################

if [[ -z $RELEASE_VERSION ]]; then
  echo "Please input release version, e.g. 1.7.0"
  exit 1
fi

echo "Release version: ${RELEASE_VERSION}"

RELEASE_TEMP_DIR=${RELEASE_DIR}/tmp
mkdir -p ${RELEASE_TEMP_DIR}
ANNOUNCE=${RELEASE_TEMP_DIR}/v${RELEASE_VERSION}_announce.temp

cat >$ANNOUNCE<<EOF
Title: [ANNOUNCE] Apache Kyuubi v${RELEASE_VERSION} is available

Content:
Hi all,

The Apache Kyuubi community is pleased to announce that
Apache Kyuubi v${RELEASE_VERSION} has been released!

Apache Kyuubi™ is a distributed and multi-tenant gateway to provide
serverless SQL on Data Warehouses and Lakehouses.

Kyuubi builds distributed SQL query engines on top of various kinds of
modern computing frameworks, e.g., Apache Spark, Apache Flink, Apache
Doris, Apache Hive, Trino, and StarRocks, etc., to query massive datasets
distributed over fleets of machines from heterogeneous data sources.

The full release notes and download links are available at:
Release Notes: https://kyuubi.apache.org/release/${RELEASE_VERSION}.html
Download Links: https://kyuubi.apache.org/releases.html

To learn more about Apache Kyuubi, please see
https://kyuubi.apache.org/

Kyuubi Resources:
- Documentation: https://kyuubi.readthedocs.io/en/v${RELEASE_VERSION}/
- Issue: https://github.com/apache/kyuubi/issues
- Mailing list: dev@kyuubi.apache.org

We would like to thank all contributors of the Kyuubi community
who made this release possible!

Thanks,
On behalf of Apache Kyuubi community
EOF

echo "please Use announce@apache.org, dev@kyuubi.apache.org, user@spark.apache.org
see announce content in $ANNOUNCE"
