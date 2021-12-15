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
# release version, e.g. 1.4.0-incubating
release_version=${release_version:-""}
################################################

if [[ -z $release_version ]]; then
  echo "Please input release version"
  exit 1
fi

echo "Release version: ${release_version}"

RELEASE_TEMP_DIR=${RELEASE_DIR}/tmp
mkdir -p ${RELEASE_TEMP_DIR}
ANNOUNCE=${RELEASE_TEMP_DIR}/${release_version}_announce.temp

cat >$ANNOUNCE<<EOF
Title: [ANNOUNCE] Apache Kyuubi (Incubating) released ${release_version}

Content:
Hi all,

The Apache Kyuubi (Incubating) community is pleased to announce that
Apache Kyuubi (Incubating) ${release_version} has been released!

Apache Kyuubi (Incubating) is a distributed multi-tenant JDBC server for
large-scale data processing and analytics, built on top of Apache Spark
and designed to support more engines (i.e. Apache Flink).

Kyuubi provides a pure SQL gateway through Thrift JDBC/ODBC interface
for end-users to manipulate large-scale data with pre-programmed and
extensible Spark SQL engines.

We are aiming to make Kyuubi an "out-of-the-box" tool for data warehouses
and data lakes.

This "out-of-the-box" model minimizes the barriers and costs for end-users
to use Spark at the client side.

At the server-side, Kyuubi server and engine's multi-tenant architecture
provides the administrators a way to achieve computing resource isolation,
data security, high availability, high client concurrency, etc.

The full release notes and download links are available at:
Release Notes: https://kyuubi.apache.org/release/${release_version}.html

To learn more about Apache Kyuubi (Incubating), please see
https://kyuubi.apache.org/

Kyuubi Resources:
- Issue: https://github.com/apache/incubator-kyuubi/issues
- Mailing list: dev@kyuubi.apache.org

We would like to thank all contributors of the Kyuubi community and Incubating
community who made this release possible!

Thanks,
On behalf of Apache Kyuubi (Incubating) community
EOF

echo "please Use general@incubator.apache.org, announce@apache.org, dev@kyuubi.apache.org, user@spark.apache.org
see announce content in $ANNOUNCE"
