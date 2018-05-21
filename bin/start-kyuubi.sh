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

## Kyuubi Server Main Entrance
CLASS="yaooqinn.kyuubi.server.KyuubiServer"
export KYUUBI_HOME="$(cd "`dirname "$0"`"/..; pwd)"

set -a
. "${KYUUBI_HOME}/bin/kyuubi-env.sh"
set +a

function usage {
  echo "Usage: ./bin/start-kyuubi.sh <args...>"
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

## Find the Kyuubi Jar
if [ -z "$KYUUBI_JAR_DIR" ]; then
  KYUUBI_JAR_DIR="$KYUUBI_HOME/lib"
  if [ ! -d ${KYUUBI_JAR_DIR} ]; then
  echo -e "\nCandidate Kyuubi lib $KYUUBI_JAR_DIR doesn't exist, searching development environment..."
    KYUUBI_JAR_DIR="$KYUUBI_HOME/target"
  fi
fi

KYUUBI_JAR_NUM="$(ls ${KYUUBI_JAR_DIR} | grep kyuubi- | grep .jar | wc -l)"

if [ ${KYUUBI_JAR_NUM} = "0" ]; then
  echo "Kyuubi Server: need to build kyuubi first. Run ./bin/mvn clean package" >&2
  exit 1
fi

if [ ${KYUUBI_JAR_NUM} != "1" ]; then
  echo "Kyuubi Server: duplicated kyuubi jars found. Run ./bin/mvn clean package" >&2
  exit 1
fi

export KYUUBI_JAR=${KYUUBI_JAR_DIR}/"$(ls ${KYUUBI_JAR_DIR} |grep kyuubi- | grep .jar)"

echo "Kyuubi Server: jar founded:" ${KYUUBI_JAR} >&2

if [ "$KYUUBI_SUBMIT_ENABLE" == "true" ]; then
  exec "${KYUUBI_HOME}"/bin/kyuubi-daemon.sh start ${CLASS} 1 "$@" "$KYUUBI_JAR"
else
  exec "${SPARK_HOME}"/sbin/spark-daemon.sh start ${CLASS} 1 "$@" "$KYUUBI_JAR"
fi


