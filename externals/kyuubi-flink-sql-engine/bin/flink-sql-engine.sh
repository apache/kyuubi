#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

################################################################################
# Adopted from "flink" bash script
################################################################################

if [[ -z "$FLINK_HOME" || ! -d "$FLINK_HOME" ]]; then
  (>&2 echo "Invalid FLINK_HOME: ${FLINK_HOME:-unset}")
  exit 1
fi

# do NOT let config.sh detect FLINK_HOME
_FLINK_HOME_DETERMINED=1 . "$FLINK_HOME/bin/config.sh"

FLINK_IDENT_STRING=${FLINK_IDENT_STRING:-"$USER"}
FLINK_SQL_CLIENT_JAR=$(find "$FLINK_OPT_DIR" -regex ".*flink-sql-client.*.jar")
CC_CLASSPATH=`constructFlinkClassPath`

FLINK_SQL_ENGINE_HOME="$(cd `dirname $0`/..; pwd)"
if [[ "$FLINK_SQL_ENGINE_HOME" == "$KYUUBI_HOME/externals/engines/flink" ]]; then
  FLINK_SQL_ENGINE_LIB_DIR="$FLINK_SQL_ENGINE_HOME/lib"
  FLINK_SQL_ENGINE_JAR=$(find "$FLINK_SQL_ENGINE_LIB_DIR" -regex ".*/kyuubi-flink-sql-engine_.*\.jar")
  FLINK_HADOOP_CLASSPATH="$INTERNAL_HADOOP_CLASSPATHS"
  log_file="$KYUUBI_LOG_DIR/kyuubi-flink-sql-engine-$FLINK_IDENT_STRING-$HOSTNAME.log"
  log4j2_conf_file="file:$FLINK_CONF_DIR/log4j.properties"
  logback_conf_file="file:$FLINK_CONF_DIR/logback.xml"
else
  echo -e "\nFLINK_SQL_ENGINE_HOME $FLINK_SQL_ENGINE_HOME doesn't match production directory, assuming in development environment..."
  FLINK_SQL_ENGINE_LIB_DIR="$FLINK_SQL_ENGINE_HOME/target"
  FLINK_SQL_ENGINE_JAR=$(find "$FLINK_SQL_ENGINE_LIB_DIR" -regex '.*/kyuubi-flink-sql-engine_.*\.jar$' | grep -v '\-javadoc.jar$' | grep -v '\-tests.jar$')
  _FLINK_SQL_ENGINE_HADOOP_CLIENT_JARS=$(find $FLINK_SQL_ENGINE_LIB_DIR -regex '.*/hadoop-client-.*\.jar$' | tr '\n' ':')
  FLINK_HADOOP_CLASSPATH="${_FLINK_SQL_ENGINE_HADOOP_CLIENT_JARS%:}:${HADOOP_CLASSPATH}:${HADOOP_CONF_DIR}:${YARN_CONF_DIR}"
  log_file="unused.log"
  log4j2_conf_file="file:$FLINK_CONF_DIR/log4j-session.properties" # which send all logs to console
  logback_conf_file="unused.xml"
fi

FULL_CLASSPATH="$FLINK_SQL_ENGINE_JAR:$FLINK_SQL_CLIENT_JAR:$CC_CLASSPATH:$FLINK_HADOOP_CLASSPATH"

log_setting=(
  -Dlog.file="$log_file"
  -Dlog4j2.configurationFile="$log4j2_conf_file"
  -Dlogback.configurationFile="$logback_conf_file"
)

if [ -n "$FLINK_SQL_ENGINE_JAR" ]; then
  exec $JAVA_RUN ${FLINK_SQL_ENGINE_DYNAMIC_ARGS} "${log_setting[@]}" -cp ${FULL_CLASSPATH} \
    org.apache.kyuubi.engine.flink.FlinkSQLEngine "$@"
else
  (>&2 echo "[ERROR] Flink SQL Engine JAR file 'kyuubi-flink-sql-engine*.jar' should be located in $FLINK_SQL_ENGINE_LIB_DIR.")
  exit 1
fi
