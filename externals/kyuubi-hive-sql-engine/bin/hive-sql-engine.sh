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
if [[ -z ${JAVA_HOME} ]]; then
  echo "[ERROR] JAVA_HOME IS NOT SET! CANNOT PROCEED."
  exit 1
fi

RUNNER="${JAVA_HOME}/bin/java"

if [[ "$HIVE_ENGINE_HOME" == "$KYUUBI_HOME/externals/engines/hive" ]]; then
  HIVE_CLIENT_JAR="$HIVE_ENGINE_JAR"
  HIVE_CLIENT_JARS_DIR="$HIVE_ENGINE_HOME/jars"
else
  echo "\nHIVE_ENGINE_HOME $HIVE_ENGINE_HOME doesn't match production directory, assuming in development environment..."
  HIVE_CLIENT_JAR=$(find $HIVE_ENGINE_HOME/target -regex '.*/kyuubi-hive-sql-engine_.*.jar$' | grep -v '\-sources.jar$' | grep -v '\-javadoc.jar$' | grep -v '\-tests.jar$')
  HIVE_CLIENT_JARS_DIR=$(find $HIVE_ENGINE_HOME/target -regex '.*/jars')
fi

HIVE_CLIENT_CLASSPATH="$HIVE_CLIENT_JARS_DIR/*"
if [[ -z ${YARN_CONF_DIR} ]]; then
  FULL_CLASSPATH="$HIVE_CLIENT_CLASSPATH:$HIVE_CLIENT_JAR:$HADOOP_CONF_DIR:$HIVE_CONF_DIR"
else
  FULL_CLASSPATH="$HIVE_CLIENT_CLASSPATH:$HIVE_CLIENT_JAR:$HADOOP_CONF_DIR:$HIVE_CONF_DIR:$YARN_CONF_DIR"
fi

if [ -n "$HIVE_CLIENT_JAR" ]; then
  exec $RUNNER ${HIVE_SQL_ENGINE_JAVA_OPTS} ${HIVE_ENGINE_DYNAMIC_ARGS} -cp ${FULL_CLASSPATH} org.apache.kyuubi.engine.hive.HiveSQLEngine "$@"
else
  (>&2 echo "[ERROR] HIVE Engine JAR file 'kyuubi-hive-sql-engine*.jar' should be located in $HIVE_ENGINE_HOME/jars.")
  exit 1
fi
