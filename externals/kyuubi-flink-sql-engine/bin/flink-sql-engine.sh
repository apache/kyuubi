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


if [ -z "$FLINK_HOME" ]; then
  (>&2  echo "FLINK_HOME is not found in environment variable.")
  (>&2  echo "Configures the FLINK_HOME environment variable using the following command: export FLINK_HOME=<flink-install-dir>")
  exit 1
fi

if [ ! -d "$FLINK_HOME" ]; then
  (>&2 echo "$FLINK_HOME does not exist.")
  exit 1
fi

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
  if [ "$iteration" -gt 100 ]; then
    echo "Cannot resolve path: You have a cyclic symlink in $target."
    break
  fi
  ls=`ls -ld -- "$target"`
  target=`expr "$ls" : '.* -> \(.*\)$'`
  iteration=$((iteration + 1))
done

# Convert relative path to absolute path
bin=`dirname "$target"`
FLINK_SQL_ENGINE_HOME=`cd "$bin/.."; pwd -P`
set -x
export FLINK_CONF_DIR="$FLINK_HOME/conf"
FLINK_SQL_ENGINE_CONF="$FLINK_HOME/conf"
FLINK_SQL_ENGINE_LIB="$FLINK_SQL_ENGINE_HOME/target"
FLINK_SQL_ENGINE_LOG="$FLINK_SQL_ENGINE_HOME/log"

FLINK_SQL_ENGINE_DEFAULT_CONF="$FLINK_SQL_ENGINE_CONF/flink-sql-engine-defaults.yaml"

FLINK_SQL_ENGINE_JAR=$(find "$FLINK_SQL_ENGINE_LIB" -regex ".*/kyuubi-flink-sql-engine_.*\.jar" | grep -v "javadoc.jar" | grep -v "tests.jar")

# build kyuubi-flink-sql-engine classpath
FLINK_SQL_ENGINE_CLASSPATH=""
while read -d '' -r jarfile ; do
  if [[ "$FLINK_SQL_ENGINE_CLASSPATH" == "" ]]; then
    FLINK_SQL_ENGINE_CLASSPATH="$jarfile";
  else
    FLINK_SQL_ENGINE_CLASSPATH="$FLINK_SQL_ENGINE_CLASSPATH":"$jarfile"
  fi
done < <(find "$FLINK_SQL_ENGINE_LIB" ! -type d -name '*.jar' -print0 | sort -z)

FLINK_CONFIG_FILE="$FLINK_HOME/bin/config.sh"
SQL_ENGINE_CONFIG_FILE="$FLINK_SQL_ENGINE_HOME"/bin/config.sh
# replace target="$0" with target="<real_flink_config.sh_path>" and write to a new file
# this could make sure flink-sql-engine.sh can be executed anywhere
cat "$FLINK_CONFIG_FILE" | sed 's|target=\"$0\"|'target="$FLINK_CONFIG_FILE"'|g' > "$SQL_ENGINE_CONFIG_FILE"
# execute flink config
. "$SQL_ENGINE_CONFIG_FILE"
# remove it
rm -f "$SQL_ENGINE_CONFIG_FILE"

if [ "$FLINK_IDENT_STRING" = "" ]; then
  FLINK_IDENT_STRING="$USER"
fi

FLINK_SQL_CLIENT_JAR=$(find "$FLINK_OPT_DIR" -regex ".*flink-sql-client.*.jar")
CC_CLASSPATH=`constructFlinkClassPath`
FULL_CLASSPATH="$FLINK_SQL_ENGINE_JAR:$FLINK_SQL_CLIENT_JAR:$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"

# build log config
log=$FLINK_SQL_ENGINE_LOG/kyuubi-flink-sql-engine-$FLINK_IDENT_STRING-$HOSTNAME.log
log_setting=(-Dlog.file="$log" -Dlog4j.configurationFile=file:"$FLINK_SQL_ENGINE_CONF"/log4j.properties -Dlog4j.configuration=file:"$FLINK_SQL_ENGINE_CONF"/log4j.properties -Dlogback.configurationFile=file:"$FLINK_SQL_ENGINE_CONF"/logback.xml)

# read jvm args from config
#
#jvm_args_output=`${JAVA_RUN} "${log_setting[@]}" -classpath ${FULL_CLASSPATH} org.apache.kyuubi.engine.flink.config.BashJavaUtil "GET_SERVER_JVM_ARGS" "$@" --defaults "$FLINK_SQL_ENGINE_DEFAULT_CONF" 2>&1 | tail -n 1000`
#if [[ $? -ne 0 ]]; then
#  echo "[ERROR] Cannot run BashJavaUtil to execute command GET_SERVER_JVM_ARGS." 1>&2
#  # Print the output in case the user redirect the log to console.
#  echo "$jvm_args_output" 1>&2
#  exit 1
#fi
#JVM_ARGS=`extractExecutionResults "$jvm_args_output" 1`

if [ -n "$FLINK_SQL_ENGINE_JAR" ]; then
  echo $JAVA_RUN $JVM_ARGS "${log_setting[@]}" -cp ${FULL_CLASSPATH} org.apache.kyuubi.engine.flink.FlinkSQLEngine "$@" --defaults "$FLINK_SQL_ENGINE_DEFAULT_CONF" "$FLINK_SQL_ENGINE_JAR"
  exec $JAVA_RUN  "${log_setting[@]}" -cp ${FULL_CLASSPATH} org.apache.kyuubi.engine.flink.FlinkSQLEngine "$@" --defaults "$FLINK_SQL_ENGINE_DEFAULT_CONF" "$FLINK_SQL_ENGINE_JAR"
else
  (>&2 echo "[ERROR] Flink SQL Engine JAR file 'kyuubi-flink-sql-engine*.jar' should be located in $FLINK_SQL_ENGINE_LIB.")
  exit 1
fi
