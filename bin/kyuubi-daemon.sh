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

# Runs a Spark command as a daemon.
#
# Environment Variables
#
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
#   SPARK_LOG_DIR   Where log files are stored. ${SPARK_HOME}/logs by default.
#   SPARK_MASTER    host:path where spark code should be rsync'd from
#   SPARK_PID_DIR   The pid files are stored. /tmp by default.
#   SPARK_IDENT_STRING   A string representing this instance of spark. $USER by default
#   SPARK_NICENESS The scheduling priority for daemons. Defaults to 0.
#   SPARK_NO_DAEMONIZE   If set, will run the proposed command in the foreground. It will not output a PID file.
##

usage="Usage: spark-daemon.sh (start|stop) <spark-command> <spark-instance-number> <args...>"

. "${SPARK_HOME}/sbin/spark-config.sh"

# get arguments
option=$1
shift
command=$1
shift
instance=$1
shift

rotate_log() {
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ ${num} -gt 1 ]; do
        prev=`expr ${num} - 1`
        [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
        num=${prev}
    done
    mv "$log" "$log.$num";
    fi
}

. "${SPARK_HOME}/bin/load-spark-env.sh"

export KYUUBI_LOG_DIR="${KYUUBI_LOG_DIR:-"${KYUUBI_HOME}/logs"}"
mkdir -p "$KYUUBI_LOG_DIR"
export KYUUBI_PID_DIR="${KYUUBI_PID_DIR:-"${KYUUBI_HOME}/pid"}"
export SPARK_PRINT_LAUNCH_COMMAND="1"

log="$KYUUBI_LOG_DIR/kyuubi-$USER-$command-$instance-$HOSTNAME.out"
pid="$KYUUBI_PID_DIR/kyuubi-$USER-$command-$instance.pid"

execute_command() {
    nohup -- "$@" >> ${log} 2>&1 < /dev/null &
    newpid="$!"

    echo "$newpid" > "$pid"

    # Poll for up to 5 seconds for the java process to start
    for i in {1..10}
    do
        if [[ $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
           break
        fi
        sleep 0.5
    done

    sleep 2
    # Check if the process has died; in that case we'll tail the log so the user can see
    if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
        echo "failed to launch: $@"
        tail -2 "$log" | sed 's/^/  /'
        echo "full log in $log"
    fi
}

case ${option} in

  (start)
    mkdir -p "$KYUUBI_PID_DIR"

    if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
        if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
          echo "$command running as process $TARGET_ID.  Stop it first."
          exit 1
        fi
    fi

    rotate_log "$log"
    echo "starting $command, logging to $log"
    execute_command bash "${KYUUBI_HOME}"/bin/kyuubi-class.sh org.apache.spark.KyuubiSubmit --class "$command" "$@"
    ;;

  (stop)

    if [ -f ${pid} ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
    ;;

  (*)
    echo ${usage}
    exit 1
    ;;

esac
