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

KYUUBI_OPTIONS=$1
shift
SPARK_OPTIONS=$@

function usage {
  echo "Usage: ./bin/start-kyuubi.sh (start|stop) [spark-submit options]"
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

## Find the Kyuubi Jar
KYUUBI_JAR_DIR="$(cd "`dirname "$0"`"/..; pwd)/target"
KYUUBI_JAR_NUM="$(ls ${KYUUBI_JAR_DIR} | grep kyuubi- | grep .jar | wc -l)"

if [ ${KYUUBI_JAR_NUM} = "0" ]; then
  echo "Kyuubi Server: need to build kyuubi first. Run ./bin/mvn clean package" >&2
  exit 1
fi

if [ ${KYUUBI_JAR_NUM} != "1" ]; then
  echo "Kyuubi Server: duplicated kyuubi jars found. Run ./bin/mvn clean package" >&2
  exit 1
fi

KYUUBI_JAR=${KYUUBI_JAR_DIR}/"$(ls ${KYUUBI_JAR_DIR} |grep kyuubi- | grep .jar)"

echo "Kyuubi Server: jar founded:" ${KYUUBI_JAR} >&2


function rotate_log {
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

# get log directory
if [ "$KYUUBI_LOG_DIR" = "" ]; then
  export KYUUBI_LOG_DIR="$(cd "`dirname "$0"`"/..; pwd)/logs"
fi
mkdir -p "$KYUUBI_LOG_DIR"
touch "$KYUUBI_LOG_DIR"/.kyuubi_test > /dev/null 2>&1

if [ "$?" = "0" ]; then
  rm -f "$KYUUBI_LOG_DIR"/.kyuubi_test
else
  chown "$USER" "$KYUUBI_LOG_DIR"
fi

LOG="$KYUUBI_LOG_DIR/kyuubi-$USER-$CLASS-$HOSTNAME.out"
PID="$KYUUBI_LOG_DIR/kyuubi-$USER-$CLASS-$HOSTNAME.pid"

function start_server {
  rotate_log "$LOG"
  echo "Kyuubi Server: starting and logging to $LOG" >&2

  # Find the spark-submit
  if [ -n "$SPARK_HOME" ]; then
    SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
  else
    echo "Kyuubi Server: SPARK_HOME is not set" >&2
    exit 1
  fi

  nohup bash "$SPARK_SUBMIT" --class "$CLASS" "$SPARK_OPTIONS" "$KYUUBI_JAR" >> "$LOG" 2>&1 < /dev/null &

  PID_TMP="$!"
  echo ${PID_TMP} > ${PID}

  # Poll for up to 5 seconds for the java process to start
  for i in {1..10}
    do
      if [[ $(ps -p "$PID_TMP" -o comm=) =~ "java" ]]; then
        break
      fi
    sleep 0.5
    done

  sleep 2

  # Check if the process has died; in that case we'll tail the log so the user can see
  if [[ ! $(ps -p "$PID_TMP" -o comm=) =~ "java" ]]; then
    echo "Kyuubi Server: failed to launch: $SPARK_OPTIONS" >&2
    tail -2 "$log" | sed 's/^/  /'
    echo "Kyuubi Server: full log in $log" >&2
  fi
}

case ${KYUUBI_OPTIONS} in
  (start)
    start_server
    ;;

  (stop)
    if [ -f ${PID} ]; then
      TARGET_ID="$(cat "$PID")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "Kyuubi Server: stopping $CLASS"
        kill "$TARGET_ID" && rm -f "$PID"
      else
        echo "Kyuubi Server: no $CLASS to stop"
      fi
    else
      echo "Kyuubi Server: no $CLASS to stop"
    fi
    ;;

  (*)
    usage
    exit 1
    ;;

esac
