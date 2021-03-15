#!/usr/bin/env bash
# Find the spark-submit
export KYUUBI_HOME="${KYUUBI_HOME:-"$(cd "`dirname $0`"/..; pwd)"}"
. $KYUUBI_HOME/bin/env.sh

export KYUUBI_CONF_DIR="${KYUUBI_CONF_DIR:-"${KYUUBI_HOME}"/conf}"
export KYUUBI_PID_DIR="${KYUUBI_PID_DIR:-"${KYUUBI_HOME}/pid"}"
if [[ -e ${KYUUBI_LOG_DIR} ]]; then
  mkdir -p ${KYUUBI_LOG_DIR}
fi

if [[ -z ${JAVA_HOME} ]]; then
   if [[ $(command -v java) ]]; then
     export JAVA_HOME="$(dirname $(dirname $(which java)))"
   fi
fi
export KYUUBI_SCALA_VERSION="${KYUUBI_SCALA_VERSION:-"2.11"}"


## Find the kyuubi Jar
if [[ -z "$KYUUBI_JAR_DIR" ]]; then
  KYUUBI_JAR_DIR="$KYUUBI_HOME/lib"
  if [[ ! -d ${KYUUBI_JAR_DIR} ]]; then
  echo -e "\nCandidate kyuubi lib $KYUUBI_JAR_DIR doesn't exist, searching development environment..."
    KYUUBI_JAR_DIR="$KYUUBI_HOME/kyuubi-web/target"
  fi
fi

export KYUUBI_JAR=${KYUUBI_JAR_DIR}/"$(ls ${KYUUBI_JAR_DIR} | grep kyuubi-web | grep -v original | grep .jar$)"

LAUNCH_CLASSPATH=""
for file in ${KYUUBI_JAR_DIR}/*
do
    if test -f $file
    then
        LAUNCH_CLASSPATH="$LAUNCH_CLASSPATH:$file"
    fi
done
LAUNCH_CLASSPATH="$LAUNCH_CLASSPATH:$KYUUBI_CONF_DIR"


pid="$KYUUBI_PID_DIR/kyuubi-web-$USER.pid"
# get arguments
option=$1
shift

case ${option} in

  (start)
    # Print essential environment variables to console
    echo "JAVA_HOME: ${JAVA_HOME}"
    echo "KYUUBI_HOME: ${KYUUBI_HOME}"
    echo "KYUUBI_CONF_DIR: ${KYUUBI_CONF_DIR}"
    echo "KYUUBI_JAR: ${KYUUBI_JAR}"
    echo "KYUUBI_PID_DIR: ${KYUUBI_PID_DIR}"
    echo "HADOOP_CONF_DIR: ${HADOOP_CONF_DIR}"

    mkdir -p "$KYUUBI_PID_DIR"

    if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
        if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
          echo "kyuubi Web Server running as process $TARGET_ID.  Stop it first."
          exit 1
        fi
    fi

    # Find the java binary
    if [ -n "${JAVA_HOME}" ]; then
      RUNNER="${JAVA_HOME}/bin/java"
    else
      if [ "$(command -v java)" ]; then
        RUNNER="java"
      else
        echo "JAVA_HOME is not set" >&2
        exit 1
      fi
    fi
    JAVA_OPTS="-Dlog4j.configuration=file://$KYUUBI_HOME/conf/log4j.properties -Dlog4j.logDir=$KYUUBI_HOME/logs"
    CMD="$RUNNER $JAVA_OPTS $JAVA_EXTRA_OPTS -cp "$LAUNCH_CLASSPATH"  org.apache.kyuubi.web.kyuubiWebServer $KYUUBI_JAR"
#    echo $CMD
    nohup $CMD >/dev/null  2>&1 &
    newpid="$!"
    if [ -z $newpid ]; then
      echo "kyuubi Web Server start failed, restart please."
      exit 1
    fi
    echo "$newpid" > "$pid"
    echo "kyuubi Web Server start success, pid:  $newpid"
    ;;

  (status)

    if [[ -f ${pid} ]]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "kyuubi Web Server is running (pid: $TARGET_ID)"
      else
        echo "kyuubi Web Server is not running"
      fi
    else
      echo "kyuubi Web Server is not running"
    fi

    ;;

  (stop)

    if [ -f ${pid} ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping kyuubi Web Server"
        kill "$TARGET_ID" && rm -f "$pid"
        PID_EXIST=$(ps aux | awk '{print $2}'| grep -w $TARGET_ID)

        if [ ! $PID_EXIST ];then
          sleep 1
          PID_EXIST=$(ps aux | awk '{print $2}'| grep -w $TARGET_ID)
        else
          echo "kyuubi Web Server stop success"
        fi

      else
        echo "no kyuubi Web Server to stop"
      fi
    else
      echo "no kyuubi Web Server to stop"
    fi
    ;;

  (*)
    echo "Usage: kyuubi.sh (start|stop)"
    exit 1
    ;;

esac
