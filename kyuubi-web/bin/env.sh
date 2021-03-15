#!/usr/bin/env bash
export KYUUBI_HOME="${KYUUBI_HOME:-"$(cd "`dirname $0`"/..; pwd)"}"

export JAVA_EXTRA_OPTS=" -Xmx2G -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007"