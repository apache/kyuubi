#!/usr/bin/env bash
export KYUUBI_HOME="${KYUUBI_HOME:-"$(cd "`dirname $0`"/..; pwd)"}"

. $KYUUBI_HOME/bin/kyuubi.sh status
. $KYUUBI_HOME/bin/kyuubi.sh stop

. $KYUUBI_HOME/bin/kyuubi.sh status

. $KYUUBI_HOME/bin/kyuubi.sh start