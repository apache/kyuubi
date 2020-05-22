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
KYUUBI_HOME="$(cd "`dirname "$0"`"/..; pwd)"

set -a
. "${KYUUBI_HOME}/bin/kyuubi-env.sh"
set +a


if [ "$KYUUBI_SUBMIT_ENABLE" == "true" ]; then
  "${KYUUBI_HOME}"/bin/kyuubi-daemon.sh stop ${CLASS} 1
else
  "${SPARK_HOME}/sbin"/spark-daemon.sh stop "$CLASS" 1
fi
