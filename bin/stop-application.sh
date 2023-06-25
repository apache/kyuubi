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

if [[ $# -lt 1 ]] ; then
  echo "USAGE: $0 <application_id>"
  exit 1
fi

check_cmd() {
    command -v "$1" > /dev/null 2>&1
}

YARN_CMD=""

if check_cmd "yarn"; then
  YARN_CMD="yarn"
fi

if [ -z "$YARN_CMD" ] && [ -n "${HADOOP_HOME}" ] && check_cmd "${HADOOP_HOME}/bin/yarn"; then
  YARN_CMD="${HADOOP_HOME}/bin/yarn"
fi

if [[ -z "$YARN_CMD" ]]; then
  echo "Error: Cannot find yarn command! Please ensure your 'yarn' command is available or define the 'HADOOP_HOME' which contains yarn script in 'kyuubi-env.sh'."
  exit 1
fi

$YARN_CMD application -kill $1
