#!/bin/bash
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
var CMD = "java -cp ${CLEANER_CLASSPATH} org.apache.kyuubi.KubernetesShuffleFileCleaner  "

CMD="${CMD} -cache_dirs ${SHUFFLE_DIR}"

if ! [[ -n "${FILE_EXPIRED_TIME}" ]]; then
  CMD="${CMD} -file_expired_time ${FILE_EXPIRED_TIME}"
fi

if ! [[ -n "${FREE_SPACE_THRESHOLD}" ]]; then
  CMD="${CMD} -free_space_threshold ${FREE_SPACE_THRESHOLD}"
fi

if ! [[ -n "${CYCLE_INTERVAL_TIME}" ]]; then
  CMD="${CMD} -cycle_interval_time ${CYCLE_INTERVAL_TIME}"
fi

if ! [[ -n "${DEEP_CLEAN_FILE_EXPIRED_TIME}" ]]; then
  CMD="${CMD} -deep_clean_file_expired_time ${DEEP_CLEAN_FILE_EXPIRED_TIME}"
fi

${CMD}
