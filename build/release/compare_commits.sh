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

set -o pipefail
set -e
set -x

BASE_TAG="v1.9.0"
TARGET_TAG="v1.10.0-rc0"
OUTPUT_FILE="commit_diff.txt.tmp"


# 获取两个标签之间的提交差异
git log $BASE_TAG..$TARGET_TAG --pretty=format:"- [ %s ](https://github.com/apache/kyuubi/commit/%h )" > ./$OUTPUT_FILE

echo "Commit differences saved to $OUTPUT_FILE"