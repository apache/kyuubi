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
OUTPUT_FILE="commit_diff.md.tmp"

HEADER="
# Highlight

## Server

### Spark Engine

### Spark AuthZ Plugin

### Other Spark Plugins

### Flink Engine

### Trino Engine

### JDBC Engine

### Web UI

### Documentation

### Building, Infrastructure and Dependency

### Changelogs

"

CONTRIBUTOR_HEADER="

## Credits

Last but not least, this release would not have been possible without the following contributors:

"

echo "$HEADER" >> ./$OUTPUT_FILE
git log $BASE_TAG..$TARGET_TAG --pretty=format:"%s" | sed -E 's/^/- /; s/\[KYUUBI #([0-9]+)\]/[KYUUBI [#\1](https:\/\/github.com\/apache\/kyuubi\/issues\/\1)]/g' >> ./$OUTPUT_FILE


echo "$HEADER" >> ./$OUTPUT_FILE
git log $BASE_TAG..$TARGET_TAG --pretty=format:"- %an" | sort -u >> ./$OUTPUT_FILE

echo "Release note draft saved to $OUTPUT_FILE"
