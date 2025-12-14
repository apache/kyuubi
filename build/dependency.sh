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

set -ex

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

# Explicitly set locale in order to make `sort` output consistent across machines.
# See https://stackoverflow.com/questions/28881 for more details.
export LC_ALL=C

# Starting with Maven 3.9.0, this variable contains arguments passed to Maven before CLI arguments.
# See https://maven.apache.org/configure.html#maven_args-environment-variable for more details.
export MAVEN_ARGS="-Pflink-provided,spark-provided,hive-provided -Dmaven.javadoc.skip=true -Drat.skip=true -Dscalastyle.skip=true -Dspotless.check.skip"

MVN="${FWDIR}/build/mvn"
DEP_PR="${FWDIR}/dev/dependencyList.tmp"
DEP="${FWDIR}/dev/dependencyList"

# We'll switch the version to a temp. one, publish POMs using that new version, then switch back to
# the old version. We need to do this because the `dependency:build-classpath` task needs to
# resolve Kyuubi's internal submodule dependencies.

# From http://stackoverflow.com/a/26514030
set +e
OLD_VERSION=$(grep -A3 "<artifactId>kyuubi-parent</artifactId>" "${FWDIR}/pom.xml" |
  grep "<version>" | head -n1 | awk -F '[<>]' '{print $3}')
set -e

TEMP_VERSION="kyuubi-$(awk 'BEGIN {srand(); print int(100000 + rand() * 900000)}')"

function reset_version {
  # Delete the temporary POMs that we wrote to the local Maven repo:
  find "$HOME/.m2/" | grep "$TEMP_VERSION" | xargs rm -rf

  # Restore the original version number:
  $MVN -q versions:set -DnewVersion=$OLD_VERSION -DgenerateBackupPoms=false > /dev/null
}
trap reset_version EXIT

$MVN -q versions:set -DnewVersion=$TEMP_VERSION -DgenerateBackupPoms=false > /dev/null

echo "Performing Maven install"
$MVN jar:jar jar:test-jar install:install clean -q

echo "Performing Maven validate"
$MVN validate -q

rm -rf "${DEP_PR}"
cat >"${DEP_PR}"<<EOF
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

EOF

echo "Generating dependency manifest"
$MVN dependency:build-classpath -pl kyuubi-assembly -am \
  | grep "Dependencies classpath:" -A 1 \
  | tail -n 1 | tr ":" "\n" | awk -F '/' '{
    # For each dependency classpath, we fetch the last three parts split by "/": artifact id, version, and jar name.
    # Since classifier, if exists, always sits between "artifact_id-version-" and ".jar" suffix in the jar name,
    # we extract classifier and put it right before the jar name explicitly.
    # For example, `orc-core/1.5.5/nohive/orc-core-1.5.5-nohive.jar`
    #                              ^^^^^^
    #                              extracted classifier
    #               `okio/1.15.0//okio-1.15.0.jar`
    #                           ^
    #                           empty for dependencies without classifier
    artifact_id=$(NF-2);
    version=$(NF-1);
    jar_name=$NF;
    classifier_start_index=length(artifact_id"-"version"-") + 1;
    classifier_end_index=index(jar_name, ".jar") - 1;
    classifier=substr(jar_name, classifier_start_index, classifier_end_index - classifier_start_index + 1);
    print artifact_id"/"version"/"classifier"/"jar_name
  }' | sort | grep -v kyuubi >> $DEP_PR

if [[ "$1" == "--replace" ]]; then
    rm -rf "${DEP}"
    mv "${DEP_PR}" "${DEP}"
    exit 0
fi

set +e
the_diff=$(diff "${DEP}" "${DEP_PR}")
set -e
rm -rf "${DEP_PR}"
if [[ -n "${the_diff}" ]]; then
  echo "Dependency List Changed Detected: "
  echo "${the_diff}"
  echo "To update the dependency file, run './build/dependency.sh --replace'."
  exit 1
fi

exit 0
