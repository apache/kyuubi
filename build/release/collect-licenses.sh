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

# This script extracts from all jars in the specified directory the NOTICE files and the
# licenses folders. It then concatenates all NOTICE files and collects the contents of all
# licenses folders in the specified output directory.
#
# This tool can be used to generate a rough skeleton for the binary NOTICE file. Be aware,
# that it does not deduplicate contents.

set -o pipefail
set -e
set -x

SRC=${1:-.}
DST=${2:-licenses-output}
KYUUBI_DIR="$(cd "$(dirname "$0")"/../..; pwd)"
TMP="${DST}/tmp"
NOTICE_BINARY_PREAMBLE="${KYUUBI_DIR}/NOTICE-binary"

USAGE="$0 <SOURCE_DIRECTORY:-.> <OUTPUT_DIRECTORY:-licenses-output>"

source "$KYUUBI_DIR/build/util.sh"

if [ "${SRC}" = "-h" ]; then
	echo "${USAGE}"
	exit 0
fi

for jar_file in $(find -L "${SRC}" -name "*.jar" | grep -v "kyuubi-")
do
	DIR="${TMP}/$(basename -- "${jar_file}" .jar)"
	mkdir -p "${DIR}"
	JAR=$(realpath "${jar_file}")
	(cd "${DIR}" && jar xf ${JAR} META-INF/NOTICE META-INF/licenses)
done

NOTICE="${DST}/NOTICE"
[ -f "${NOTICE}" ] && rm "${NOTICE}"
cp "${NOTICE_BINARY_PREAMBLE}" "${NOTICE}"
(export LC_ALL=C; find "${TMP}" -name "NOTICE" | sort | xargs cat >> "${NOTICE}")

LICENSES="${DST}/licenses"
[ -f "${LICENSES}" ] && rm -r "${LICENSES}"
find "${TMP}" -name "licenses" -type d -exec cp -r -- "{}" "${DST}" \;

# Search and collect license files that not bundled in any jars.
find "${SRC}" -name "LICENSE.*" -type f \
! -path "${DST}/licenses/*" ! -path "${TMP}/licenses/*" -exec cp -- "{}" "${DST}/licenses" \;

rm -r "${TMP}"
