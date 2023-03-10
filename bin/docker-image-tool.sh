#!/usr/bin/env bash

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

# This script builds and pushes docker images when run from a release of Kyuubi
# with Kubernetes support.

function error {
  echo "$@" 1>&2
  exit 1
}

if [ -z "${KYUUBI_HOME}" ]; then
  KYUUBI_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
KYUUBI_IMAGE_NAME="kyuubi"

function is_dev_build {
  [ ! -f "$KYUUBI_HOME/RELEASE" ]
}

if is_dev_build; then
  cat <<EOF
  Current docker-image-tool.sh only support build docker image from binary package.
  You can download Kyuubi binary package from kyuubi web-sit https://kyuubi.apache.org/releases.html.
  Or you can build binary from source code with $KYUUBI_HOME/build/dist, find more detail about build binary in that script

EOF
  exit 1
fi

function image_ref {
  local image="$1"
  local add_repo="${2:-1}"
  if [ $add_repo = 1 ] && [ -n "$REPO" ]; then
    image="$REPO/$image"
  fi
  if [ -n "$TAG" ]; then
    image="$image:$TAG"
  else
    image="$image:$KYUUBI_VERSION"
  fi
  echo "$image"
}

function docker_push {
  local image_name="$1"
  if [ ! -z $(docker images -q "$(image_ref ${image_name})") ]; then
    docker push "$(image_ref ${image_name})"
    if [ $? -ne 0 ]; then
      error "Failed to push $image_name Docker image."
    fi
  else
    echo "$(image_ref ${image_name}) image not found. Skipping push for this image."
  fi
}

function resolve_file {
  local FILE=$1
  if [ -n "$FILE" ]; then
    local DIR=$(dirname $FILE)
    DIR=$(cd $DIR && pwd)
    FILE="${DIR}/$(basename $FILE)"
  fi
  echo $FILE
}

function img_ctx_dir {
  echo "$KYUUBI_HOME"
}

function build {
  local BUILD_ARGS
  local KYUUBI_ROOT="$KYUUBI_HOME"
  local BUILD_ARGS=(${BUILD_PARAMS})

  # mkdir spark-binary to cache spark
  # clean cache if spark-binary exists
  if [[ ! -d "$KYUUBI_ROOT/spark-binary" ]]; then
    mkdir "$KYUUBI_ROOT/spark-binary"
  else
    rm -rf "$KYUUBI_ROOT/spark-binary/*"
  fi

  # If SPARK_HOME_IN_DOCKER configured,
  # Kyuubi won't copy local spark into docker image.
  # Use SPARK_HOME_IN_DOCKER as SPARK_HOME in docker image.
  if [[ -n "${SPARK_HOME_IN_DOCKER}" ]]; then
    BUILD_ARGS+=(--build-arg spark_home_in_docker=$SPARK_HOME_IN_DOCKER)
    BUILD_ARGS+=(--build-arg spark_provided="spark_provided")
  else
    if [[ ! -d "$SPARK_HOME" ]]; then
      if [[ -d "$KYUUBI_ROOT/externals/spark-*" ]]; then
        SPARK_HOME="$(find "$KYUUBI_ROOT/externals" -name 'spark-*' -type d)"
      else
        error "Cannot found dir SPARK_HOME $SPARK_HOME, you must configure SPARK_HOME correct."
      fi
    fi
    cp -r "$SPARK_HOME/" "$KYUUBI_ROOT/spark-binary/"
  fi

  # Verify that the Docker image content directory is present
  if [ ! -d "$KYUUBI_ROOT/docker" ]; then
    error "Can't find Kyuubi docker context $KYUUBI_ROOT/docker, please check whether the binary package is complete."
  fi

  # Verify that that Kyuubi need JARS is present
  local TOTAL_JARS=$(ls $KYUUBI_ROOT/jars/kyuubi-* | wc -l)
  TOTAL_JARS=$(( $TOTAL_JARS ))
  if [ "${TOTAL_JARS}" -eq 0 ]; then
    error "Cannot find Kyuubi JARs. Please check whether the binary package is complete."
  fi

  if [ -n "$KYUUBI_UID" ]; then
    BUILD_ARGS+=(--build-arg kyuubi_uid=$KYUUBI_UID)
  fi

  local BASEDOCKERFILE=${BASEDOCKERFILE:-"docker/Dockerfile"}
  local ARCHS=${ARCHS:-"--platform linux/amd64,linux/arm64"}

  (cd $(img_ctx_dir base) && docker build $NOCACHEARG "${BUILD_ARGS[@]}" \
    -t $(image_ref $KYUUBI_IMAGE_NAME) \
    -f "$BASEDOCKERFILE" .)
  if [ $? -ne 0 ]; then
    error "Failed to build Kyuubi JVM Docker image, please refer to Docker build output for details."
  fi
  if [ "${CROSS_BUILD}" != "false" ]; then
  (cd $(img_ctx_dir base) && docker buildx build $ARCHS $NOCACHEARG "${BUILD_ARGS[@]}" --push \
    -t $(image_ref $KYUUBI_IMAGE_NAME) \
    -f "$BASEDOCKERFILE" .)
  fi
}

function push {
  docker_push $KYUUBI_IMAGE_NAME
}

function usage {
  cat <<EOF
Usage: $0 [options] [command]
Builds or pushes the built-in Kyuubi Docker image.

Commands:
  build       Build image. Requires a repository address to be provided if the image will be
              pushed to a different registry.
  push        Push a pre-built image to a registry. Requires a repository address to be provided.

Options:
  -f                    Dockerfile to build for JVM based Jobs. By default builds the Dockerfile shipped with Kyuubi.
  -r                    Repository address.
  -t                    Tag to apply to the built image, or to identify the image to be pushed.
  -n                    Build docker image with --no-cache
  -u                    UID to use in the USER directive to set the user the main Kyuubi process runs as inside the
                        resulting container
  -X                    Use docker buildx to cross build. Automatically pushes.
                        See https://docs.docker.com/buildx/working-with-buildx/ for steps to setup buildx.
  -b                    Build arg to build or push the image. For multiple build args, this option needs to
                        be used separately for each build arg.
  -s                    Put the specified Spark into the Kyuubi image to be used as the internal SPARK_HOME
                        of the container.
  -S                    Declare SPARK_HOME in Docker Image. When you configured -S, you need to provide an image
                        with Spark as BASE_IMAGE.

Examples:

  - Build and push image with tag "v1.4.0" to docker.io/myrepo
    $0 -r docker.io/myrepo -t v1.4.0 build
    $0 -r docker.io/myrepo -t v1.4.0 push

  - Build and push with tag "v1.4.0" and Spark-3.2.1 as base image to docker.io/myrepo
    $0 -r docker.io/myrepo -t v1.4.0 -b BASE_IMAGE=repo/spark:3.2.1 build
    $0 -r docker.io/myrepo -t v1.4.0 push

  - Build and push for multiple archs to docker.io/myrepo
    $0 -r docker.io/myrepo -t v1.4.0 -X build

    # Note: buildx, which does cross building, needs to do the push during build
    # So there is no separate push step with -X

  - Build with Spark placed "/path/spark"
    $0 -s /path/spark build

  - Build with Spark Image myrepo/spark:3.1.0
    $0 -S /opt/spark -b BASE_IMAGE=myrepo/spark:3.1.0 build

EOF
}

if [[ "$*" = *--help ]] || [[ "$*" = *-h ]]; then
  usage
  exit 0
fi

REPO=
TAG=
BASEDOCKERFILE=
NOCACHEARG=
BUILD_PARAMS=
KYUUBI_UID=
CROSS_BUILD="false"
SPARK_HOME_IN_DOCKER=
while getopts f:r:t:Xnb:u:s:S: option
do
 case "${option}"
 in
 f) BASEDOCKERFILE=$(resolve_file ${OPTARG});;
 r) REPO=${OPTARG};;
 t) TAG=${OPTARG};;
 n) NOCACHEARG="--no-cache";;
 b) BUILD_PARAMS=${BUILD_PARAMS}" --build-arg "${OPTARG};;
 X) CROSS_BUILD=1;;
 u) KYUUBI_UID=${OPTARG};;
 s) SPARK_HOME=${OPTARG};;
 S) SPARK_HOME_IN_DOCKER=${OPTARG};;
 esac
done

. "${KYUUBI_HOME}/bin/load-kyuubi-env.sh"
case "${@: -1}" in
  build)
    build
    ;;
  push)
    if [ -z "$REPO" ]; then
      usage
      exit 1
    fi
    push
    ;;
  *)
    usage
    exit 1
    ;;
esac
