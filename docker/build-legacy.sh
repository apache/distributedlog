#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

ROOT_DIR=$(git rev-parse --show-toplevel)
DOCKER_DIR="${ROOT_DIR}/docker"

DOCKER_IMAGE_NAME="distributedlog"

build_docker_image() {
    if [ -z "$1" ]; then
        echo "no distributedlog version is provided."
    fi
    version=$1
    echo "Building distributedlog image '${version}' ..."
    docker build --build-arg VERSION=${version} \
                 -t ${DOCKER_IMAGE_NAME}:${version} ${DOCKER_DIR}/legacy/${version}
    echo "Built distributedlog image '${version}'."
}

usage() {
    cat <<EOF
Usage: build-legacy.sh [version]

NOTE: if version is omitted, all versions are built.
EOF
}

if [ $# -lt 1 ]; then
    version="all"
else
    version=$1
fi

case $version in
    (all)
        for dir in `ls ${DOCKER_DIR}/legacy`; do
            build_docker_image $dir
        done
        ;;
    (*)
        build_docker_image $version 
        ;;
esac
