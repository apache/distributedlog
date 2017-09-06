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
cd $ROOT_DIR/docker

MVN_VERSION=`./get-version.sh`
ALL_MODULE="distributedlog-dist"
DOCKER_IMAGE_NAME="distributedlog"
DOCKER_GRAFANA_NAME="distributedlog-grafana"
DOCKER_IMAGE_VERSION="nightly"

echo "distributedlog version: ${MVN_VERSION}"

DLOG_TGZ=$(dirname $PWD)/distributedlog-dist/target/${ALL_MODULE}-${MVN_VERSION}-bin.tar.gz

if [ ! -f $DLOG_TGZ ]; then
    echo "distributedlog bin distribution not found at ${DLOG_TGZ}"
    exit 1
fi

LINKED_DLOG_TGZ=${ALL_MODULE}-${MVN_VERSION}-bin.tar.gz
ln -f ${DLOG_TGZ} $LINKED_DLOG_TGZ

echo "Using distributedlog binary package at ${DLOG_TGZ}"

# Build base image, reused by all other components
docker build --build-arg VERSION=${MVN_VERSION} \
             -t ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION} .

if [ $? != 0 ]; then
    echo "Error: Failed to create Docker image for distributedlog"
    exit 1
fi

rm ${ALL_MODULE}-${MVN_VERSION}-bin.tar.gz

# Build distributedlog-grafana image
docker build -t ${DOCKER_GRAFANA_NAME}:${DOCKER_IMAGE_VERSION} grafana
if [ $? != 0 ]; then
    echo "Error: Failed to create Docker image for distributedlog-grafana"
    exit 1
fi
