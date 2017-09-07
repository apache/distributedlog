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
TESTS_DIR="${ROOT_DIR}/tests"

# docker images
DOCKER_IMAGE_NAME="distributedlog-tests-${USER}"
DOCKER_IMAGE_VERSION="nightly"

# target jars
BENCHMARKS_JAR="$TESTS_DIR/jmh/target/jmh-benchmarks.jar"
BENCHMARKS_JAR_040="$TESTS_DIR/jmh-0.4/target/jmh-benchmarks-0.4.0.jar"
BACKWARD_TEST_JAR="$TESTS_DIR/backward-compat/target/backward-compat-test.jar"
BACKWARD_TEST_JAR_040="$TESTS_DIR/backward-compat-0.4/target/backward-compat-test-0.4.0.jar"

LINKED_BENCHMARKS_JAR="$TESTS_DIR/docker/jmh-benchmarks.jar"
LINKED_BENCHMARKS_JAR_040="$TESTS_DIR/docker/jmh-benchmarks-0.4.0.jar"
LINKED_BACKWARD_TEST_JAR="$TESTS_DIR/docker/backward-compat-test.jar"
LINKED_BACKWARD_TEST_JAR_040="$TESTS_DIR/docker/backward-compat-test-0.4.0.jar"

cd $ROOT_DIR

if [ ! -f $BENCHMARKS_JAR ] && [ ! -f $BENCHMARKS_JAR_040 ] && [ ! -f $BACKWARD_TEST_JAR ] && [ ! -f $BACKWARD_TEST_JAR_040 ]; then
    echo "Couldn't find benchmarks jar."
    read -p "Do you want me to run \`mvn clean package -DskipTests\' for you ? " answer
    case "${answer:0:1}" in
        y|Y )
            mvn clean package -DskipTests
            ;;
        * )
            exit 1
            ;;
    esac
fi

cp ${BENCHMARKS_JAR} ${LINKED_BENCHMARKS_JAR}
cp ${BENCHMARKS_JAR_040} ${LINKED_BENCHMARKS_JAR_040}
cp ${BACKWARD_TEST_JAR} ${LINKED_BACKWARD_TEST_JAR}
cp ${BACKWARD_TEST_JAR_040} ${LINKED_BACKWARD_TEST_JAR_040}
cp -r ${TESTS_DIR}/conf ${TESTS_DIR}/docker/conf

# Build the image
echo "Building ${DOCKER_IMAGE_NAME} image ..."
docker build -t ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION} ${TESTS_DIR}/docker
echo "Built ${DOCKER_IMAGE_NAME} image."

rm ${LINKED_BENCHMARKS_JAR}
rm ${LINKED_BENCHMARKS_JAR_040}
rm ${LINKED_BACKWARD_TEST_JAR}
rm ${LINKED_BACKWARD_TEST_JAR_040}
rm -r ${TESTS_DIR}/docker/conf
