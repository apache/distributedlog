#!/bin/bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

usage() {
  echo "Usage: build <env> [dest] [serve]."
}

if [ $# -lt 1 ]; then
  usage
  exit 1
fi

DLOG_ENV=$1

OVERRIDED_CONFIG=_config-${DLOG_ENV}.yml

BINDIR=`dirname "$0"`
DLOG_HOME=`cd $BINDIR/.. > /dev/null;pwd`

if [ $# -gt 1 ]; then
  DEST_DIR=$2
else 
  DEST_DIR=${DLOG_HOME}
fi

SERVE="FALSE"
if [ $# -gt 2 ]; then
  SERVE="TRUE"
fi

rm -rf ${DEST_DIR}/content

if [ ! -d "${DLOG_HOME}/website/docs" ]; then
  mkdir ${DLOG_HOME}/website/docs
fi

# Get the project version
PROJECT_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version 2> /dev/null | grep -Ev '(^\[|Download\w+:)')

echo "Building the website to ${DEST_DIR}/content ..."

# build the website
cd ${DLOG_HOME}/website

bundle exec jekyll build --destination ${DEST_DIR}/content --config _config.yml,${OVERRIDED_CONFIG}

echo "Built the website @ ${DEST_DIR}/content."

echo "Building the documentation for version ${PROJECT_VERSION} ..."

# build the documents

if [[ ${PROJECT_VERSION} == *"SNAPSHOT"* ]]; then
  # it is a snapshot version, built the docs into latest
  DOC_SRC_HOME="${DLOG_HOME}/website/docs/latest"
  DOC_DEST_HOME="${DEST_DIR}/content/docs/latest"
else
  # it is a release version, built the docs to release-version directory.
  DOC_SRC_HOME="${DLOG_HOME}/website/docs/${PROJECT_VERSION}"
  DOC_DEST_HOME="${DEST_DIR}/content/docs/${PROJECT_VERSION}"
fi

# link the doc source directory if necessary
if [ ! -d "${DOC_SRC_HOME}" ]; then
  ln -s ../../docs ${DOC_SRC_HOME} 
fi

# create the doc dest directory
mkdir -p ${DOC_DEST_HOME}

cd ${DOC_SRC_HOME}

bundle exec jekyll build --destination ${DOC_DEST_HOME} --config _config.yml,${OVERRIDED_CONFIG}

# build the javadoc API

cd ${DLOG_HOME}
# create the api directory
mkdir -p ${DEST_DIR}/content/docs/latest/api/java
# build the javadoc
mvn -DskipTests clean package javadoc:aggregate \
    -Ddoctitle="Apache DistributedLog for Java, version ${PROJECT_VERSION}" \
    -Dwindowtitle="Apache DistributedLog for Java, version ${PROJECT_VERSION}" \
    -Dmaven.javadoc.failOnError=false
# copy the built javadoc
cp -r ${DLOG_HOME}/target/site/apidocs/* ${DOC_DEST_HOME}/api/java

echo "Built the documentation for version ${PROJECT_VERSION}."

if [[ "${SERVE}" == "TRUE" ]]; then
  cd ${DLOG_HOME}/website
  bundle exec jekyll serve --destination ${DEST_DIR}/content --config _config.yml,${OVERRIDED_CONFIG} --incremental
fi

