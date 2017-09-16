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
  echo "Usage: build <env> [dest]."
}

if [ $# -lt 1 ]; then
  usage
  exit 1
fi

DLOG_ENV=$1

BINDIR=`dirname "$0"`
DLOG_HOME=`cd $BINDIR/.. > /dev/null;pwd`

if [ $# -gt 1 ]; then
  DEST_DIR=$2
else 
  DEST_DIR=${DLOG_HOME}/website
fi

CONTENT_DIR=${DLOG_ENV}_content
TMP_DEST_DIR=${DEST_DIR}/temp_${CONTENT_DIR}
TMP_WEBSITE_DIR=${TMP_DEST_DIR}/website

# build the documents

function build_docs() {
  version=$1
  tag=$2

  DOC_DEST_HOME="${TMP_DEST_DIR}/docs_${version}"

  echo "Building the javadoc for version ${version} to ${DOC_DEST_HOME} ..."

  # create the api directory
  mkdir -p ${DOC_DEST_HOME}/api/java
  if [ "$version" == "latest" ]; then
    cd ${DLOG_HOME}
    # build the javadoc
    mvn -DskipTests clean package javadoc:aggregate \
        -Ddoctitle="Apache DistributedLog for Java, version ${version}" \
        -Dwindowtitle="Apache DistributedLog for Java, version ${version}" \
        -Dmaven.javadoc.failOnError=false
    # copy the built javadoc
    cp -r ${DLOG_HOME}/target/site/apidocs/* ${DOC_DEST_HOME}/api/java
  else
    rm -rf /tmp/distributedlog-${version}
    git clone https://github.com/apache/distributedlog.git /tmp/distributedlog-${version}
    cd /tmp/distributedlog-${version}
    git checkout $tag
    # build the javadoc
    mvn -DskipTests clean package javadoc:aggregate \
        -Ddoctitle="Apache DistributedLog for Java, version ${version}" \
        -Dwindowtitle="Apache DistributedLog for Java, version ${version}" \
        -Dnotimestamp \
        -Dmaven.javadoc.failOnError=false
    # copy the built javadoc
    cp -r /tmp/distributedlog-${version}/target/site/apidocs/* ${DOC_DEST_HOME}/api/java
  fi
  echo "Built the javadoc for version ${version} in ${DOC_DEST_HOME}."
}

copy_docs() {
  version=$1
  [[ -d ${DEST_DIR}/${CONTENT_DIR}/docs/${version}/api/java ]] && rm -r ${DEST_DIR}/${CONTENT_DIR}/docs/${version}/api/java
  mkdir -p ${DEST_DIR}/${CONTENT_DIR}/docs/${version}/api
  cp -r ${TMP_DEST_DIR}/docs_${version}/api/java ${DEST_DIR}/${CONTENT_DIR}/docs/${version}/api/java 
}

# build the javadoc API

build_docs "latest"
build_docs "0.4.0-incubating" "v0.4.0-incubating_2.11"
build_docs "0.5.0" "v0.5.0"
copy_docs "latest"
copy_docs "0.4.0-incubating"
copy_docs "0.5.0"
