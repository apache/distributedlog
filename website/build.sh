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
if [ $# -gt 3 ]; then
  SERVE="TRUE"
fi

TMP_DEST_DIR=${DEST_DIR}/temp_content
TMP_WEBSITE_DIR=${TMP_DEST_DIR}/website

rm -rf ${DEST_DIR}/content
rm -rf ${TMP_DEST_DIR}

if [ ! -d "${DLOG_HOME}/website/docs" ]; then
  mkdir ${DLOG_HOME}/website/docs
fi

echo "Building the website to ${TMP_WEBSITE_DIR} ..."

# build the website
cd ${DLOG_HOME}/website

bundle exec jekyll build --destination ${TMP_WEBSITE_DIR} --config _config.yml,${OVERRIDED_CONFIG}

echo "Built the website @ ${TMP_WEBSITE_DIR}."

# build the documents

function build_docs() {
  version=$1
  tag=$2

  echo "Building the documentation for version ${version} ..."

  DOC_SRC_HOME="${DLOG_HOME}/website/docs/${version}"
  DOC_DEST_HOME="${TMP_DEST_DIR}/docs_${version}"

  cd ${DOC_SRC_HOME}

  bundle exec jekyll build --destination ${DOC_DEST_HOME} --config _config.yml,${OVERRIDED_CONFIG}

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
  echo "Built the documentation for version ${version}."
}

# build the javadoc API

build_docs "latest"
build_docs "0.4.0-incubating" "v0.4.0-incubating_2.11"
build_docs "0.5.0" "v0.5.0"

cp -r ${TMP_DEST_DIR}/website ${DEST_DIR}/content
mkdir -p ${DEST_DIR}/content/docs
[[ -d ${DEST_DIR}/content/docs/latest ]] && rm -r ${DEST_DIR}/content/docs/latest
[[ -d ${DEST_DIR}/content/docs/0.4.0-incubating ]] && rm -r ${DEST_DIR}/content/docs/0.4.0-incubating
[[ -d ${DEST_DIR}/content/docs/0.5.0]] && rm -r ${DEST_DIR}/content/docs/0.5.0
cp -r ${TMP_DEST_DIR}/docs_latest ${DEST_DIR}/content/docs/latest
cp -r ${TMP_DEST_DIR}/docs_0.4.0-incubating ${DEST_DIR}/content/docs/0.4.0-incubating
cp -r ${TMP_DEST_DIR}/docs_0.5.0 ${DEST_DIR}/content/docs/0.5.0

cp -r ${TMP_DEST_DIR}/website ${DEST_DIR}/content
mkdir -p ${DEST_DIR}/content/docs
cp -r ${TMP_DEST_DIR}/docs_latest ${DEST_DIR}/content/docs/latest
cp -r ${TMP_DEST_DIR}/docs_0.4.0-incubating ${DEST_DIR}/content/docs/0.4.0-incubating
cp -r ${TMP_DEST_DIR}/docs_0.5.0 ${DEST_DIR}/content/docs/0.5.0

if [[ "${SERVE}" == "TRUE" ]]; then
  cd ${DLOG_HOME}/website
  bundle exec jekyll serve --destination ${DEST_DIR}/content --config _config.yml,${OVERRIDED_CONFIG} --incremental
fi
