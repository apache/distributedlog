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
  DEST_DIR=${DLOG_HOME}/website
fi

SERVE="FALSE"
if [ $# -gt 3 ]; then
  SERVE="TRUE"
fi

CONTENT_DIR=${DLOG_ENV}_content
TMP_DEST_DIR=${DEST_DIR}/temp_${CONTENT_DIR}
TMP_WEBSITE_DIR=${TMP_DEST_DIR}/website

rm -rf ${DEST_DIR}/${CONTENT_DIR}
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

build_docs() {
  version=$1
  tag=$2

  DOC_SRC_HOME="${DLOG_HOME}/website/docs/${version}"
  DOC_DEST_HOME="${TMP_DEST_DIR}/docs_${version}"

  cd ${DOC_SRC_HOME}
  echo ""
  echo "@${DOC_SRC_HOME}"
  echo "Building the documentation for version ${version} to ${DOC_DEST_HOME} ..."

  bundle exec jekyll build --destination ${DOC_DEST_HOME} --config _config.yml,${OVERRIDED_CONFIG}

  echo "Built the documentation for version ${version} in ${DOC_DEST_HOME}."
}

copy_docs() {
  version=$1

  [[ -d ${DEST_DIR}/${CONTENT_DIR}/docs/${version} ]] && rm -r ${DEST_DIR}/${CONTENT_DIR}/docs/${version}
  cp -r ${TMP_DEST_DIR}/docs_${version} ${DEST_DIR}/${CONTENT_DIR}/docs/${version}
}

# build the javadoc API

build_docs "latest"
build_docs "0.4.0-incubating" "v0.4.0-incubating_2.11"
build_docs "0.5.0" "v0.5.0"

cp -r ${TMP_DEST_DIR}/website ${DEST_DIR}/${CONTENT_DIR}
mkdir -p ${DEST_DIR}/${CONTENT_DIR}/docs
copy_docs "latest"
copy_docs "0.4.0-incubating"
copy_docs "0.5.0"

if [[ "${SERVE}" == "TRUE" ]]; then
  cd ${DLOG_HOME}/website
  bundle exec jekyll serve --destination ${DEST_DIR}/${CONTENT_DIR} --config _config.yml,${OVERRIDED_CONFIG} --incremental
fi
