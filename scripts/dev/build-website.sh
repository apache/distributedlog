#!/usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

DLOG_ENV=$1

set -e

PROJECT_NAME="incubator-distributedlog"
CAPITALIZED_PROJECT_NAME="DL"

BASEDIR=$(dirname "$0")
DLOG_ROOT="${BASEDIR}/../.."
DLOG_ROOT=`cd $DLOG_ROOT > /dev/null;pwd`
# Location of the local git repository
REPO_HOME=${DL_HOME:-"$DLOG_ROOT"}
SITE_REMOTE="https://git-wip-us.apache.org/repos/asf/${PROJECT_NAME}.git"
BUILT_DIR=${DLOG_ROOT}/build/website

# remove the built content first
rm -rf ${BUILT_DIR}

# Prefix added to temporary branches
TEMP_BRANCH_PREFIX="PR_WEBSITE_"

# BRANCHES
SRC_BRANCH="master"
SITE_BRANCH="asf-site"

# fetch apache/master
git fetch apache ${SRC_BRANCH}

# checkout apache/master
git checkout "apache/master"

# build the websiste
echo "Building the website to ${BUILT_DIR} ..."

echo ${DLOG_ROOT}/website/build.sh ${DLOG_ENV} ${BUILT_DIR}
${DLOG_ROOT}/website/build.sh ${DLOG_ENV} ${BUILT_DIR}

echo "Built the website into ${BUILT_DIR}."

# checkout asf-site
git checkout "apache/asf-site"

# cp the built content
cp -r ${BUILT_DIR}/content ${DLOG_ROOT}/content


