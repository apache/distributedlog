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

set -e

PROJECT_NAME="incubator-distributedlog"
CAPITALIZED_PROJECT_NAME="DL"
# Location of the local git repository
REPO_HOME=${DL_HOME:-"$PWD"}
SITE_REMOTE="https://git-wip-us.apache.org/repos/asf/${PROJECT_NAME}.git"

# Prefix added to temporary branches
TEMP_BRANCH_PREFIX="PR_WEBSITE_"

# BRANCHES
SRC_BRANCH="master"
SITE_BRANCH="asf-site"

# fetch apache/master
git fetch apache ${SRC_BRANCH}

# create a temp branch to build the website
SITE_MERGE_BRANCH="#{TEMP_BRANCH_PREFIX}_MERGE_${RANDOM}"
git checkout -b ${SITE_MERGE_BRANCH}
