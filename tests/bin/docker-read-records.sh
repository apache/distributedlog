#!/usr/bin/env bash
#
#/**
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

DLOG_ROOT=$(git rev-parse --show-toplevel)

source "${DLOG_ROOT}/tests/bin/docker-common.sh"

stream_name=$1
num_records=$2
start_tx_id=$3
test_version=$4

reader_name="dlog_reader_${test_version}"

[ -f /tmp/${reader_name} ] && rm /tmp/${reader_name}

read_records ${NETWORK} ${ZK_NAME} ${DL_NAMESPACE} ${num_records} ${start_tx_id} ${test_version} ${stream_name} ${reader_name}

echo "$?" > /tmp/${reader_name}
