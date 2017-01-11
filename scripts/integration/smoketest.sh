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

BASEDIR=$(dirname "$0")
DLOG_ROOT="${BASEDIR}/../.."

LOG_DIR="${DLOG_ROOT}/logs"
mkdir -p ${LOG_DIR}

# DL ZK & Namespace
NAMESPACE="${USER}-${RANDOM}"
ZK_PORT=7000

# write proxies
export WP_SHARD_ID=1
export WP_SERVICE_PORT=8000
export WP_STATS_PORT=8001
export WP_NAMESPACE="distributedlog://127.0.0.1:${ZK_PORT}/messaging/${NAMESPACE}"

# streams
SMOKESTREAM_PREFIX="smoketest-stream-"

# start the sandbox
nohup ${DLOG_ROOT}/distributedlog-service/bin/dlog local ${ZK_PORT} > ${LOG_DIR}/sandbox.out 2>&1&
echo $! > ${LOG_DIR}/sandbox.pid

# create namespace
${DLOG_ROOT}/distributedlog-core/bin/dlog admin bind -l /ledgers -s 127.0.0.1:${ZK_PORT} -c distributedlog://127.0.0.1:${ZK_PORT}/messaging/${NAMESPACE}

if [ $? -ne 0 ]; then
  echo "Failed to create namespace '${NAMESPACE}'."
  exit 1
fi

# create streams
${DLOG_ROOT}/distributedlog-service/bin/dlog tool create -u distributedlog://127.0.0.1:${ZK_PORT}/messaging/${NAMESPACE} -r ${SMOKESTREAM_PREFIX} -e 1-5 -f

if [ $? -ne 0 ]; then
  echo "Failed to create streams prefixed with '${SMOKESTREAM_PREFIX}'."
  exit 1
fi

# list streams
STREAM_OUTPUT=$(${DLOG_ROOT}/distributedlog-service/bin/dlog tool list -u distributedlog://127.0.0.1:${ZK_PORT}/messaging/${NAMESPACE} | grep "${SMOKESTREAM_PREFIX}")

echo "Create streams : ${STREAM_OUTPUT}."

for i in {1..5}; do
  if [[ ${STREAM_OUTPUT} != *"${SMOKESTREAM_PREFIX}${i}"* ]]; then
    echo "Stream '${SMOKESTREAM_PREFIX}${i}' is not created."
    exit 1
  fi
done

# start a write proxy
${DLOG_ROOT}/distributedlog-service/bin/dlog-daemon.sh start writeproxy

# tail the the streams
nohup ${DLOG_ROOT}/distributedlog-tutorials/distributedlog-basic/bin/runner run org.apache.distributedlog.basic.MultiReader distributedlog://127.0.0.1:${ZK_PORT}/messaging/${NAMESPACE} ${SMOKESTREAM_PREFIX}1,${SMOKESTREAM_PREFIX}2,${SMOKESTREAM_PREFIX}3,${SMOKESTREAM_PREFIX}4,${SMOKESTREAM_PREFIX}5 > ${LOG_DIR}/reader.out 2>&1&
echo $! > ${LOG_DIR}/reader.pid

# generate the records
nohup ${DLOG_ROOT}/distributedlog-tutorials/distributedlog-basic/bin/runner run org.apache.distributedlog.basic.RecordGenerator "inet!127.0.0.1:${WP_SERVICE_PORT}" ${SMOKESTREAM_PREFIX}1 1 > ${LOG_DIR}/writer.out 2>&1&
echo $! > ${LOG_DIR}/writer.pid

# wait for 20 seconds
sleep 20

# kill the writer
if [ -f ${LOG_DIR}/writer.pid ]; then
  writerpid=$(cat ${LOG_DIR}/writer.pid) 
  if kill -0 $writerpid > /dev/null 2>&1; then
    kill $writerpid
  fi
fi

# stop the reader
if [ -f ${LOG_DIR}/reader.pid ]; then
  readerpid=$(cat ${LOG_DIR}/reader.pid) 
  if kill -0 $readerpid > /dev/null 2>&1; then
    kill $readerpid
  fi
fi

# check the number of records received
NUM_RECORDS=`cat ${LOG_DIR}/reader.out | grep "record-" | wc -l`

echo "Received ${NUM_RECORDS} records : "
cat ${LOG_DIR}/reader.out

if [ $NUM_RECORDS -lt 18 ]; then
  echo "[FAILURE] Received less than 18 records."
  exit 1
fi

# stop a write proxy
${DLOG_ROOT}/distributedlog-service/bin/dlog-daemon.sh stop writeproxy

# stop the sandbox
kill `cat ${LOG_DIR}/sandbox.pid`

exit 0
