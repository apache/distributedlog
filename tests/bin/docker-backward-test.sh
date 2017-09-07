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

main() {
    local num_records=20

    # stop cluster if there is already one    
    stop_cluster

    # start the cluster (the cluster starts 0.4.0 bookies)
    start_cluster

    # wait bookies 
    wait_bookies $NETWORK $ZK_NAME 3

    #########################################
    # backend = 0.4.0, stream = 0.4.0
    #########################################

    # write records using 0.4.0 version
    write_records ${NETWORK} ${ZK_NAME} ${DL_NAMESPACE} ${num_records} ${BACKWARD_TEST_040} ${STREAM_040}

    # read records using 0.4.0 version
    read_records ${NETWORK} ${ZK_NAME} ${DL_NAMESPACE} ${num_records} 1 ${BACKWARD_TEST_040} ${STREAM_040} "dlog_reader_040"

    # read records using latest version
    read_records ${NETWORK} ${ZK_NAME} ${DL_NAMESPACE} ${num_records} 1 ${BACKWARD_TEST_LATEST} ${STREAM_040} "dlog_reader_latest"

    # tailing read records using 0.4.0 version
    nohup ${DLOG_ROOT}/tests/bin/docker-read-records.sh ${STREAM_040} ${num_records} $((num_records + 1)) ${BACKWARD_TEST_040} &

    ## tailing read records using latest version
    nohup ${DLOG_ROOT}/tests/bin/docker-read-records.sh ${STREAM_040} ${num_records} $((num_records + 1)) ${BACKWARD_TEST_LATEST} &

    ## write another 20 records using 0.4.0 version
    write_records ${NETWORK} ${ZK_NAME} ${DL_NAMESPACE} ${num_records} ${BACKWARD_TEST_040} ${STREAM_040}

    wait

    read_status_040=`cat /tmp/dlog_reader_${BACKWARD_TEST_040}`
    if [ $read_status_040 -eq 0 ]
    then
        echo "Successfully tailing read $num_records records using 0.4.0 reader."
    else
        echo "Failed to tailing read $num_records records using 0.4.0 reader."
        exit -1
    fi

    read_status_latest=`cat /tmp/dlog_reader_${BACKWARD_TEST_LATEST}`
    if [ $read_status_latest -eq 0 ]
    then
        echo "Successfully tailing read $num_records records using 0.4.0 reader."
    else
        echo "Failed to tailing read $num_records records using 0.4.0 reader."
        exit -1
    fi

    #########################################
    # upgrade backend 0.4.0 from latest
    #########################################

    # stop 0.4.0 bookies
    echo "stop bookies (v0.4.0) ..."
    stop_bookies $NUM_BOOKIES

    # start latest bookies
    echo "start bookies (latest) ..."
    start_bookies $NETWORK $ZK_NAME $BK_BACKEND_LATEST $NUM_BOOKIES

    # wait bookies
    wait_bookies $NETWORK $ZK_NAME 3
    sleep 30

    # read records using 0.4.0 version
    read_records ${NETWORK} ${ZK_NAME} ${DL_NAMESPACE} $((2 * num_records)) 1 ${BACKWARD_TEST_040} ${STREAM_040} "dlog_reader_040"

    # read records using latest version
    read_records ${NETWORK} ${ZK_NAME} ${DL_NAMESPACE} $((2 * num_records)) 1 ${BACKWARD_TEST_LATEST} ${STREAM_040} "dlog_reader_latest"

    # tailing read records using 0.4.0 version
    nohup ${DLOG_ROOT}/tests/bin/docker-read-records.sh ${STREAM_040} ${num_records} $((2 * num_records + 1)) ${BACKWARD_TEST_040} &

    ## tailing read records using latest version
    nohup ${DLOG_ROOT}/tests/bin/docker-read-records.sh ${STREAM_040} ${num_records} $((2 * num_records + 1)) ${BACKWARD_TEST_LATEST} &

    ## write another 20 records using 0.4.0 version
    write_records ${NETWORK} ${ZK_NAME} ${DL_NAMESPACE} ${num_records} ${BACKWARD_TEST_040} ${STREAM_040}

    wait

    read_status_040=`cat /tmp/dlog_reader_${BACKWARD_TEST_040}`
    if [ $read_status_040 -eq 0 ]
    then
        echo "Successfully tailing read $num_records records using 0.4.0 reader."
    else
        echo "Failed to tailing read $num_records records using 0.4.0 reader."
        exit -1
    fi

    read_status_latest=`cat /tmp/dlog_reader_${BACKWARD_TEST_LATEST}`
    if [ $read_status_latest -eq 0 ]
    then
        echo "Successfully tailing read $num_records records using 0.4.0 reader."
    else
        echo "Failed to tailing read $num_records records using 0.4.0 reader."
        exit -1
    fi

    # stop the cluster
    stop_cluster
}

main
