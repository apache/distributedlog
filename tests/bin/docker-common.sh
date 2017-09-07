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

# cluster setup
NETWORK="dlog_network"
ZK_NAME="dlog_zookeeper"
BK_BACKEND_IMAGE="distributedlog"
BK_BACKEND_V040="0.4.0-incubating"
BK_BACKEND_LATEST="nightly"
NUM_BOOKIES=3
# Test
DL_NAMESPACE="/distributedlog"
BACKWARD_TEST_IMAGE="distributedlog-tests-${USER}:nightly"
BACKWARD_TEST_LATEST="backward-compat-test.jar"
BACKWARD_TEST_040="backward-compat-test-0.4.0.jar"
STREAM_LATEST="stream_latest"
STREAM_040="stream_040"

# create network: create_network <network>
create_network() {
    local network=$1
    # create the network
    echo "Create docker network '${network}'"
    [ ! "$(docker network ls | grep ${network})" ] && docker network create $network
    return 0
}

# remove network: remove_network <network>
remove_network() {
    local network=$1
    # remove docker network
    [ ! -z "$(docker network ls | grep ${network})" ] && docker network rm $network
    return 0
}

# stop docker image: stop_image <name>
stop_image() {
    local name=$1
    local container=$(docker ps -a -q -f "name=${name}")
    [ ! -z "${container}" ] && docker stop $container
    return 0
}

# remove docker image: remove_image <name>
remove_image() {
    local name=$1
    local container=$(docker ps -a -q -f "name=${name}")
    [ ! -z "${container}" ] && docker rm -f ${container}
    return 0
}

# create volume: create_volume <name>
create_volume() {
    name=$1
    docker volume create $name
    return 0
}

# remove volume: remove_volume <name>
remove_volume() {
    name=$1
    [ ! -z "$(docker volume ls | grep ${name})" ] && docker volume rm ${name}
    return 0
}

# start zookeeper: start_zookeeper <network> <name>
start_zookeeper() {
    local network=$1
    local name=$2
    # start a standalone zookeeper
    local container=$(docker ps -a -f "name=${name}" -q)
    [ ! -z "${container}" ] && docker rm -f ${container}
    echo "Start a standalone zookeeper '${name}'"
    docker run -d \
        --network $network \
        --name $name \
        --hostname $name \
        -p 2181:2181 \
        zookeeper
    return 0
}

# format bookkeeper: format_bookkeeper <network> <zookeeper> <bkversion>
format_bookkeeper() {
    local network=$1
    local zkname=$2
    local bkversion=$3
    echo "Format bookkeeper '${zkname}'"
    remove_image "bkmetaformat"
    docker run -i \
        --network $network \
        --env BK_zkServers=${zkname}:2181 \
        --name "bkmetaformat" \
        ${BK_BACKEND_IMAGE}:${bkversion} \
        /opt/distributedlog/bin/dlog org.apache.bookkeeper.bookie.BookieShell -conf /opt/distributedlog/conf/bookie.conf metaformat -n -f
    remove_image "bkmetaformat"
    return 0
}

# create namespace: create_namespace <network> <zkname> <bkversion> <namespace>
create_namespace() {
    local network=$1
    local zkname=$2
    local bkversion=$3
    local namespace=$4
    echo "Create distributedlog namespace '${namespace}'"
    remove_image "dlmetaformat" 
    docker run -i \
        --network $network \
        --env BK_zkServers=${zkname}:2181 \
        --name "dlmetaformat" \
        ${BK_BACKEND_IMAGE}:${bkversion} \
        /opt/distributedlog/bin/dlog admin bind -l /bookkeeper/ledgers -s ${zkname}:2181 -c distributedlog://${zkname}:2181${namespace} || yes
    remove_image "dlmetaformat"
    return 0
}

# start bookie: start_bookie <network> <zkname> <bkversion> <bkport>
start_bookie() {
    local network=$1
    local zkname=$2
    local bkversion=$3
    local bkport=$4
    local volume="bookie-volume-${bkport}"
    echo "Starting bookie @ port $bkport ..."
    docker run -d --rm \
        --network $network \
        --env BK_zkServers=${zkname}:2181 \
        --env BK_bookiePort=$bkport \
        --name "bookie-${bkport}" \
        --hostname "bookie-${bkport}" \
        --mount source=${volume},target=/data \
        ${BK_BACKEND_IMAGE}:${bkversion} \
        /opt/distributedlog/bin/dlog org.apache.bookkeeper.proto.BookieServer --conf /opt/distributedlog/conf/bookie.conf
    return 0
}

create_bookie_volumes() {
    local num_bookies=$1
    for ((n=0;n<${num_bookies};n++))
    do
        local bkport=$((3181+n))
        local volume="bookie-volume-${bkport}"
        echo "creating volume $volume"
        create_volume $volume
    done
    return 0
}

remove_bookie_volumes() {
    local num_bookies=$1
    for ((n=0;n<${num_bookies};n++))
    do
        local bkport=$((3181+n))
        local volume="bookie-volume-${bkport}"
        echo "removing volume $volume"
        remove_volume $volume
    done
    return 0
}

# start bookies: start_bookies <network> <zkname> <bkversion> <num_bookies>
start_bookies() {
    local network=$1
    local zkname=$2
    local bkversion=$3
    local num_bookies=$4
    for ((n=0;n<${num_bookies};n++))
    do
        local bkport=$((3181+n))
        local bkname="bookie-${bkport}"
        remove_image $bkname 
        start_bookie $network $zkname $bkversion $bkport
    done
    return 0
}

# stop bookies: stop_bookies <num_bookies>
stop_bookies() {
    local num_bookies=$1
    for ((n=0;n<${num_bookies};n++))
    do
        local bkport=$((3181+n))
        local bkname="bookie-${bkport}"
        echo "Stopping bookie $bkport ..."
        remove_image $bkname
    done
    return 0
}

wait_bookies() {
    local network=$1
    local zkname=$2
    local num_bookies=$1

    for ((n=0;n<${num_bookies};n++))
    do
        local bkport=$((3181+n))
        local bkname="bookie-${bkport}"
        until [ "`docker inspect -f {{.State.Running}} ${bkname}`" == "true" ]; do
            sleep 0.1;
        done
    done
    remove_image "waitbookies"
    docker run -i \
        --network $network \
        --env BK_zkServers=${zkname}:2181 \
        --name "waitbookies" \
        ${BK_BACKEND_IMAGE}:nightly \
        /opt/distributedlog/bin/wait_bookies.sh $zkname /bookkeeper/ledgers ${num_bookies} 2
    remove_image "waitbookies"
}

####################################

# main process

start_cluster() {
    # create network
    create_network $NETWORK

    # start zookeeper
    start_zookeeper $NETWORK $ZK_NAME

    # initialize metadata
    format_bookkeeper $NETWORK $ZK_NAME $BK_BACKEND_V040
    create_namespace $NETWORK $ZK_NAME $BK_BACKEND_V040 $DL_NAMESPACE

    # remove volumes
    remove_bookie_volumes $NUM_BOOKIES
    # create volumes
    create_bookie_volumes $NUM_BOOKIES

    # start v040 bookies
    start_bookies $NETWORK $ZK_NAME $BK_BACKEND_V040 $NUM_BOOKIES
}

stop_cluster() {
    # stop v040 bookies
    stop_bookies $NUM_BOOKIES

    # remove volumes
    remove_bookie_volumes $NUM_BOOKIES

    # stop zookeeper
    remove_image $ZK_NAME

    # remove network
    remove_network $NETWORK
}

warmup() {
    sleep 5
}

write_records() {
    local network=$1
    local zkname=$2
    local namespace=$3
    local num_records=$4
    local backward_test_jar=$5
    local stream_name=$6
    local image_name="dlog_writer"
    remove_image ${image_name}
    docker run -i \
        --network $network \
        --name ${image_name} \
        ${BACKWARD_TEST_IMAGE} \
        java -cp conf:lib/${backward_test_jar} -Dlog4j.configuration=log4j.properties org.apache.distributedlog.tests.backward.WriterTest \
        distributedlog://${zkname}:2181${namespace} ${stream_name} ${num_records}
    local rc=$?
    remove_image ${image_name}
    return $rc
}

read_records() {
    local network=$1
    local zkname=$2
    local namespace=$3
    local num_records=$4
    local start_tx_id=$5
    local backward_test_jar=$6
    local stream_name=$7
    local image_name=$8
    remove_image ${image_name}
    docker run -i \
        --network $network \
        --name ${image_name} \
        ${BACKWARD_TEST_IMAGE} \
        java -cp conf:lib/${backward_test_jar} -Dlog4j.configuration=log4j.properties org.apache.distributedlog.tests.backward.ReaderTest \
        distributedlog://${zkname}:2181${namespace} ${stream_name} ${num_records} ${start_tx_id}
    local rc=$?
    remove_image ${image_name}
    return $rc
}
