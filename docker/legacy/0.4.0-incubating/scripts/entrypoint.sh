#!/bin/bash
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

export PATH=$PATH:/opt/bookkeeper/bin:/opt/distributedlog/bin
export JAVA_HOME=/usr

# env var used often
PORT0=${PORT0:-${BOOKIE_PORT}}
PORT0=${PORT0:-3181}
BK_DATA_DIR=${BK_DATA_DIR:-"/data/bookkeeper"}
BK_CLUSTER_ROOT_PATH=${BK_CLUSTER_ROOT_PATH:-"/bookkeeper"}

# env vars to replace values in config files
export BK_bookiePort=${BK_bookiePort:-${PORT0}}
export BK_zkServers=${BK_zkServers}
export BK_zkLedgersRootPath=${BK_zkLedgersRootPath:-"${BK_CLUSTER_ROOT_PATH}/ledgers"}
export BK_journalDirectory=${BK_journalDirectory:-${BK_DATA_DIR}/journal}
export BK_ledgerDirectories=${BK_ledgerDirectories:-${BK_DATA_DIR}/ledgers}
export BK_indexDirectories=${BK_indexDirectories:-${BK_DATA_DIR}/index}

echo "BK_bookiePort bookie service port is $BK_bookiePort"
echo "BK_zkServers is $BK_zkServers"
echo "BK_DATA_DIR is $BK_DATA_DIR"
echo "BK_CLUSTER_ROOT_PATH is $BK_CLUSTER_ROOT_PATH"

mkdir -p "${BK_journalDirectory}" "${BK_ledgerDirectories}" "${BK_indexDirectories}"
# -------------- #
# Allow the container to be started with `--user`
if [ "$1" = '/opt/distributedlog/bin/dlog' -a "$(id -u)" = '0' ]; then
    chown -R "$BK_USER:$BK_USER" "/opt/bookkeeper/" "/opt/distributedlog/" "${BK_journalDirectory}" "${BK_ledgerDirectories}" "${BK_indexDirectories}"
    sudo -s -E -u "$BK_USER" /bin/bash "$0" "$@"
    exit
fi
# -------------- #

python /opt/bookkeeper/apply-config-from-env.py /opt/bookkeeper/conf
python /opt/bookkeeper/apply-config-from-env.py /opt/distributedlog/conf
python /opt/distributedlog/bin/apply-config-from-env.py /opt/distributedlog/conf

# Create the ledger root path
ZK_ROOT_PATH=$(dirname $BK_zkLedgersRootPath)
ROOT_EXISTS=`zk-shell --run-once "exists ${ZK_ROOT_PATH}" $BK_zkServers`
if [ "Path ${ZK_ROOT_PATH} doesn't exist" == "$ROOT_EXISTS" ]; then
    zk-shell --run-once "create $ZK_ROOT_PATH '' false false true" $BK_zkServers
fi

# Format zk metadata
ROOT_EXISTS=`zk-shell --run-once "exists $BK_zkLedgersRootPath" $BK_zkServers`
if [ "Path ${BK_zkLedgersRootPath} doesn't exist" == "$ROOT_EXISTS" ]; then
    echo "format zk metadata"
    echo "please ignore the failure, if it has already been formatted, "
    export BOOKIE_CONF=/opt/bookkeeper/conf/bk_server.conf
    export SERVICE_PORT=$PORT0
    /opt/bookkeeper/bin/bookkeeper shell metaformat -n || true
fi

# Create distributedlog namespace
ROOT_EXISTS=`zk-shell --run-once "exists /distributedlog" $BK_zkServers`
if [ "Path /distributedlog doesn't exist" == "$ROOT_EXISTS" ]; then
    /opt/distributedlog/bin/dlog admin bind -l $BK_zkLedgersRootPath -s ${BK_zkServers} -c distributedlog://${BK_zkServers}/distributedlog
fi

exec "$@"
