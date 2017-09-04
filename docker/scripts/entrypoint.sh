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

export PATH=$PATH:/opt/distributedlog/bin
export JAVA_HOME=/usr

# env var used often
PORT0=${PORT0:-${BOOKIE_PORT}}
PORT0=${PORT0:-3181}
DL_DATA_DIR=${DL_DATA_DIR:-"/data/bookkeeper"}
DL_CLUSTER_ROOT_PATH=${DL_CLUSTER_ROOT_PATH:-"/messaging/bookkeeper"}

# env vars to replace values in config files
export DL_bookiePort=${DL_bookiePort:-${PORT0}}
export DL_zkServers=${DL_zkServers}
export DL_zkLedgersRootPath=${DL_zkLedgersRootPath:-"${DL_CLUSTER_ROOT_PATH}/ledgers"}
export DL_journalDirectory=${DL_journalDirectory:-${DL_DATA_DIR}/journal}
export DL_ledgerDirectories=${DL_ledgerDirectories:-${DL_DATA_DIR}/ledgers}
export DL_indexDirectories=${DL_indexDirectories:-${DL_DATA_DIR}/index}

echo "DL_bookiePort bookie service port is $DL_bookiePort"
echo "DL_zkServers is $DL_zkServers"
echo "DL_DATA_DIR is $DL_DATA_DIR"
echo "DL_CLUSTER_ROOT_PATH is $DL_CLUSTER_ROOT_PATH"

mkdir -p "${DL_journalDirectory}" "${DL_ledgerDirectories}" "${DL_indexDirectories}"
# -------------- #
# Allow the container to be started with `--user`
if [ "$1" = '/opt/distributedlog/bin/dlog' -a "$(id -u)" = '0' ]; then
    chown -R "$DL_USER:$DL_USER" "/opt/distributedlog/" "${DL_journalDirectory}" "${DL_ledgerDirectories}" "${DL_indexDirectories}"
    sudo -s -E -u "$DL_USER" /bin/bash "$0" "$@"
    exit
fi
# -------------- #

python /opt/distributedlog/bin/apply-config-from-env.py /opt/distributedlog/conf

# Create the ledger root path

ZK_ROOT_PATH=$(dirname $DL_zkLedgersRootPath)
zk-shell --run-once "create $ZK_ROOT_PATH '' false false true" $DL_zkServers

exec "$@"
