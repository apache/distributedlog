#!/bin/sh
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

##################
# General
##################

# Log4j configuration file
# DLOG_LOG_CONF=

DLOG_JVM_NUM_GC_THREADS=4

# MEM settings
DLOG_JVM_MEM_OPTS="-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g -XX:MaxMetaspaceSize=128M -XX:MetaspaceSize=128M"

# GC Settings
DLOG_JVM_GC_OPTS="-XX:ParallelGCThreads=${DLOG_JVM_NUM_GC_THREADS} -Xloggc:gc.log -XX:+CMSScavengeBeforeRemark -XX:TargetSurvivorRatio=90 -XX:+PrintCommandLineFlags -verbosegc -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=64M -XX:+UseGCLogFileRotation -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC"

# Extra options to be passed to the jvm
DLOG_EXTRA_OPTS="${DLOG_JVM_MEM_OPTS} ${DLOG_JVM_GC_OPTS} -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.linkCapacity=1024"

# Add extra paths to the dlog classpath
# DLOG_EXTRA_CLASSPATH=

# Configure the root logger
# DLOG_ROOT_LOGGER=

# Configure the log dir
# DLOG_LOG_DIR=

# Configure the log file
# DLOG_LOG_FILE=

#################
# ZooKeeper
#################

# Configure zookeeper root logger
# ZK_ROOT_LOGGER=

#################
# Bookie
#################

# Configure bookie root logger
# BK_ROOT_LOGGER=

#################
# Write Proxy
#################

# Configure write proxy root logger
# WP_ROOT_LOGGER=

# write proxy configuration file
# WP_CONF_FILE=${DL_HOME}/conf/write_proxy.conf

# port and stats port
# WP_SERVICE_PORT=4181
# WP_STATS_PORT=9000

# shard id
# WP_SHARD_ID=0

# write proxy namespace
# WP_NAMESPACE=distributedlog://127.0.0.1:2181/messaging/distributedlog/mynamespace
