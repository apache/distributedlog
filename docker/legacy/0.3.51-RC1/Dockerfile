#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM centos:7
MAINTAINER Apache DistributedLog <distributedlog-dev@bookkeeper.apache.org>

ARG VERSION=0.3.51-RC1
ARG GITSHA=3ff9e33fa577f50eebb8ee971ddb265c971c3717
ARG DISTRO_NAME=distributedlog-service-${GITSHA}

ENV BOOKIE_PORT=3181
EXPOSE $BOOKIE_PORT
ENV DL_USER=distributedlog

# Download Apache DistributedLog, untar and clean up
RUN set -x \
    && adduser "${DL_USER}" \
    && yum install -y java-1.8.0-openjdk-headless wget bash python unzip sudo \
    && mkdir -pv /opt \
    && cd /opt \
    && wget -q "https://github.com/twitter/distributedlog/releases/download/${VERSION}/${DISTRO_NAME}.zip" \
    && unzip ${DISTRO_NAME}.zip \
    && mv distributedlog-service/ /opt/distributedlog/ \
    && rm -rf "$DISTRO_NAME.zip" \
    && yum remove -y wget \
    && yum clean all

WORKDIR /opt/distributedlog

VOLUME ["/opt/distributedlog/conf", "/data"]
