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

FROM apache/bookkeeper:4.5.0
MAINTAINER Apache DistributedLog <distributedlog-dev@bookkeeper.apache.org>

ARG VERSION=0.4.0-incubating
ARG DISTRO_NAME=distributedlog-service_2.11-${VERSION}-bin
ARG GPG_KEY=FD74402C

ENV BOOKIE_PORT=3181
EXPOSE $BOOKIE_PORT
ENV DL_USER=distributedlog

# Download Apache DistributedLog, untar and clean up
RUN set -x \
    && adduser "${DL_USER}" \
    && chown "${DL_USER}:${DL_USER}" -R /opt/bookkeeper \
    && yum install -y wget \
    && wget -q https://bootstrap.pypa.io/get-pip.py \
    && python get-pip.py \
    && pip install zk-shell \
    && mkdir -pv /opt \
    && cd /opt \
    && wget -q "https://dist.apache.org/repos/dist/release/bookkeeper/distributedlog/${VERSION}/${DISTRO_NAME}.tar.gz" \
    && wget -q "https://dist.apache.org/repos/dist/release/bookkeeper/distributedlog/${VERSION}/${DISTRO_NAME}.tar.gz.asc" \
    && wget -q "https://dist.apache.org/repos/dist/release/bookkeeper/distributedlog/${VERSION}/${DISTRO_NAME}.tar.gz.md5" \
    && wget -q "https://dist.apache.org/repos/dist/release/bookkeeper/distributedlog/${VERSION}/${DISTRO_NAME}.tar.gz.sha1" \
    && md5sum -c ${DISTRO_NAME}.tar.gz.md5 \
    && sha1sum -c ${DISTRO_NAME}.tar.gz.sha1 \
    && gpg --keyserver ha.pool.sks-keyservers.net --recv-key "$GPG_KEY" \
    && gpg --batch --verify "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz" \
    && tar -xzf "${DISTRO_NAME}.tar.gz" \
    && mv distributedlog-service_2.11-${VERSION}/ /opt/distributedlog/ \
    && rm -rf "$DISTRO_NAME.tar.gz" "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz.md5" "$DISTRO_NAME.tar.gz.sha1" \
    && rm -rf get-pip.py \
    && rm /opt/distributedlog/bin/* \
    && rm /opt/distributedlog/conf/* \
    && yum remove -y wget \
    && yum clean all

COPY bin/* /opt/distributedlog/bin/
COPY conf/* /opt/distributedlog/conf/
COPY scripts/apply-config-from-env.py /opt/distributedlog/bin
COPY scripts/gen-zk-config.sh /opt/distributedlog/bin
COPY scripts/zk-ruok.sh /opt/distributedlog/bin
COPY scripts/entrypoint.sh /opt/distributedlog/bin
COPY scripts/wait_bookies.sh /opt/distributedlog/bin

ENTRYPOINT [ "/bin/bash", "/opt/distributedlog/bin/entrypoint.sh" ]
WORKDIR /opt/distributedlog
