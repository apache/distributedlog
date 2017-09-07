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

ARG VERSION

ENV BOOKIE_PORT=3181
EXPOSE $BOOKIE_PORT
ENV DL_USER=distributedlog

# install dependencies
RUN set -x \
    && yum install -y wget \
    && wget -q https://bootstrap.pypa.io/get-pip.py \
    && python get-pip.py \
    && pip install zk-shell \
    && mkdir -pv /opt \
    && rm -rf get-pip.py \
    && yum remove -y wget \
    && yum clean all

ADD distributedlog-dist-${VERSION}-bin.tar.gz /opt
RUN mv /opt/distributedlog-dist-${VERSION} /opt/distributedlog

COPY scripts/apply-config-from-env.py /opt/distributedlog/bin
COPY scripts/gen-zk-config.sh /opt/distributedlog/bin
COPY scripts/zk-ruok.sh /opt/distributedlog/bin
COPY scripts/entrypoint.sh /opt/distributedlog/bin
COPY scripts/wait_bookies.sh /opt/distributedlog/bin

ENTRYPOINT [ "/bin/bash", "/opt/distributedlog/bin/entrypoint.sh" ]
CMD ["/opt/bookkeeper/bin/bookkeeper", "bookie"]
