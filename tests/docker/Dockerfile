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

FROM openjdk:8-jdk
MAINTAINER Apache DistributedLog <distributedlog-dev@bookkeeper.apache.org>

RUN mkdir -p /opt/distributedlog/tests/conf
RUN mkdir -p /opt/distributedlog/tests/lib
# copy backward compat test jars
COPY backward-compat-test.jar /opt/distributedlog/tests/lib
COPY backward-compat-test-0.4.0.jar /opt/distributedlog/tests/lib
# copy benchmark jars
COPY jmh-benchmarks.jar /opt/distributedlog/tests/lib
COPY jmh-benchmarks-0.4.0.jar /opt/distributedlog/tests/lib
# copy the configurations
COPY conf/* /opt/distributedlog/tests/conf/


WORKDIR /opt/distributedlog/tests
