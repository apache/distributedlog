#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

for module in distributedlog-benchmark distributedlog-client distributedlog-core distributedlog-protocol distributedlog-service distributedlog-tutorials/distributedlog-basic distributedlog-tutorials/distributedlog-kafka distributedlog-tutorials/distributedlog-messaging distributedlog-tutorials/distributedlog-mapreduce; do
    echo "Repackaging module ${module}"
    if [[ -d "${module}/src/main/java/com/twitter/distributedlog" ]]; then
        mkdir -p ${module}/src/main/java/org/apache/distributedlog
        git mv ${module}/src/main/java/com/twitter/distributedlog/* ${module}/src/main/java/org/apache/distributedlog/
    fi
    if [[ -d "${module}/src/test/java/com/twitter/distributedlog" ]]; then
        mkdir -p ${module}/src/test/java/org/apache/distributedlog
        git mv ${module}/src/test/java/com/twitter/distributedlog/* ${module}/src/test/java/org/apache/distributedlog/
    fi
done

for file in `find . -type f -name '*.*'`; do
    echo ${file}
    sed -i "" 's/com\.twitter\.distributedlog/org.apache.distributedlog/g' ${file}
done

for file in `find . -type f -name 'findbugsExclude.xml'`; do
    echo ${file}
    sed -i "" 's/com\\\.twitter\\\.distributedlog/org\\\.apache\\\.distributedlog/g' ${file}
done
