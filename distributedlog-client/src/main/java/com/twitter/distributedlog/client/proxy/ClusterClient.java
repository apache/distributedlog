/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.client.proxy;

import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

/**
 * Cluster client.
 */
public class ClusterClient {

    private final Service<ThriftClientRequest, byte[]> client;
    private final DistributedLogService.ServiceIface service;

    public ClusterClient(Service<ThriftClientRequest, byte[]> client,
                         DistributedLogService.ServiceIface service) {
        this.client = client;
        this.service = service;
    }

    public Service<ThriftClientRequest, byte[]> getClient() {
        return client;
    }

    public DistributedLogService.ServiceIface getService() {
        return service;
    }

    public Future<BoxedUnit> close() {
        return client.close();
    }
}
