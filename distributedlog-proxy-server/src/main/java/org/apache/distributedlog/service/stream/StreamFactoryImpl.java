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
package org.apache.distributedlog.service.stream;

import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.namespace.DistributedLogNamespace;
import org.apache.distributedlog.service.FatalErrorHandler;
import org.apache.distributedlog.service.config.ServerConfiguration;
import org.apache.distributedlog.service.config.StreamConfigProvider;
import org.apache.distributedlog.service.streamset.StreamPartitionConverter;
import org.apache.distributedlog.util.OrderedScheduler;
import com.twitter.util.Timer;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.jboss.netty.util.HashedWheelTimer;

/**
 * The implementation of {@link StreamFactory}.
 */
public class StreamFactoryImpl implements StreamFactory {
    private final String clientId;
    private final StreamOpStats streamOpStats;
    private final ServerConfiguration serverConfig;
    private final DistributedLogConfiguration dlConfig;
    private final FeatureProvider featureProvider;
    private final StreamConfigProvider streamConfigProvider;
    private final StreamPartitionConverter streamPartitionConverter;
    private final DistributedLogNamespace dlNamespace;
    private final OrderedScheduler scheduler;
    private final FatalErrorHandler fatalErrorHandler;
    private final HashedWheelTimer requestTimer;
    private final Timer futureTimer;

    public StreamFactoryImpl(String clientId,
        StreamOpStats streamOpStats,
        ServerConfiguration serverConfig,
        DistributedLogConfiguration dlConfig,
        FeatureProvider featureProvider,
        StreamConfigProvider streamConfigProvider,
        StreamPartitionConverter streamPartitionConverter,
        DistributedLogNamespace dlNamespace,
        OrderedScheduler scheduler,
        FatalErrorHandler fatalErrorHandler,
        HashedWheelTimer requestTimer) {

        this.clientId = clientId;
        this.streamOpStats = streamOpStats;
        this.serverConfig = serverConfig;
        this.dlConfig = dlConfig;
        this.featureProvider = featureProvider;
        this.streamConfigProvider = streamConfigProvider;
        this.streamPartitionConverter = streamPartitionConverter;
        this.dlNamespace = dlNamespace;
        this.scheduler = scheduler;
        this.fatalErrorHandler = fatalErrorHandler;
        this.requestTimer = requestTimer;
        this.futureTimer = new com.twitter.finagle.util.HashedWheelTimer(requestTimer);
    }

    @Override
    public Stream create(String name,
                         DynamicDistributedLogConfiguration streamConf,
                         StreamManager streamManager) {
        return new StreamImpl(name,
            streamPartitionConverter.convert(name),
            clientId,
            streamManager,
            streamOpStats,
            serverConfig,
            dlConfig,
            streamConf,
            featureProvider,
            streamConfigProvider,
            dlNamespace,
            scheduler,
            fatalErrorHandler,
            requestTimer,
            futureTimer);
    }
}
