/*
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

package org.apache.distributedlog.namespace;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URI;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.junit.Test;

/**
 * Unit test of {@link DistributedLogNamespaceBuilder}.
 */
public class TestDistributedLogNamespaceBuilder {

    private final NamespaceBuilder underlying = mock(NamespaceBuilder.class);
    private final DistributedLogNamespaceBuilder builder = new DistributedLogNamespaceBuilder(underlying);

    @Test
    public void testConf() {
        DistributedLogConfiguration conf = mock(DistributedLogConfiguration.class);
        builder.conf(conf);
        verify(underlying, times(1)).conf(eq(conf));
    }

    @Test
    public void testDynConf() {
        DynamicDistributedLogConfiguration conf = mock(DynamicDistributedLogConfiguration.class);
        builder.dynConf(conf);
        verify(underlying, times(1)).dynConf(eq(conf));
    }

    @Test
    public void testUri() {
        URI uri = URI.create("distributedlog://127.0.0.1/messaging/distributedlog");
        builder.uri(uri);
        verify(underlying, times(1)).uri(eq(uri));
    }

    @Test
    public void testStatsLogger() {
        StatsLogger statsLogger = mock(StatsLogger.class);
        builder.statsLogger(statsLogger);
        verify(underlying, times(1)).statsLogger(eq(statsLogger));
    }

    @Test
    public void testPerLogStatsLogger() {
        StatsLogger statsLogger = mock(StatsLogger.class);
        builder.perLogStatsLogger(statsLogger);
        verify(underlying, times(1)).perLogStatsLogger(eq(statsLogger));
    }

    @Test
    public void testFeatureProvider() {
        FeatureProvider provider = mock(FeatureProvider.class);
        builder.featureProvider(provider);
        verify(underlying, times(1)).featureProvider(eq(provider));
    }

    @Test
    public void testClientId() {
        String clientId = "test-client-id";
        builder.clientId(clientId);
        verify(underlying, times(1)).clientId(eq(clientId));
    }

    @Test
    public void testRegionId() {
        int regionId = 1234;
        builder.regionId(regionId);
        verify(underlying, times(1)).regionId(eq(regionId));
    }

    @Test
    public void testBuild() throws Exception {
        builder.build();
        verify(underlying, times(1)).build();
    }

}
