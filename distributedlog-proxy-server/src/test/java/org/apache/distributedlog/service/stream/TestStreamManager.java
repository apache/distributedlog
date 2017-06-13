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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.service.config.StreamConfigProvider;
import org.apache.distributedlog.service.streamset.Partition;
import org.apache.distributedlog.service.streamset.StreamPartitionConverter;
import com.twitter.util.Await;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test Case for StreamManager.
 */
public class TestStreamManager {

    @Rule
    public TestName testName = new TestName();

    ScheduledExecutorService mockExecutorService = mock(ScheduledExecutorService.class);

    @Test(timeout = 60000)
    public void testCollectionMethods() throws Exception {
        Stream mockStream = mock(Stream.class);
        when(mockStream.getStreamName()).thenReturn("stream1");
        when(mockStream.getPartition()).thenReturn(new Partition("stream1", 0));
        StreamFactory mockStreamFactory = mock(StreamFactory.class);
        StreamPartitionConverter mockPartitionConverter = mock(StreamPartitionConverter.class);
        StreamConfigProvider mockStreamConfigProvider = mock(StreamConfigProvider.class);
        when(mockStreamFactory.create(
                (String) any(),
                (DynamicDistributedLogConfiguration) any(),
                (StreamManager) any())).thenReturn(mockStream);
        StreamManager streamManager = new StreamManagerImpl(
                "",
                new DistributedLogConfiguration(),
                mockExecutorService,
                mockStreamFactory,
                mockPartitionConverter,
                mockStreamConfigProvider,
                mock(Namespace.class));

        assertFalse(streamManager.isAcquired("stream1"));
        assertEquals(0, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyAcquired(mockStream);
        assertTrue(streamManager.isAcquired("stream1"));
        assertEquals(1, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyReleased(mockStream);
        assertFalse(streamManager.isAcquired("stream1"));
        assertEquals(0, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyAcquired(mockStream);
        assertTrue(streamManager.isAcquired("stream1"));
        assertEquals(1, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyAcquired(mockStream);
        assertTrue(streamManager.isAcquired("stream1"));
        assertEquals(1, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyReleased(mockStream);
        assertFalse(streamManager.isAcquired("stream1"));
        assertEquals(0, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyReleased(mockStream);
        assertFalse(streamManager.isAcquired("stream1"));
        assertEquals(0, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());
    }

    @Test(timeout = 60000)
    public void testCreateStream() throws Exception {
        Stream mockStream = mock(Stream.class);
        final String streamName = "stream1";
        when(mockStream.getStreamName()).thenReturn(streamName);
        StreamFactory mockStreamFactory = mock(StreamFactory.class);
        StreamPartitionConverter mockPartitionConverter = mock(StreamPartitionConverter.class);
        StreamConfigProvider mockStreamConfigProvider = mock(StreamConfigProvider.class);
        when(mockStreamFactory.create(
            (String) any(),
            (DynamicDistributedLogConfiguration) any(),
            (StreamManager) any())
        ).thenReturn(mockStream);
        Namespace dlNamespace = mock(Namespace.class);
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

        StreamManager streamManager = new StreamManagerImpl(
                "",
                new DistributedLogConfiguration(),
                executorService,
                mockStreamFactory,
                mockPartitionConverter,
                mockStreamConfigProvider,
                dlNamespace);

        assertTrue(Await.ready(streamManager.createStreamAsync(streamName)).isReturn());
        verify(dlNamespace).createLog(streamName);
    }
}
