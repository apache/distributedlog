/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.service.placement;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.distributedlog.client.routing.RoutingService;
import org.apache.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test Case for {@link LeastLoadPlacementPolicy}.
 */
public class TestLeastLoadPlacementPolicy {

    @Test(timeout = 10000)
    public void testCalculateBalances() throws Exception {
        int numSevers = new Random().nextInt(20) + 1;
        int numStreams = new Random().nextInt(200) + 1;
        RoutingService mockRoutingService = mock(RoutingService.class);
        DistributedLogNamespace mockNamespace = mock(DistributedLogNamespace.class);
        LeastLoadPlacementPolicy leastLoadPlacementPolicy = new LeastLoadPlacementPolicy(
            new EqualLoadAppraiser(),
            mockRoutingService,
            mockNamespace,
            null,
            Duration.fromSeconds(600),
            new NullStatsLogger());
        TreeSet<ServerLoad> serverLoads =
            Await.result(leastLoadPlacementPolicy.calculate(generateServers(numSevers), generateStreams(numStreams)));
        long lowLoadPerServer = numStreams / numSevers;
        long highLoadPerServer = lowLoadPerServer + 1;
        for (ServerLoad serverLoad : serverLoads) {
            long load = serverLoad.getLoad();
            assertEquals(load, serverLoad.getStreamLoads().size());
            assertTrue(String.format("Load %d is not between %d and %d",
                load, lowLoadPerServer, highLoadPerServer), load == lowLoadPerServer || load == highLoadPerServer);
        }
    }

    @Test(timeout = 10000)
    public void testRefreshAndPlaceStream() throws Exception {
        int numSevers = new Random().nextInt(20) + 1;
        int numStreams = new Random().nextInt(200) + 1;
        RoutingService mockRoutingService = mock(RoutingService.class);
        when(mockRoutingService.getHosts()).thenReturn(generateSocketAddresses(numSevers));
        DistributedLogNamespace mockNamespace = mock(DistributedLogNamespace.class);
        try {
            when(mockNamespace.getLogs()).thenReturn(generateStreams(numStreams).iterator());
        } catch (IOException e) {
            fail();
        }
        PlacementStateManager mockPlacementStateManager = mock(PlacementStateManager.class);
        LeastLoadPlacementPolicy leastLoadPlacementPolicy = new LeastLoadPlacementPolicy(
            new EqualLoadAppraiser(),
            mockRoutingService,
            mockNamespace,
            mockPlacementStateManager,
            Duration.fromSeconds(600),
            new NullStatsLogger());
        leastLoadPlacementPolicy.refresh();

        final ArgumentCaptor<TreeSet> captor = ArgumentCaptor.forClass(TreeSet.class);
        verify(mockPlacementStateManager).saveOwnership(captor.capture());
        TreeSet<ServerLoad> serverLoads = (TreeSet<ServerLoad>) captor.getValue();
        ServerLoad next = serverLoads.first();
        String serverPlacement = Await.result(leastLoadPlacementPolicy.placeStream("newstream1"));
        assertEquals(next.getServer(), serverPlacement);
    }

    @Test(timeout = 10000)
    public void testCalculateUnequalWeight() throws Exception {
        int numSevers = new Random().nextInt(20) + 1;
        int numStreams = new Random().nextInt(200) + 1;
    /* use AtomicInteger to have a final object in answer method */
        final AtomicInteger maxLoad = new AtomicInteger(Integer.MIN_VALUE);
        RoutingService mockRoutingService = mock(RoutingService.class);
        DistributedLogNamespace mockNamespace = mock(DistributedLogNamespace.class);
        LoadAppraiser mockLoadAppraiser = mock(LoadAppraiser.class);
        when(mockLoadAppraiser.getStreamLoad(anyString())).then(new Answer<Future<StreamLoad>>() {
            @Override
            public Future<StreamLoad> answer(InvocationOnMock invocationOnMock) throws Throwable {
                int load = new Random().nextInt(100000);
                if (load > maxLoad.get()) {
                    maxLoad.set(load);
                }
                return Future.value(new StreamLoad(invocationOnMock.getArguments()[0].toString(), load));
            }
        });
        LeastLoadPlacementPolicy leastLoadPlacementPolicy = new LeastLoadPlacementPolicy(
            mockLoadAppraiser,
            mockRoutingService,
            mockNamespace,
            null,
            Duration.fromSeconds(600),
            new NullStatsLogger());
        TreeSet<ServerLoad> serverLoads =
            Await.result(leastLoadPlacementPolicy.calculate(generateServers(numSevers), generateStreams(numStreams)));
        long highestLoadSeen = Long.MIN_VALUE;
        long lowestLoadSeen = Long.MAX_VALUE;
        for (ServerLoad serverLoad : serverLoads) {
            long load = serverLoad.getLoad();
            if (load < lowestLoadSeen) {
                lowestLoadSeen = load;
            }
            if (load > highestLoadSeen) {
                highestLoadSeen = load;
            }
        }
        assertTrue("Unexpected placement for " + numStreams + " streams to "
                + numSevers + " servers : highest load = " + highestLoadSeen
                + ", lowest load = " + lowestLoadSeen + ", max stream load = " + maxLoad.get(),
            highestLoadSeen - lowestLoadSeen < maxLoad.get());
    }

    private Set<SocketAddress> generateSocketAddresses(int num) {
        LinkedHashSet<SocketAddress> socketAddresses = new LinkedHashSet<SocketAddress>();
        for (int i = 0; i < num; i++) {
            socketAddresses.add(new InetSocketAddress(i));
        }
        return socketAddresses;
    }

    private Set<String> generateStreams(int num) {
        LinkedHashSet<String> streams = new LinkedHashSet<String>();
        for (int i = 0; i < num; i++) {
            streams.add("stream_" + i);
        }
        return streams;
    }

    private Set<String> generateServers(int num) {
        LinkedHashSet<String> servers = new LinkedHashSet<String>();
        for (int i = 0; i < num; i++) {
            servers.add("server_" + i);
        }
        return servers;
    }
}
