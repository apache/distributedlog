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

import org.apache.distributedlog.client.routing.RoutingService;
import org.apache.distributedlog.api.namespace.Namespace;
import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Futures;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

/**
 * Least Load Placement Policy.
 *
 * <p>A LoadPlacementPolicy that attempts to place streams in such a way that the load is balanced as
 * evenly as possible across all shards. The LoadAppraiser remains responsible for determining what
 * the load of a server would be. This placement policy then distributes these streams across the
 * servers.
 */
public class LeastLoadPlacementPolicy extends PlacementPolicy {

    private static final Logger logger = LoggerFactory.getLogger(LeastLoadPlacementPolicy.class);

    private TreeSet<ServerLoad> serverLoads = new TreeSet<ServerLoad>();
    private Map<String, String> streamToServer = new HashMap<String, String>();

    public LeastLoadPlacementPolicy(LoadAppraiser loadAppraiser, RoutingService routingService,
                                    Namespace namespace, PlacementStateManager placementStateManager,
                                    Duration refreshInterval, StatsLogger statsLogger) {
        super(loadAppraiser, routingService, namespace, placementStateManager, refreshInterval, statsLogger);
        statsLogger.registerGauge("placement/load.diff", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                if (serverLoads.size() > 0) {
                    return serverLoads.last().getLoad() - serverLoads.first().getLoad();
                } else {
                    return getDefaultValue();
                }
            }
        });
    }

    private synchronized String getStreamOwner(String stream) {
        return streamToServer.get(stream);
    }

    @Override
    public Future<String> placeStream(String stream) {
        String streamOwner = getStreamOwner(stream);
        if (null != streamOwner) {
            return Future.value(streamOwner);
        }
        Future<StreamLoad> streamLoadFuture = loadAppraiser.getStreamLoad(stream);
        return streamLoadFuture.map(new Function<StreamLoad, String>() {
            @Override
            public String apply(StreamLoad streamLoad) {
                return placeStreamSynchronized(streamLoad);
            }
        });
    }

    private synchronized String placeStreamSynchronized(StreamLoad streamLoad) {
        ServerLoad serverLoad = serverLoads.pollFirst();
        serverLoad.addStream(streamLoad);
        serverLoads.add(serverLoad);
        return serverLoad.getServer();
    }

    @Override
    public void refresh() {
        logger.info("Refreshing server loads.");
        Future<Void> refresh = loadAppraiser.refreshCache();
        final Set<String> servers = getServers();
        final Set<String> allStreams = getStreams();
        Future<TreeSet<ServerLoad>> serverLoadsFuture = refresh.flatMap(
            new Function<Void, Future<TreeSet<ServerLoad>>>() {
            @Override
            public Future<TreeSet<ServerLoad>> apply(Void v1) {
                return calculate(servers, allStreams);
            }
        });
        serverLoadsFuture.map(new Function<TreeSet<ServerLoad>, BoxedUnit>() {
            @Override
            public BoxedUnit apply(TreeSet<ServerLoad> serverLoads) {
                try {
                    updateServerLoads(serverLoads);
                } catch (PlacementStateManager.StateManagerSaveException e) {
                    logger.error("The refreshed mapping could not be persisted and will not be used.", e);
                }
                return BoxedUnit.UNIT;
            }
        });
    }

    private synchronized void updateServerLoads(TreeSet<ServerLoad> serverLoads)
        throws PlacementStateManager.StateManagerSaveException {
        this.placementStateManager.saveOwnership(serverLoads);
        this.streamToServer = serverLoadsToMap(serverLoads);
        this.serverLoads = serverLoads;
    }

    @Override
    public synchronized void load(TreeSet<ServerLoad> serverLoads) {
        this.serverLoads = serverLoads;
        this.streamToServer = serverLoadsToMap(serverLoads);
    }

    public Future<TreeSet<ServerLoad>> calculate(final Set<String> servers, Set<String> streams) {
        logger.info("Calculating server loads");
        final long startTime = System.currentTimeMillis();
        ArrayList<Future<StreamLoad>> futures = new ArrayList<Future<StreamLoad>>(streams.size());

        for (String stream : streams) {
            Future<StreamLoad> streamLoad = loadAppraiser.getStreamLoad(stream);
            futures.add(streamLoad);
        }

        return Futures.collect(futures).map(new Function<List<StreamLoad>, TreeSet<ServerLoad>>() {
            @Override
            public TreeSet<ServerLoad> apply(List<StreamLoad> streamLoads) {
        /* Sort streamLoads so largest streams are placed first for better balance */
                TreeSet<StreamLoad> streamQueue = new TreeSet<StreamLoad>();
                for (StreamLoad streamLoad : streamLoads) {
                    streamQueue.add(streamLoad);
                }

                TreeSet<ServerLoad> serverLoads = new TreeSet<ServerLoad>();
                for (String server : servers) {
                    ServerLoad serverLoad = new ServerLoad(server);
                    if (!streamQueue.isEmpty()) {
                        serverLoad.addStream(streamQueue.pollFirst());
                    }
                    serverLoads.add(serverLoad);
                }

                while (!streamQueue.isEmpty()) {
                    ServerLoad serverLoad = serverLoads.pollFirst();
                    serverLoad.addStream(streamQueue.pollFirst());
                    serverLoads.add(serverLoad);
                }
                return serverLoads;
            }
        }).onSuccess(new Function<TreeSet<ServerLoad>, BoxedUnit>() {
            @Override
            public BoxedUnit apply(TreeSet<ServerLoad> serverLoads) {
                placementCalcStats.registerSuccessfulEvent(System.currentTimeMillis() - startTime);
                return BoxedUnit.UNIT;
            }
        }).onFailure(new Function<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable t) {
                logger.error("Failure calculating loads", t);
                placementCalcStats.registerFailedEvent(System.currentTimeMillis() - startTime);
                return BoxedUnit.UNIT;
            }
        });
    }

    private static Map<String, String> serverLoadsToMap(Collection<ServerLoad> serverLoads) {
        HashMap<String, String> streamToServer = new HashMap<String, String>(serverLoads.size());
        for (ServerLoad serverLoad : serverLoads) {
            for (StreamLoad streamLoad : serverLoad.getStreamLoads()) {
                streamToServer.put(streamLoad.getStream(), serverLoad.getServer());
            }
        }
        return streamToServer;
    }
}
