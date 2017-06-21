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

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.client.routing.RoutingService;
import org.apache.distributedlog.service.DLSocketAddress;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.ScheduledThreadPoolTimer;
import com.twitter.util.Time;
import com.twitter.util.Timer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

/**
 * A PlacementPolicy assigns streams to servers given an appraisal of the load that the stream contains.
 *
 * <p>The load of a stream is determined by the LoadAppraiser used. The PlacementPolicy will
 * then distributed these StreamLoads to the available servers in a manner defined by the
 * implementation creating ServerLoad objects. It then saves this assignment via the
 * PlacementStateManager.
 */
public abstract class PlacementPolicy {

    private static final Logger logger = LoggerFactory.getLogger(PlacementPolicy.class);

    protected final LoadAppraiser loadAppraiser;
    protected final RoutingService routingService;
    protected final Namespace namespace;
    protected final PlacementStateManager placementStateManager;
    private final Duration refreshInterval;
    protected final OpStatsLogger placementCalcStats;
    private Timer placementRefreshTimer;

    public PlacementPolicy(LoadAppraiser loadAppraiser, RoutingService routingService,
                           Namespace namespace, PlacementStateManager placementStateManager,
                           Duration refreshInterval, StatsLogger statsLogger) {
        this.loadAppraiser = loadAppraiser;
        this.routingService = routingService;
        this.namespace = namespace;
        this.placementStateManager = placementStateManager;
        this.refreshInterval = refreshInterval;
        placementCalcStats = statsLogger.getOpStatsLogger("placement");
    }

    public Set<String> getServers() {
        Set<SocketAddress> hosts = routingService.getHosts();
        Set<String> servers = new HashSet<String>(hosts.size());
        for (SocketAddress address : hosts) {
            servers.add(DLSocketAddress.toString((InetSocketAddress) address));
        }
        return servers;
    }

    public Set<String> getStreams() {
        Set<String> streams = new HashSet<String>();
        try {
            Iterator<String> logs = namespace.getLogs();
            while (logs.hasNext()) {
                streams.add(logs.next());
            }
        } catch (IOException e) {
            logger.error("Could not get streams for placement policy.", e);
        }
        return streams;
    }

    public void start(boolean leader) {
        logger.info("Starting placement policy");

        TreeSet<ServerLoad> emptyServerLoads = new TreeSet<ServerLoad>();
        for (String server : getServers()) {
            emptyServerLoads.add(new ServerLoad(server));
        }
        load(emptyServerLoads); //Pre-Load so streams don't NPE
        if (leader) { //this is the leader shard
            logger.info("Shard is leader. Scheduling timed refresh.");
            placementRefreshTimer = new ScheduledThreadPoolTimer(1, "timer", true);
            placementRefreshTimer.schedule(Time.now(), refreshInterval, new Function0<BoxedUnit>() {
                @Override
                public BoxedUnit apply() {
                    refresh();
                    return BoxedUnit.UNIT;
                }
            });
        } else {
            logger.info("Shard is not leader. Watching for server load changes.");
            placementStateManager.watch(new PlacementStateManager.PlacementCallback() {
                @Override
                public void callback(TreeSet<ServerLoad> serverLoads) {
                    if (!serverLoads.isEmpty()) {
                        load(serverLoads);
                    }
                }
            });
        }
    }

    public void close() {
        if (placementRefreshTimer != null) {
            placementRefreshTimer.stop();
        }
    }

    /**
     * Places the stream on a server according to the policy.
     *
     * <p>It returns a future containing the host that owns the stream upon completion
     */
    public abstract Future<String> placeStream(String stream);

    /**
     * Recalculates the entire placement mapping and updates stores it using the PlacementStateManager.
     */
    public abstract void refresh();

    /**
     * Loads the placement mapping into the node from a TreeSet of ServerLoads.
     */
    public abstract void load(TreeSet<ServerLoad> serverLoads);
}
