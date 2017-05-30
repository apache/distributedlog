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
package org.apache.distributedlog.client.routing;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Sets;
import org.apache.distributedlog.client.resolver.RegionResolver;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import java.net.SocketAddress;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chain multiple routing services.
 */
public class RegionsRoutingService implements RoutingService {

    private static final Logger logger = LoggerFactory.getLogger(RegionsRoutingService.class);

    /**
     * Create a multiple regions routing services based on a list of region routing {@code services}.
     *
     * <p>It is deprecated. Please use {@link Builder} to build multiple regions routing service.
     *
     * @param regionResolver region resolver
     * @param services a list of region routing services.
     * @return multiple regions routing service
     * @see Builder
     */
    @Deprecated
    public static RegionsRoutingService of(RegionResolver regionResolver,
                                         RoutingService...services) {
        return new RegionsRoutingService(regionResolver, services);
    }

    /**
     * Create a builder to build a multiple-regions routing service.
     *
     * @return builder to build a multiple-regions routing service.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder to build a multiple-regions routing service.
     */
    public static class Builder implements RoutingService.Builder {

        private RegionResolver resolver;
        private RoutingService.Builder[] routingServiceBuilders;
        private StatsReceiver statsReceiver = NullStatsReceiver.get();

        private Builder() {}

        public Builder routingServiceBuilders(RoutingService.Builder...builders) {
            this.routingServiceBuilders = builders;
            return this;
        }

        public Builder resolver(RegionResolver regionResolver) {
            this.resolver = regionResolver;
            return this;
        }

        @Override
        public RoutingService.Builder statsReceiver(StatsReceiver statsReceiver) {
            this.statsReceiver = statsReceiver;
            return this;
        }

        @Override
        public RegionsRoutingService build() {
            checkNotNull(routingServiceBuilders, "No routing service builder provided.");
            checkNotNull(resolver, "No region resolver provided.");
            checkNotNull(statsReceiver, "No stats receiver provided");
            RoutingService[] services = new RoutingService[routingServiceBuilders.length];
            for (int i = 0; i < services.length; i++) {
                String statsScope;
                if (0 == i) {
                    statsScope = "local";
                } else {
                    statsScope = "remote_" + i;
                }
                services[i] = routingServiceBuilders[i]
                        .statsReceiver(statsReceiver.scope(statsScope))
                        .build();
            }
            return new RegionsRoutingService(resolver, services);
        }
    }

    protected final RegionResolver regionResolver;
    protected final RoutingService[] routingServices;

    private RegionsRoutingService(RegionResolver resolver,
                                  RoutingService[] routingServices) {
        this.regionResolver = resolver;
        this.routingServices = routingServices;
    }

    @Override
    public Set<SocketAddress> getHosts() {
        Set<SocketAddress> hosts = Sets.newHashSet();
        for (RoutingService rs : routingServices) {
            hosts.addAll(rs.getHosts());
        }
        return hosts;
    }

    @Override
    public void startService() {
        for (RoutingService service : routingServices) {
            service.startService();
        }
        logger.info("Regions Routing Service Started");
    }

    @Override
    public void stopService() {
        for (RoutingService service : routingServices) {
            service.stopService();
        }
        logger.info("Regions Routing Service Stopped");
    }

    @Override
    public RoutingService registerListener(RoutingListener listener) {
        for (RoutingService service : routingServices) {
            service.registerListener(listener);
        }
        return this;
    }

    @Override
    public RoutingService unregisterListener(RoutingListener listener) {
        for (RoutingService service : routingServices) {
            service.registerListener(listener);
        }
        return this;
    }

    @Override
    public SocketAddress getHost(String key, RoutingContext routingContext)
            throws NoBrokersAvailableException {
        for (RoutingService service : routingServices) {
            try {
                SocketAddress addr = service.getHost(key, routingContext);
                if (routingContext.hasUnavailableRegions()) {
                    // current region is unavailable
                    String region = regionResolver.resolveRegion(addr);
                    if (routingContext.isUnavailableRegion(region)) {
                        continue;
                    }
                }
                if (!routingContext.isTriedHost(addr)) {
                    return addr;
                }
            } catch (NoBrokersAvailableException nbae) {
                // if there isn't broker available in current service, try next service.
                logger.debug("No brokers available in region {} : ", service, nbae);
            }
        }
        throw new NoBrokersAvailableException("No host found for " + key + ", routing context : " + routingContext);
    }

    @Override
    public void removeHost(SocketAddress address, Throwable reason) {
        for (RoutingService service : routingServices) {
            service.removeHost(address, reason);
        }
    }
}
