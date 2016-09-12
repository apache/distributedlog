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
package com.twitter.distributedlog.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.client.ClientConfig;
import com.twitter.distributedlog.client.DistributedLogClientImpl;
import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import com.twitter.distributedlog.client.proxy.ProxyClient;
import com.twitter.distributedlog.client.resolver.RegionResolver;
import com.twitter.distributedlog.client.resolver.DefaultRegionResolver;
import com.twitter.distributedlog.client.finagle.routing.RoutingService;
import com.twitter.distributedlog.client.finagle.routing.RoutingUtils;
import com.twitter.distributedlog.client.stats.ClientStats;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;

import java.net.SocketAddress;

public abstract class DistributedLogClientBuilder<T extends DistributedLogClientBuilder> {

    protected String _name = null;

    protected RoutingService.Builder _routingServiceBuilder = null;

    protected StatsReceiver _statsReceiver = new NullStatsReceiver();
    protected StatsReceiver _streamStatsReceiver = new NullStatsReceiver();
    protected ClientConfig _clientConfig = new ClientConfig();
    protected boolean _enableRegionStats = false;
    protected final RegionResolver _regionResolver = new DefaultRegionResolver();

    // protected constructor
    protected DistributedLogClientBuilder() {}

    public abstract T getBuilder();

    protected abstract T createBuilder(DistributedLogClientBuilder<T> builder);

    protected void copyBuilder(DistributedLogClientBuilder<T> oldBuilder,
                               DistributedLogClientBuilder<T> newBuilder) {
        newBuilder._name = oldBuilder._name;
        newBuilder._routingServiceBuilder = oldBuilder._routingServiceBuilder;
        newBuilder._statsReceiver = oldBuilder._statsReceiver;
        newBuilder._streamStatsReceiver = oldBuilder._streamStatsReceiver;
        newBuilder._enableRegionStats = oldBuilder._enableRegionStats;
        newBuilder._clientConfig = ClientConfig.newConfig(oldBuilder._clientConfig);
    }

    /**
     * Client Name.
     *
     * @param name
     *          client name
     * @return client builder.
     */
    public T name(String name) {
        T newBuilder = createBuilder(this);
        newBuilder._name = name;
        return newBuilder;
    }

    /**
     * Address of write proxy to connect.
     *
     * @param address
     *          write proxy address.
     * @return client builder.
     */
    public T host(SocketAddress address) {
        T newBuilder = createBuilder(this);
        newBuilder._routingServiceBuilder = RoutingUtils.buildRoutingService(address);
        newBuilder._enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Configure the routing service builder used by this client.
     *
     * @param builder routing service builder
     * @param enableRegionStats whether to enable region stats or not
     * @return client builder
     */
    public T routingServiceBuilder(RoutingService.Builder builder,
                                                                boolean enableRegionStats) {
        T newBuilder = createBuilder(this);
        newBuilder._routingServiceBuilder = builder;
        newBuilder._enableRegionStats = enableRegionStats;
        return newBuilder;
    }

    /**
     * Routing Service to access proxy services.
     *
     * @param routingService
     *          routing service
     * @return client builder.
     */
    @VisibleForTesting
    public T routingService(RoutingService routingService) {
        T newBuilder = createBuilder(this);
        newBuilder._routingServiceBuilder = RoutingUtils.buildRoutingService(routingService);
        newBuilder._enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Stats receiver to expose client stats.
     *
     * @param statsReceiver
     *          stats receiver.
     * @return client builder.
     */
    public T statsReceiver(StatsReceiver statsReceiver) {
        T newBuilder = createBuilder(this);
        newBuilder._statsReceiver = statsReceiver;
        return newBuilder;
    }

    /**
     * Stream Stats Receiver to expose per stream stats.
     *
     * @param streamStatsReceiver
     *          stream stats receiver
     * @return client builder.
     */
    public T streamStatsReceiver(StatsReceiver streamStatsReceiver) {
        T newBuilder = createBuilder(this);
        newBuilder._streamStatsReceiver = streamStatsReceiver;
        return newBuilder;
    }

    /**
     * Backoff time when redirecting to an already retried host.
     *
     * @param ms
     *          backoff time.
     * @return client builder.
     */
    public T redirectBackoffStartMs(int ms) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setRedirectBackoffStartMs(ms);
        return newBuilder;
    }

    /**
     * Max backoff time when redirecting to an already retried host.
     *
     * @param ms
     *          backoff time.
     * @return client builder.
     */
    public T redirectBackoffMaxMs(int ms) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setRedirectBackoffMaxMs(ms);
        return newBuilder;
    }

    /**
     * Max redirects that is allowed per request. If <i>redirects</i> are
     * exhausted, fail the request immediately.
     *
     * @param redirects
     *          max redirects allowed before failing a request.
     * @return client builder.
     */
    public T maxRedirects(int redirects) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setMaxRedirects(redirects);
        return newBuilder;
    }

    /**
     * Timeout per request in millis.
     *
     * @param timeoutMs
     *          timeout per request in millis.
     * @return client builder.
     */
    public T requestTimeoutMs(int timeoutMs) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setRequestTimeoutMs(timeoutMs);
        return newBuilder;
    }

    /**
     * Set thriftmux enabled.
     *
     * @param enabled
     *          is thriftmux enabled
     * @return client builder.
     */
    public T thriftmux(boolean enabled) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setThriftMux(enabled);
        return newBuilder;
    }

    /**
     * Set failfast stream exception handling enabled.
     *
     * @param enabled
     *          is failfast exception handling enabled
     * @return client builder.
     */
    public T streamFailfast(boolean enabled) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setStreamFailfast(enabled);
        return newBuilder;
    }

    /**
     * Set the regex to match stream names that the client cares about.
     *
     * @param nameRegex
     *          stream name regex
     * @return client builder
     */
    public T streamNameRegex(String nameRegex) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setStreamNameRegex(nameRegex);
        return newBuilder;
    }

    /**
     * Whether to use the new handshake endpoint to exchange ownership cache. Enable this
     * when the servers are updated to support handshaking with client info.
     *
     * @param enabled
     *          new handshake endpoint is enabled.
     * @return client builder.
     */
    @Deprecated
    public T handshakeWithClientInfo(boolean enabled) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setHandshakeWithClientInfo(enabled);
        return newBuilder;
    }

    /**
     * Set the periodic handshake interval in milliseconds. Every <code>intervalMs</code>,
     * the DL client will handshake with existing proxies again. If the interval is less than
     * ownership sync interval, the handshake won't sync ownerships. Otherwise, it will.
     *
     * @see #periodicOwnershipSyncIntervalMs(long)
     * @param intervalMs
     *          handshake interval
     * @return client builder.
     */
    public T periodicHandshakeIntervalMs(long intervalMs) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setPeriodicHandshakeIntervalMs(intervalMs);
        return newBuilder;
    }

    /**
     * Set the periodic ownership sync interval in milliseconds. If periodic handshake is enabled,
     * the handshake will sync ownership if the elapsed time is larger than sync interval.
     *
     * @see #periodicHandshakeIntervalMs(long)
     * @param intervalMs
     *          interval that handshake should sync ownerships.
     * @return client builder
     */
    public T periodicOwnershipSyncIntervalMs(long intervalMs) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setPeriodicOwnershipSyncIntervalMs(intervalMs);
        return newBuilder;
    }

    /**
     * Enable/Disable periodic dumping ownership cache.
     *
     * @param enabled
     *          flag to enable/disable periodic dumping ownership cache
     * @return client builder.
     */
    public T periodicDumpOwnershipCache(boolean enabled) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setPeriodicDumpOwnershipCacheEnabled(enabled);
        return newBuilder;
    }

    /**
     * Set periodic dumping ownership cache interval.
     *
     * @param intervalMs
     *          interval on dumping ownership cache, in millis.
     * @return client builder
     */
    public T periodicDumpOwnershipCacheIntervalMs(long intervalMs) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setPeriodicDumpOwnershipCacheIntervalMs(intervalMs);
        return newBuilder;
    }

    /**
     * Enable handshake tracing.
     *
     * @param enabled
     *          flag to enable/disable handshake tracing
     * @return client builder
     */
    public T handshakeTracing(boolean enabled) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setHandshakeTracingEnabled(enabled);
        return newBuilder;
    }

    /**
     * Enable checksum on requests to the proxy.
     *
     * @param enabled
     *          flag to enable/disable checksum
     * @return client builder
     */
    public T checksum(boolean enabled) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig.setChecksumEnabled(enabled);
        return newBuilder;
    }

    T clientConfig(ClientConfig clientConfig) {
        T newBuilder = createBuilder(this);
        newBuilder._clientConfig = ClientConfig.newConfig(clientConfig);
        return newBuilder;
    }

    /**
     * Build distributedlog client.
     *
     * @return distributedlog client.
     */
    public DistributedLogClient build() {
        return buildClient();
    }

    /**
     * Build monitor service client.
     *
     * @return monitor service client.
     */
    public MonitorServiceClient buildMonitorClient() {
        return buildClient();
    }

    protected void beforeBuildClient() {
        Preconditions.checkNotNull(_name, "No name provided.");
        Preconditions.checkNotNull(_routingServiceBuilder, "No routing service builder provided.");
        Preconditions.checkNotNull(_statsReceiver, "No stats receiver provided.");
    }

    protected abstract ProxyClient.Builder newProxyClientBuilder(ClientConfig clientConfig,
                                                                 ClientStats clientStats);

    DistributedLogClientImpl buildClient() {
        beforeBuildClient();
        // create the stream stats receiver
        if (null == _streamStatsReceiver) {
            _streamStatsReceiver = new NullStatsReceiver();
        }
        // build the routing service
        RoutingService routingService = _routingServiceBuilder
                .statsReceiver(_statsReceiver.scope("routing"))
                .build();
        // build the client stats
        ClientStats clientStats = new ClientStats(
                _statsReceiver,
                _enableRegionStats,
                _regionResolver);
        DistributedLogClientImpl clientImpl =
                new DistributedLogClientImpl(
                        _name,
                        routingService,
                        _clientConfig,
                        newProxyClientBuilder(_clientConfig, clientStats),
                        _statsReceiver,
                        _streamStatsReceiver,
                        clientStats,
                        _regionResolver,
                        _enableRegionStats);
        routingService.startService();
        clientImpl.handshake();
        return clientImpl;
    }

}
