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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.client.ClientConfig;
import com.twitter.distributedlog.client.DistributedLogClientImpl;
import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import com.twitter.distributedlog.client.proxy.ClusterClient;
import com.twitter.distributedlog.client.resolver.DefaultRegionResolver;
import com.twitter.distributedlog.client.resolver.RegionResolver;
import com.twitter.distributedlog.client.routing.RegionsRoutingService;
import com.twitter.distributedlog.client.routing.RoutingService;
import com.twitter.distributedlog.client.routing.RoutingUtils;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.finagle.Name;
import com.twitter.finagle.Resolver$;
import com.twitter.finagle.Service;
import com.twitter.finagle.ThriftMux;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Random;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * Builder to build {@link DistributedLogClient}.
 */
public final class DistributedLogClientBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLogClientBuilder.class);

    private static final Random random = new Random(System.currentTimeMillis());

    private String name = null;
    private ClientId clientId = null;
    private RoutingService.Builder routingServiceBuilder = null;
    private ClientBuilder clientBuilder = null;
    private String serverRoutingServiceFinagleName = null;
    private StatsReceiver statsReceiver = new NullStatsReceiver();
    private StatsReceiver streamStatsReceiver = new NullStatsReceiver();
    private ClientConfig clientConfig = new ClientConfig();
    private boolean enableRegionStats = false;
    private final RegionResolver regionResolver = new DefaultRegionResolver();

    /**
     * Create a client builder.
     *
     * @return client builder
     */
    public static DistributedLogClientBuilder newBuilder() {
        return new DistributedLogClientBuilder();
    }

    /**
     * Create a new client builder from an existing {@code builder}.
     *
     * @param builder the existing builder.
     * @return a new client builder.
     */
    public static DistributedLogClientBuilder newBuilder(DistributedLogClientBuilder builder) {
        DistributedLogClientBuilder newBuilder = new DistributedLogClientBuilder();
        newBuilder.name = builder.name;
        newBuilder.clientId = builder.clientId;
        newBuilder.clientBuilder = builder.clientBuilder;
        newBuilder.routingServiceBuilder = builder.routingServiceBuilder;
        newBuilder.statsReceiver = builder.statsReceiver;
        newBuilder.streamStatsReceiver = builder.streamStatsReceiver;
        newBuilder.enableRegionStats = builder.enableRegionStats;
        newBuilder.serverRoutingServiceFinagleName = builder.serverRoutingServiceFinagleName;
        newBuilder.clientConfig = ClientConfig.newConfig(builder.clientConfig);
        return newBuilder;
    }

    // private constructor
    private DistributedLogClientBuilder() {}

    /**
     * Client Name.
     *
     * @param name
     *          client name
     * @return client builder.
     */
    public DistributedLogClientBuilder name(String name) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.name = name;
        return newBuilder;
    }

    /**
     * Client ID.
     *
     * @param clientId
     *          client id
     * @return client builder.
     */
    public DistributedLogClientBuilder clientId(ClientId clientId) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientId = clientId;
        return newBuilder;
    }

    /**
     * Serverset to access proxy services.
     *
     * @param serverSet
     *          server set.
     * @return client builder.
     */
    public DistributedLogClientBuilder serverSet(ServerSet serverSet) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.routingServiceBuilder = RoutingUtils.buildRoutingService(serverSet);
        newBuilder.enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Server Sets to access proxy services.
     *
     * <p>The <i>local</i> server set will be tried first then <i>remotes</i>.
     *
     * @param local local server set.
     * @param remotes remote server sets.
     * @return client builder.
     */
    public DistributedLogClientBuilder serverSets(ServerSet local, ServerSet...remotes) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        RoutingService.Builder[] builders = new RoutingService.Builder[remotes.length + 1];
        builders[0] = RoutingUtils.buildRoutingService(local);
        for (int i = 1; i < builders.length; i++) {
            builders[i] = RoutingUtils.buildRoutingService(remotes[i - 1]);
        }
        newBuilder.routingServiceBuilder = RegionsRoutingService.newBuilder()
                .resolver(regionResolver)
                .routingServiceBuilders(builders);
        newBuilder.enableRegionStats = remotes.length > 0;
        return newBuilder;
    }

    /**
     * Name to access proxy services.
     *
     * @param finagleNameStr
     *          finagle name string.
     * @return client builder.
     */
    public DistributedLogClientBuilder finagleNameStr(String finagleNameStr) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.routingServiceBuilder = RoutingUtils.buildRoutingService(finagleNameStr);
        newBuilder.enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Finagle name strs to access proxy services.
     *
     * <p>The <i>local</i> finalge name str will be tried first, then <i>remotes</i>.
     *
     * @param local local server set.
     * @param remotes remote server sets.
     * @return client builder.
     */
    public DistributedLogClientBuilder finagleNameStrs(String local, String...remotes) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        RoutingService.Builder[] builders = new RoutingService.Builder[remotes.length + 1];
        builders[0] = RoutingUtils.buildRoutingService(local);
        for (int i = 1; i < builders.length; i++) {
            builders[i] = RoutingUtils.buildRoutingService(remotes[i - 1]);
        }
        newBuilder.routingServiceBuilder = RegionsRoutingService.newBuilder()
                .routingServiceBuilders(builders)
                .resolver(regionResolver);
        newBuilder.enableRegionStats = remotes.length > 0;
        return newBuilder;
    }

    /**
     * URI to access proxy services.
     *
     * <p>Assuming the write proxies are announced under `.write_proxy` of the provided namespace uri.
     * The builder will convert the dl uri (e.g. distributedlog://{zkserver}/path/to/namespace) to
     * zookeeper serverset based finagle name str (`zk!{zkserver}!/path/to/namespace/.write_proxy`)
     *
     * @param uri namespace uri to access the serverset of write proxies
     * @return distributedlog builder
     */
    public DistributedLogClientBuilder uri(URI uri) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        String zkServers = uri.getAuthority().replace(";", ",");
        String[] zkServerList = StringUtils.split(zkServers, ',');
        String finagleNameStr = String.format(
                "zk!%s!%s/.write_proxy",
                zkServerList[random.nextInt(zkServerList.length)], // zk server
                uri.getPath());
        newBuilder.routingServiceBuilder = RoutingUtils.buildRoutingService(finagleNameStr);
        newBuilder.enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Address of write proxy to connect.
     *
     * @param address
     *          write proxy address.
     * @return client builder.
     */
    public DistributedLogClientBuilder host(SocketAddress address) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.routingServiceBuilder = RoutingUtils.buildRoutingService(address);
        newBuilder.enableRegionStats = false;
        return newBuilder;
    }

    private DistributedLogClientBuilder routingServiceBuilder(RoutingService.Builder builder) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.routingServiceBuilder = builder;
        newBuilder.enableRegionStats = false;
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
    public DistributedLogClientBuilder routingService(RoutingService routingService) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.routingServiceBuilder = RoutingUtils.buildRoutingService(routingService);
        newBuilder.enableRegionStats = false;
        return newBuilder;
    }

    /**
     * Stats receiver to expose client stats.
     *
     * @param statsReceiver
     *          stats receiver.
     * @return client builder.
     */
    public DistributedLogClientBuilder statsReceiver(StatsReceiver statsReceiver) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.statsReceiver = statsReceiver;
        return newBuilder;
    }

    /**
     * Stream Stats Receiver to expose per stream stats.
     *
     * @param streamStatsReceiver
     *          stream stats receiver
     * @return client builder.
     */
    public DistributedLogClientBuilder streamStatsReceiver(StatsReceiver streamStatsReceiver) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.streamStatsReceiver = streamStatsReceiver;
        return newBuilder;
    }

    /**
     * Set underlying finagle client builder.
     *
     * @param builder
     *          finagle client builder.
     * @return client builder.
     */
    public DistributedLogClientBuilder clientBuilder(ClientBuilder builder) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientBuilder = builder;
        return newBuilder;
    }

    /**
     * Backoff time when redirecting to an already retried host.
     *
     * @param ms
     *          backoff time.
     * @return client builder.
     */
    public DistributedLogClientBuilder redirectBackoffStartMs(int ms) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setRedirectBackoffStartMs(ms);
        return newBuilder;
    }

    /**
     * Max backoff time when redirecting to an already retried host.
     *
     * @param ms
     *          backoff time.
     * @return client builder.
     */
    public DistributedLogClientBuilder redirectBackoffMaxMs(int ms) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setRedirectBackoffMaxMs(ms);
        return newBuilder;
    }

    /**
     * Max redirects that is allowed per request.
     *
     * <p>If <i>redirects</i> are exhausted, fail the request immediately.
     *
     * @param redirects
     *          max redirects allowed before failing a request.
     * @return client builder.
     */
    public DistributedLogClientBuilder maxRedirects(int redirects) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setMaxRedirects(redirects);
        return newBuilder;
    }

    /**
     * Timeout per request in millis.
     *
     * @param timeoutMs
     *          timeout per request in millis.
     * @return client builder.
     */
    public DistributedLogClientBuilder requestTimeoutMs(int timeoutMs) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setRequestTimeoutMs(timeoutMs);
        return newBuilder;
    }

    /**
     * Set thriftmux enabled.
     *
     * @param enabled
     *          is thriftmux enabled
     * @return client builder.
     */
    public DistributedLogClientBuilder thriftmux(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setThriftMux(enabled);
        return newBuilder;
    }

    /**
     * Set failfast stream exception handling enabled.
     *
     * @param enabled
     *          is failfast exception handling enabled
     * @return client builder.
     */
    public DistributedLogClientBuilder streamFailfast(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setStreamFailfast(enabled);
        return newBuilder;
    }

    /**
     * Set the regex to match stream names that the client cares about.
     *
     * @param nameRegex
     *          stream name regex
     * @return client builder
     */
    public DistributedLogClientBuilder streamNameRegex(String nameRegex) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setStreamNameRegex(nameRegex);
        return newBuilder;
    }

    /**
     * Whether to use the new handshake endpoint to exchange ownership cache.
     *
     * <p>Enable this when the servers are updated to support handshaking with client info.
     *
     * @param enabled
     *          new handshake endpoint is enabled.
     * @return client builder.
     */
    public DistributedLogClientBuilder handshakeWithClientInfo(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setHandshakeWithClientInfo(enabled);
        return newBuilder;
    }

    /**
     * Set the periodic handshake interval in milliseconds.
     *
     * <p>Every <code>intervalMs</code>, the DL client will handshake with existing proxies again.
     * If the interval is less than ownership sync interval, the handshake won't sync ownerships. Otherwise, it will.
     *
     * @see #periodicOwnershipSyncIntervalMs(long)
     * @param intervalMs
     *          handshake interval
     * @return client builder.
     */
    public DistributedLogClientBuilder periodicHandshakeIntervalMs(long intervalMs) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setPeriodicHandshakeIntervalMs(intervalMs);
        return newBuilder;
    }

    /**
     * Set the periodic ownership sync interval in milliseconds.
     *
     * <p>If periodic handshake is enabled, the handshake will sync ownership if the elapsed time is larger than
     * sync interval.
     *
     * @see #periodicHandshakeIntervalMs(long)
     * @param intervalMs
     *          interval that handshake should sync ownerships.
     * @return client builder
     */
    public DistributedLogClientBuilder periodicOwnershipSyncIntervalMs(long intervalMs) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setPeriodicOwnershipSyncIntervalMs(intervalMs);
        return newBuilder;
    }

    /**
     * Enable/Disable periodic dumping ownership cache.
     *
     * @param enabled
     *          flag to enable/disable periodic dumping ownership cache
     * @return client builder.
     */
    public DistributedLogClientBuilder periodicDumpOwnershipCache(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setPeriodicDumpOwnershipCacheEnabled(enabled);
        return newBuilder;
    }

    /**
     * Set periodic dumping ownership cache interval.
     *
     * @param intervalMs
     *          interval on dumping ownership cache, in millis.
     * @return client builder
     */
    public DistributedLogClientBuilder periodicDumpOwnershipCacheIntervalMs(long intervalMs) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setPeriodicDumpOwnershipCacheIntervalMs(intervalMs);
        return newBuilder;
    }

    /**
     * Enable handshake tracing.
     *
     * @param enabled
     *          flag to enable/disable handshake tracing
     * @return client builder
     */
    public DistributedLogClientBuilder handshakeTracing(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setHandshakeTracingEnabled(enabled);
        return newBuilder;
    }

    /**
     * Enable checksum on requests to the proxy.
     *
     * @param enabled
     *          flag to enable/disable checksum
     * @return client builder
     */
    public DistributedLogClientBuilder checksum(boolean enabled) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig.setChecksumEnabled(enabled);
        return newBuilder;
    }

    /**
     * Configure the finagle name string for the server-side routing service.
     *
     * @param nameStr name string of the server-side routing service
     * @return client builder
     */
    public DistributedLogClientBuilder serverRoutingServiceFinagleNameStr(String nameStr) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.serverRoutingServiceFinagleName = nameStr;
        return newBuilder;
    }

    DistributedLogClientBuilder clientConfig(ClientConfig clientConfig) {
        DistributedLogClientBuilder newBuilder = newBuilder(this);
        newBuilder.clientConfig = ClientConfig.newConfig(clientConfig);
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

    @SuppressWarnings("unchecked")
    ClusterClient buildServerRoutingServiceClient(String serverRoutingServiceFinagleName) {
        ClientBuilder builder = this.clientBuilder;
        if (null == builder) {
            builder = ClientBuilder.get()
                    .tcpConnectTimeout(Duration.fromMilliseconds(200))
                    .connectTimeout(Duration.fromMilliseconds(200))
                    .requestTimeout(Duration.fromSeconds(1))
                    .retries(20);
            if (!clientConfig.getThriftMux()) {
                builder = builder.hostConnectionLimit(1);
            }
        }
        if (clientConfig.getThriftMux()) {
            builder = builder.stack(ThriftMux.client().withClientId(clientId));
        } else {
            builder = builder.codec(ThriftClientFramedCodec.apply(Option.apply(clientId)));
        }

        Name name;
        try {
            name = Resolver$.MODULE$.eval(serverRoutingServiceFinagleName);
        } catch (Exception exc) {
            logger.error("Exception in Resolver.eval for name {}", serverRoutingServiceFinagleName, exc);
            throw new RuntimeException(exc);
        }

        // builder the client
        Service<ThriftClientRequest, byte[]> client =
                ClientBuilder.safeBuildFactory(
                        builder.dest(name).reportTo(statsReceiver.scope("routing"))
                ).toService();
        DistributedLogService.ServiceIface service =
                new DistributedLogService.ServiceToClient(client, new TBinaryProtocol.Factory());
        return new ClusterClient(client, service);
    }

    DistributedLogClientImpl buildClient() {
        checkNotNull(name, "No name provided.");
        checkNotNull(clientId, "No client id provided.");
        checkNotNull(routingServiceBuilder, "No routing service builder provided.");
        checkNotNull(statsReceiver, "No stats receiver provided.");
        if (null == streamStatsReceiver) {
            streamStatsReceiver = new NullStatsReceiver();
        }

        Optional<ClusterClient> serverRoutingServiceClient = Optional.absent();
        if (null != serverRoutingServiceFinagleName) {
            serverRoutingServiceClient = Optional.of(
                    buildServerRoutingServiceClient(serverRoutingServiceFinagleName));
        }

        RoutingService routingService = routingServiceBuilder
                .statsReceiver(statsReceiver.scope("routing"))
                .build();
        DistributedLogClientImpl clientImpl =
                new DistributedLogClientImpl(
                        name,
                        clientId,
                        routingService,
                        clientBuilder,
                        clientConfig,
                        serverRoutingServiceClient,
                        statsReceiver,
                        streamStatsReceiver,
                        regionResolver,
                        enableRegionStats);
        routingService.startService();
        clientImpl.handshake();
        return clientImpl;
    }

}
