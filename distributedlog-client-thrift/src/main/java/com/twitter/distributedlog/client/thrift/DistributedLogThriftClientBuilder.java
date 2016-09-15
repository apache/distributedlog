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
package com.twitter.distributedlog.client.thrift;

import com.google.common.base.Preconditions;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.client.ClientConfig;
import com.twitter.distributedlog.client.finagle.routing.FinagleRoutingUtils;
import com.twitter.distributedlog.client.finagle.routing.RegionsRoutingService;
import com.twitter.distributedlog.client.proxy.ProxyClient;
import com.twitter.distributedlog.client.finagle.routing.RoutingService;
import com.twitter.distributedlog.client.stats.ClientStats;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import org.apache.commons.lang.StringUtils;

import java.net.URI;
import java.util.Random;

/**
 * DistributedLog Thrift Client Builder
 */
public final class DistributedLogThriftClientBuilder
        extends DistributedLogClientBuilder<DistributedLogThriftClientBuilder> {

    private static final Random random = new Random(System.currentTimeMillis());

    private ClientId _clientId = null;
    private ClientBuilder _clientBuilder = null;

    /**
     * Create a client builder
     *
     * @return client builder
     */
    public static DistributedLogThriftClientBuilder newBuilder() {
        return new DistributedLogThriftClientBuilder();
    }

    /**
     * Create a client builder from existing <i>builder</i>.
     *
     * @param builder existing builder to build thrift client
     * @return thrift client builder
     */
    public static DistributedLogThriftClientBuilder newBuilder(DistributedLogClientBuilder builder) {
        Preconditions.checkArgument(builder instanceof DistributedLogThriftClientBuilder,
                "Invalid thrift client builder found - " + builder.getClass().getName());
        return new DistributedLogThriftClientBuilder().createBuilder(builder);
    }

    public DistributedLogThriftClientBuilder getBuilder() {
        return this;
    }

    @Override
    protected DistributedLogThriftClientBuilder createBuilder(
            DistributedLogClientBuilder<DistributedLogThriftClientBuilder> builder) {
        DistributedLogThriftClientBuilder newBuilder = new DistributedLogThriftClientBuilder();
        DistributedLogThriftClientBuilder oldBuilder = builder.getBuilder();
        copyBuilder(builder, newBuilder);
        newBuilder._clientId = oldBuilder._clientId;
        newBuilder._clientBuilder = oldBuilder._clientBuilder;
        return newBuilder;
    }

    /**
     * Client ID.
     *
     * @param clientId
     *          client id
     * @return client builder.
     */
    public DistributedLogThriftClientBuilder clientId(ClientId clientId) {
        DistributedLogThriftClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientId = clientId;
        return newBuilder;
    }

    /**
     * Set underlying finagle client builder.
     *
     * @param builder
     *          finagle client builder.
     * @return client builder.
     */
    public DistributedLogThriftClientBuilder clientBuilder(ClientBuilder builder) {
        DistributedLogThriftClientBuilder newBuilder = newBuilder(this);
        newBuilder._clientBuilder = builder;
        return newBuilder;
    }

    /**
     * Serverset to access proxy services.
     *
     * @param serverSet
     *          server set.
     * @return client builder.
     */
    public DistributedLogThriftClientBuilder serverSet(ServerSet serverSet) {
        return routingServiceBuilder(
                FinagleRoutingUtils.buildRoutingService(serverSet),
                false).getBuilder();
    }

    /**
     * Server Sets to access proxy services. The <i>local</i> server set will be tried first,
     * then <i>remotes</i>.
     *
     * @param local local server set.
     * @param remotes remote server sets.
     * @return client builder.
     */
    public DistributedLogThriftClientBuilder serverSets(ServerSet local, ServerSet...remotes) {
        RoutingService.Builder[] builders = new RoutingService.Builder[remotes.length + 1];
        builders[0] = FinagleRoutingUtils.buildRoutingService(local);
        for (int i = 1; i < builders.length; i++) {
            builders[i] = FinagleRoutingUtils.buildRoutingService(remotes[i-1]);
        }
        return routingServiceBuilder(
                RegionsRoutingService.newBuilder()
                        .resolver(_regionResolver)
                        .routingServiceBuilders(builders),
                remotes.length > 0).getBuilder();
    }

    /**
     * Name to access proxy services.
     *
     * @param finagleNameStr
     *          finagle name string.
     * @return client builder.
     */
    public DistributedLogThriftClientBuilder finagleNameStr(String finagleNameStr) {
        return routingServiceBuilder(
                FinagleRoutingUtils.buildRoutingService(finagleNameStr),
                false).getBuilder();
    }

    /**
     * Finagle name strs to access proxy services. The <i>local</i> finalge name str will be tried first,
     * then <i>remotes</i>.
     *
     * @param local local server set.
     * @param remotes remote server sets.
     * @return client builder.
     */
    public DistributedLogThriftClientBuilder finagleNameStrs(String local, String...remotes) {
        RoutingService.Builder[] builders = new RoutingService.Builder[remotes.length + 1];
        builders[0] = FinagleRoutingUtils.buildRoutingService(local);
        for (int i = 1; i < builders.length; i++) {
            builders[i] = FinagleRoutingUtils.buildRoutingService(remotes[i - 1]);
        }
        return routingServiceBuilder(
                RegionsRoutingService.newBuilder()
                        .routingServiceBuilders(builders)
                        .resolver(_regionResolver),
                remotes.length > 0).getBuilder();
    }

    /**
     * URI to access proxy services. Assuming the write proxies are announced under `.write_proxy` of
     * the provided namespace uri.
     * <p>
     * The builder will convert the dl uri (e.g. distributedlog://{zkserver}/path/to/namespace) to
     * zookeeper serverset based finagle name str (`zk!{zkserver}!/path/to/namespace/.write_proxy`)
     *
     * @param uri namespace uri to access the serverset of write proxies
     * @return distributedlog builder
     */
    public DistributedLogThriftClientBuilder uri(URI uri) {
        String zkServers = uri.getAuthority().replace(";", ",");
        String[] zkServerList = StringUtils.split(zkServers, ',');
        String finagleNameStr = String.format(
                "zk!%s!%s/.write_proxy",
                zkServerList[random.nextInt(zkServerList.length)], // zk server
                uri.getPath());
        return routingServiceBuilder(
                FinagleRoutingUtils.buildRoutingService(finagleNameStr),
                false).getBuilder();
    }

    @Override
    protected void beforeBuildClient() {
        super.beforeBuildClient();
        Preconditions.checkNotNull(_clientId, "No client id provided.");
    }

    @Override
    protected ProxyClient.Builder newProxyClientBuilder(ClientConfig clientConfig,
                                                        ClientStats clientStats) {
        return ProxyThriftClient.newBuilder(
                _name,
                _clientId,
                _clientBuilder,
                clientConfig,
                clientStats);
    }
}
