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
package org.apache.distributedlog.service;

import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LocalDLMEmulator;
import org.apache.distributedlog.client.routing.SingleHostRoutingService;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.distributedlog.service.placement.EqualLoadAppraiser;
import org.apache.distributedlog.service.streamset.IdentityStreamPartitionConverter;
import com.twitter.finagle.builder.Server;
import java.io.File;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DistributedLog Cluster is an emulator to run distributedlog components.
 */
public class DistributedLogCluster {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedLogCluster.class);

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder to build distributedlog cluster.
     */
    public static class Builder {

        int numBookies = 3;
        boolean shouldStartZK = true;
        String zkHost = "127.0.0.1";
        int zkPort = 0;
        boolean shouldStartProxy = true;
        int proxyPort = 7000;
        boolean thriftmux = false;
        DistributedLogConfiguration dlConf = new DistributedLogConfiguration()
                .setLockTimeout(10)
                .setOutputBufferSize(0)
                .setImmediateFlushEnabled(true);
        ServerConfiguration bkConf = new ServerConfiguration();

        private Builder() {}

        /**
         * How many bookies to run. By default is 3.
         *
         * @return builder
         */
        public Builder numBookies(int numBookies) {
            this.numBookies = numBookies;
            return this;
        }

        /**
         * Whether to start zookeeper? By default is true.
         *
         * @param startZK
         *          flag to start zookeeper?
         * @return builder
         */
        public Builder shouldStartZK(boolean startZK) {
            this.shouldStartZK = startZK;
            return this;
        }

        /**
         * ZooKeeper server to run. By default it runs locally on '127.0.0.1'.
         *
         * @param zkServers
         *          zk servers
         * @return builder
         */
        public Builder zkServers(String zkServers) {
            this.zkHost = zkServers;
            return this;
        }

        /**
         * ZooKeeper server port to listen on. By default it listens on 2181.
         *
         * @param zkPort
         *          zookeeper server port.
         * @return builder.
         */
        public Builder zkPort(int zkPort) {
            this.zkPort = zkPort;
            return this;
        }

        /**
         * Whether to start proxy or not. By default is true.
         *
         * @param startProxy
         *          whether to start proxy or not.
         * @return builder
         */
        public Builder shouldStartProxy(boolean startProxy) {
            this.shouldStartProxy = startProxy;
            return this;
        }

        /**
         * Port that proxy server to listen on. By default is 7000.
         *
         * @param proxyPort
         *          port that proxy server to listen on.
         * @return builder
         */
        public Builder proxyPort(int proxyPort) {
            this.proxyPort = proxyPort;
            return this;
        }

        /**
         * Set the distributedlog configuration.
         *
         * @param dlConf
         *          distributedlog configuration
         * @return builder
         */
        public Builder dlConf(DistributedLogConfiguration dlConf) {
            this.dlConf = dlConf;
            return this;
        }

        /**
         * Set the Bookkeeper server configuration.
         *
         * @param bkConf
         *          bookkeeper server configuration
         * @return builder
         */
        public Builder bkConf(ServerConfiguration bkConf) {
            this.bkConf = bkConf;
            return this;
        }

        /**
         * Enable thriftmux for the dl server.
         *
         * @param enabled flag to enable thriftmux
         * @return builder
         */
        public Builder thriftmux(boolean enabled) {
            this.thriftmux = enabled;
            return this;
        }

        public DistributedLogCluster build() throws Exception {
            // build the cluster
            return new DistributedLogCluster(
                dlConf,
                bkConf,
                numBookies,
                shouldStartZK,
                zkHost,
                zkPort,
                shouldStartProxy,
                proxyPort,
                thriftmux);
        }
    }

    /**
     * Run a distributedlog proxy server.
     */
    public static class DLServer {

        static final int MAX_RETRIES = 20;
        static final int MIN_PORT = 1025;
        static final int MAX_PORT = 65535;

        int proxyPort;

        public final InetSocketAddress address;
        public final Pair<DistributedLogServiceImpl, Server> dlServer;
        private final SingleHostRoutingService routingService = SingleHostRoutingService.of(null);

        protected DLServer(DistributedLogConfiguration dlConf,
                           URI uri,
                           int basePort,
                           boolean thriftmux) throws Exception {
            proxyPort = basePort;

            boolean success = false;
            int retries = 0;
            Pair<DistributedLogServiceImpl, Server> serverPair = null;
            while (!success) {
                try {
                    org.apache.distributedlog.service.config.ServerConfiguration serverConf =
                            new org.apache.distributedlog.service.config.ServerConfiguration();
                    serverConf.loadConf(dlConf);
                    serverConf.setServerShardId(proxyPort);
                    serverPair = DistributedLogServer.runServer(
                            serverConf,
                            dlConf,
                            uri,
                            new IdentityStreamPartitionConverter(),
                            routingService,
                            new NullStatsProvider(),
                            proxyPort,
                            thriftmux,
                            new EqualLoadAppraiser());
                    routingService.setAddress(DLSocketAddress.getSocketAddress(proxyPort));
                    routingService.startService();
                    serverPair.getLeft().startPlacementPolicy();
                    success = true;
                } catch (BindException be) {
                    retries++;
                    if (retries > MAX_RETRIES) {
                        throw be;
                    }
                    proxyPort++;
                    if (proxyPort > MAX_PORT) {
                        proxyPort = MIN_PORT;
                    }
                }
            }

            LOG.info("Running DL on port {}", proxyPort);

            dlServer = serverPair;
            address = DLSocketAddress.getSocketAddress(proxyPort);
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        public void shutdown() {
            DistributedLogServer.closeServer(dlServer, 0, TimeUnit.MILLISECONDS);
            routingService.stopService();
        }
    }

    private final DistributedLogConfiguration dlConf;
    private final ZooKeeperServerShim zks;
    private final LocalDLMEmulator dlmEmulator;
    private DLServer dlServer;
    private final boolean shouldStartProxy;
    private final int proxyPort;
    private final boolean thriftmux;
    private final List<File> tmpDirs = new ArrayList<File>();

    private DistributedLogCluster(DistributedLogConfiguration dlConf,
                                  ServerConfiguration bkConf,
                                  int numBookies,
                                  boolean shouldStartZK,
                                  String zkServers,
                                  int zkPort,
                                  boolean shouldStartProxy,
                                  int proxyPort,
                                  boolean thriftmux) throws Exception {
        this.dlConf = dlConf;
        if (shouldStartZK) {
            File zkTmpDir = IOUtils.createTempDir("zookeeper", "distrlog");
            tmpDirs.add(zkTmpDir);
            if (0 == zkPort) {
                Pair<ZooKeeperServerShim, Integer> serverAndPort = LocalDLMEmulator.runZookeeperOnAnyPort(zkTmpDir);
                this.zks = serverAndPort.getLeft();
                zkPort = serverAndPort.getRight();
            } else {
                this.zks = LocalBookKeeper.runZookeeper(1000, zkPort, zkTmpDir);
            }
        } else {
            this.zks = null;
        }
        this.dlmEmulator = LocalDLMEmulator.newBuilder()
                .numBookies(numBookies)
                .zkHost(zkServers)
                .zkPort(zkPort)
                .serverConf(bkConf)
                .shouldStartZK(false)
                .build();
        this.shouldStartProxy = shouldStartProxy;
        this.proxyPort = proxyPort;
        this.thriftmux = thriftmux;
    }

    public void start() throws Exception {
        this.dlmEmulator.start();
        BKDLConfig bkdlConfig = new BKDLConfig(this.dlmEmulator.getZkServers(), "/ledgers").setACLRootPath(".acl");
        DLMetadata.create(bkdlConfig).update(this.dlmEmulator.getUri());
        if (shouldStartProxy) {
            this.dlServer = new DLServer(
                    dlConf,
                    this.dlmEmulator.getUri(),
                    proxyPort,
                    thriftmux);
        } else {
            this.dlServer = null;
        }
    }

    public void stop() throws Exception {
        if (null != dlServer) {
            this.dlServer.shutdown();
        }
        this.dlmEmulator.teardown();
        if (null != this.zks) {
            this.zks.stop();
        }
        for (File dir : tmpDirs) {
            FileUtils.forceDeleteOnExit(dir);
        }
    }

    public URI getUri() {
        return this.dlmEmulator.getUri();
    }

    public String getZkServers() {
        return this.dlmEmulator.getZkServers();
    }

    public String getProxyFinagleStr() {
        return "inet!" + (dlServer == null ? "127.0.0.1:" + proxyPort : dlServer.getAddress().toString());
    }

}
