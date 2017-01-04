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
package com.twitter.distributedlog.service.placement;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.impl.BKNamespaceDriver;
import com.twitter.distributedlog.util.Utils;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the PlacementStateManager that saves data to and loads from Zookeeper to
 * avoid necessitating an additional system for the resource placement.
 */
public class ZKPlacementStateManager implements PlacementStateManager {

    private static final Logger logger = LoggerFactory.getLogger(ZKPlacementStateManager.class);

    private static final String SERVER_LOAD_DIR = "/.server-load";

    private final String serverLoadPath;
    private final ZooKeeperClient zkClient;

    private boolean watching = false;

    public ZKPlacementStateManager(URI uri, DistributedLogConfiguration conf, StatsLogger statsLogger) {
        String zkServers = BKNamespaceDriver.getZKServersFromDLUri(uri);
        zkClient = BKNamespaceDriver.createZKClientBuilder(
            String.format("ZKPlacementStateManager-%s", zkServers),
            conf,
            zkServers,
            statsLogger.scope("placement_state_manager")).build();
        serverLoadPath = uri.getPath() + SERVER_LOAD_DIR;
    }

    private void createServerLoadPathIfNoExists(byte[] data)
        throws ZooKeeperClient.ZooKeeperConnectionException, KeeperException, InterruptedException {
        try {
            Utils.zkCreateFullPathOptimistic(
                zkClient, serverLoadPath, data, zkClient.getDefaultACL(), CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            logger.debug("the server load path {} is already created by others", serverLoadPath, nee);
        }
    }

    @Override
    public void saveOwnership(TreeSet<ServerLoad> serverLoads) throws StateManagerSaveException {
        logger.info("saving ownership");
        try {
            ZooKeeper zk = zkClient.get();
            // use timestamp as data so watchers will see any changes
            byte[] timestamp = ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array();

            if (zk.exists(serverLoadPath, false) == null) { //create path to rootnode if it does not yet exist
                createServerLoadPathIfNoExists(timestamp);
            }

            Transaction tx = zk.transaction();
            List<String> children = zk.getChildren(serverLoadPath, false);
            HashSet<String> servers = new HashSet<String>(children);
            tx.setData(serverLoadPath, timestamp, -1); // trigger the watcher that data has been updated
            for (ServerLoad serverLoad : serverLoads) {
                String server = serverToZkFormat(serverLoad.getServer());
                String serverPath = serverPath(server);
                if (servers.contains(server)) {
                    servers.remove(server);
                    tx.setData(serverPath, serverLoad.serialize(), -1);
                } else {
                    tx.create(serverPath, serverLoad.serialize(), zkClient.getDefaultACL(), CreateMode.PERSISTENT);
                }
            }
            for (String server : servers) {
                tx.delete(serverPath(server), -1);
            }
            tx.commit();
        } catch (InterruptedException | IOException | KeeperException e) {
            throw new StateManagerSaveException(e);
        }
    }

    @Override
    public TreeSet<ServerLoad> loadOwnership() throws StateManagerLoadException {
        TreeSet<ServerLoad> ownerships = new TreeSet<ServerLoad>();
        try {
            ZooKeeper zk = zkClient.get();
            List<String> children = zk.getChildren(serverLoadPath, false);
            for (String server : children) {
                ownerships.add(ServerLoad.deserialize(zk.getData(serverPath(server), false, new Stat())));
            }
            return ownerships;
        } catch (InterruptedException | IOException | KeeperException e) {
            throw new StateManagerLoadException(e);
        }
    }

    @Override
    public synchronized void watch(final PlacementCallback callback) {
        if (watching) {
            return; // do not double watch
        }
        watching = true;

        try {
            ZooKeeper zk = zkClient.get();
            try {
                zk.getData(serverLoadPath, new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        try {
                            callback.callback(loadOwnership());
                        } catch (StateManagerLoadException e) {
                            logger.error("Watch of Ownership failed", e);
                        } finally {
                            watching = false;
                            watch(callback);
                        }
                    }
                }, new Stat());
            } catch (KeeperException.NoNodeException nee) {
                byte[] timestamp = ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array();
                createServerLoadPathIfNoExists(timestamp);
                watching = false;
                watch(callback);
            }
        } catch (ZooKeeperClient.ZooKeeperConnectionException | InterruptedException | KeeperException e) {
            logger.error("Watch of Ownership failed", e);
            watching = false;
            watch(callback);
        }
    }

    public String serverPath(String server) {
        return String.format("%s/%s", serverLoadPath, server);
    }

    protected String serverToZkFormat(String server) {
        return server.replaceAll("/", "--");
    }

    protected String zkFormatToServer(String zkFormattedServer) {
        return zkFormattedServer.replaceAll("--", "/");
    }
}
