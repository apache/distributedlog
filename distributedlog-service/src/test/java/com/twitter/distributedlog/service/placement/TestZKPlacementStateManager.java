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
package com.twitter.distributedlog.service.placement;

import java.io.IOException;
import java.net.URI;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.distributedlog.DistributedLogConfiguration;

import static com.twitter.distributedlog.LocalDLMEmulator.DLOG_NAMESPACE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestZKPlacementStateManager {
  private TestingServer zkTestServer;
  private String zkServers;
  private URI uri;
  private ZKPlacementStateManager zkPlacementStateManager;

  @Before
  public void startZookeeper() throws Exception {
    zkTestServer = new TestingServer(2181);
    zkServers = "127.0.0.1:2181";
    uri = new URI("distributedlog-bk://" + zkServers + DLOG_NAMESPACE + "/bknamespace");
    zkPlacementStateManager = new ZKPlacementStateManager(uri, new DistributedLogConfiguration(), NullStatsLogger.INSTANCE);
  }

  @Test(timeout = 60000)
  public void testSaveLoad() throws Exception {
    TreeSet<ServerLoad> ownerships = new TreeSet<ServerLoad>();
    zkPlacementStateManager.saveOwnership(ownerships);
    SortedSet<ServerLoad> loadedOwnerships = zkPlacementStateManager.loadOwnership();
    assertEquals(ownerships, loadedOwnerships);

    ownerships.add(new ServerLoad("emptyServer"));
    zkPlacementStateManager.saveOwnership(ownerships);
    loadedOwnerships = zkPlacementStateManager.loadOwnership();
    assertEquals(ownerships, loadedOwnerships);

    ServerLoad sl1 = new ServerLoad("server1");
    sl1.addStream(new StreamLoad("stream1", 3));
    sl1.addStream(new StreamLoad("stream2", 4));
    ServerLoad sl2 = new ServerLoad("server2");
    sl2.addStream(new StreamLoad("stream3", 1));
    ownerships.add(sl1);
    ownerships.add(sl2);
    zkPlacementStateManager.saveOwnership(ownerships);
    loadedOwnerships = zkPlacementStateManager.loadOwnership();
    assertEquals(ownerships, loadedOwnerships);

    loadedOwnerships.remove(sl1);
    zkPlacementStateManager.saveOwnership(ownerships);
    loadedOwnerships = zkPlacementStateManager.loadOwnership();
    assertEquals(ownerships, loadedOwnerships);
  }

  private TreeSet<ServerLoad> waitForServerLoadsNotificationAsc(
          LinkedBlockingQueue<TreeSet<ServerLoad>> notificationQueue,
          int expectedNumServerLoads) throws InterruptedException {
    TreeSet<ServerLoad> notification = notificationQueue.take();
    assertNotNull(notification);
    while (notification.size() < expectedNumServerLoads) {
      notification = notificationQueue.take();
    }
    assertEquals(expectedNumServerLoads, notification.size());
    return notification;
  }

  @Test(timeout = 60000)
  public void testWatchIndefinitely() throws Exception {
    TreeSet<ServerLoad> ownerships = new TreeSet<ServerLoad>();
    ownerships.add(new ServerLoad("server1"));
    final LinkedBlockingQueue<TreeSet<ServerLoad>> serverLoadNotifications =
            new LinkedBlockingQueue<TreeSet<ServerLoad>>();
    PlacementStateManager.PlacementCallback callback = new PlacementStateManager.PlacementCallback() {
      @Override
      public void callback(TreeSet<ServerLoad> serverLoads) {
        serverLoadNotifications.add(serverLoads);
      }
    };
    zkPlacementStateManager.saveOwnership(ownerships); // need to initialize the zk path before watching
    zkPlacementStateManager.watch(callback);
    // cannot verify the callback here as it may call before the verify is called

    zkPlacementStateManager.saveOwnership(ownerships);
    assertEquals(ownerships, waitForServerLoadsNotificationAsc(serverLoadNotifications, 1));

    ServerLoad server2 = new ServerLoad("server2");
    server2.addStream(new StreamLoad("hella-important-stream", 415));
    ownerships.add(server2);
    zkPlacementStateManager.saveOwnership(ownerships);
    assertEquals(ownerships, waitForServerLoadsNotificationAsc(serverLoadNotifications, 2));
  }

  @Test(timeout = 60000)
  public void testZkFormatting() throws Exception {
    final String server = "host/10.0.0.0:31351";
    final String zkFormattedServer = "host--10.0.0.0:31351";
    URI uri = new URI("distributedlog-bk://" + zkServers + DLOG_NAMESPACE + "/bknamespace");
    ZKPlacementStateManager zkPlacementStateManager = new ZKPlacementStateManager(uri, new DistributedLogConfiguration(), NullStatsLogger.INSTANCE);
    assertEquals(zkFormattedServer, zkPlacementStateManager.serverToZkFormat(server));
    assertEquals(server, zkPlacementStateManager.zkFormatToServer(zkFormattedServer));
  }

  @After
  public void stopZookeeper() throws IOException {
    zkTestServer.stop();
  }
}
