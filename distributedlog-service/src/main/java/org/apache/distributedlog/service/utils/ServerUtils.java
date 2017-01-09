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
package org.apache.distributedlog.service.utils;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Utils that used by servers.
 */
public class ServerUtils {

  /**
   * Retrieve the ledger allocator pool name.
   *
   * @param serverRegionId region id that that server is running
   * @param shardId shard id of the server
   * @param useHostname whether to use hostname as the ledger allocator pool name
   * @return ledger allocator pool name
   * @throws IOException
   */
    public static String getLedgerAllocatorPoolName(int serverRegionId,
                                                    int shardId,
                                                    boolean useHostname)
        throws IOException {
        if (useHostname) {
            return String.format("allocator_%04d_%s", serverRegionId,
                InetAddress.getLocalHost().getHostAddress());
        } else {
            return String.format("allocator_%04d_%010d", serverRegionId, shardId);
        }
    }

}
