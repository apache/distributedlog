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

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import org.junit.Test;

/**
 * Test Case for {@link ServerUtils}.
 */
public class TestServerUtils {

    @Test(timeout = 6000)
    public void testGetLedgerAllocatorPoolName() throws Exception {
        int region = 123;
        int shard = 999;
        String hostname = InetAddress.getLocalHost().getHostAddress();
        assertEquals("allocator_0123_0000000999",
            ServerUtils.getLedgerAllocatorPoolName(region, shard, false));
        assertEquals("allocator_0123_" + hostname,
            ServerUtils.getLedgerAllocatorPoolName(region, shard, true));
    }

}
