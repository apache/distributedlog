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

import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.util.Await;
import org.junit.Test;

import java.nio.ByteBuffer;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.fail;

/**
 * Test the server with client side routing.
 */
public class TestDistributedLogServerClientRouting extends TestDistributedLogServerBase {

    public TestDistributedLogServerClientRouting() {
        super(true);
    }

    @Test(timeout = 60000)
    public void testAcceptNewStream() throws Exception {
        String name = "dlserver-accept-new-stream";

        dlClient.routingService.addHost(name, dlServer.getAddress());
        dlClient.routingService.setAllowRetrySameHost(false);

        Await.result(dlClient.dlClient.setAcceptNewStream(false));

        try {
            Await.result(dlClient.dlClient.write(name, ByteBuffer.wrap("1".getBytes(UTF_8))));
            fail("Should fail because the proxy couldn't accept new stream");
        } catch (NoBrokersAvailableException nbae) {
            // expected
        }
        checkStream(0, 0, 0, name, dlServer.getAddress(), false, false);

        Await.result(dlServer.dlServer.getLeft().setAcceptNewStream(true));
        Await.result(dlClient.dlClient.write(name, ByteBuffer.wrap("1".getBytes(UTF_8))));
        checkStream(1, 1, 1, name, dlServer.getAddress(), true, true);
    }
}
