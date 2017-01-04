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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.Test;

/**
 * Test Case for {@link ServerLoad}.
 */
public class TestServerLoad {

    @Test(timeout = 60000)
    public void testSerializeDeserialize() throws IOException {
        final ServerLoad serverLoad = new ServerLoad("th1s1s@s3rv3rn@m3");
        for (int i = 0; i < 20; i++) {
            serverLoad.addStream(new StreamLoad("stream-" + i, i));
        }
        assertEquals(serverLoad, ServerLoad.deserialize(serverLoad.serialize()));
    }

    @Test(timeout = 60000)
    public void testGetLoad() throws IOException {
        final ServerLoad serverLoad = new ServerLoad("th1s1s@s3rv3rn@m3");
        assertEquals(0, serverLoad.getLoad());
        serverLoad.addStream(new StreamLoad("stream-" + 1, 3));
        assertEquals(3, serverLoad.getLoad());
        serverLoad.addStream(new StreamLoad("stream-" + 2, 7));
        assertEquals(10, serverLoad.getLoad());
        serverLoad.addStream(new StreamLoad("stream-" + 3, 1));
        assertEquals(11, serverLoad.getLoad());
    }
}
