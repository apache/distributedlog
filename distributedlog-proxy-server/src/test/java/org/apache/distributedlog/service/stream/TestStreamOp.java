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
package org.apache.distributedlog.service.stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.distributedlog.AsyncLogWriter;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.acl.DefaultAccessControlManager;
import org.apache.distributedlog.exceptions.InternalServerException;
import org.apache.distributedlog.service.ResponseUtils;
import org.apache.distributedlog.service.config.ServerConfiguration;
import org.apache.distributedlog.service.streamset.IdentityStreamPartitionConverter;
import org.apache.distributedlog.thrift.service.StatusCode;
import org.apache.distributedlog.thrift.service.WriteResponse;
import org.apache.distributedlog.util.FutureUtils;
import org.apache.distributedlog.util.Sequencer;
import com.twitter.util.Await;
import java.nio.ByteBuffer;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test Case for StreamOps.
 */
public class TestStreamOp {

    @Rule
    public TestName testName = new TestName();

    private WriteOp getWriteOp() {
        SettableFeature disabledFeature = new SettableFeature("", 0);
        return new WriteOp("test",
            ByteBuffer.wrap("test".getBytes()),
            new NullStatsLogger(),
            new NullStatsLogger(),
            new IdentityStreamPartitionConverter(),
            new ServerConfiguration(),
            (byte) 0,
            null,
            false,
            disabledFeature,
            DefaultAccessControlManager.INSTANCE);
    }

    @Test(timeout = 60000)
    public void testResponseFailedTwice() throws Exception {
        WriteOp writeOp = getWriteOp();
        writeOp.fail(new InternalServerException("test1"));
        writeOp.fail(new InternalServerException("test2"));

        WriteResponse response = Await.result(writeOp.result());
        assertEquals(StatusCode.INTERNAL_SERVER_ERROR, response.getHeader().getCode());
        assertEquals(ResponseUtils.exceptionToHeader(new InternalServerException("test1")), response.getHeader());
    }

    @Test(timeout = 60000)
    public void testResponseSucceededThenFailed() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        when(writer.write((LogRecord) any())).thenReturn(FutureUtils.value(new DLSN(1, 2, 3)));
        when(writer.getStreamName()).thenReturn("test");
        WriteOp writeOp = getWriteOp();
        writeOp.execute(writer, new Sequencer() {
            public long nextId() {
                return 0;
            }
        }, new Object());
        writeOp.fail(new InternalServerException("test2"));

        WriteResponse response = Await.result(writeOp.result());
        assertEquals(StatusCode.SUCCESS, response.getHeader().getCode());
    }
}
