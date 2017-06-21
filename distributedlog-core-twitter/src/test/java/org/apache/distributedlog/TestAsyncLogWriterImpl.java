/*
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

package org.apache.distributedlog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.twitter.util.Futures;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.util.FutureUtils;
import org.junit.Test;

/**
 * Unit test of {@link AsyncLogWriterImpl}.
 */
public class TestAsyncLogWriterImpl {

    private final org.apache.distributedlog.api.AsyncLogWriter underlying =
        mock(org.apache.distributedlog.api.AsyncLogWriter.class);
    private final AsyncLogWriterImpl writer = new AsyncLogWriterImpl(underlying);

    @Test
    public void testWrite() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        LogRecord record = mock(LogRecord.class);
        when(underlying.write(any(LogRecord.class)))
            .thenReturn(CompletableFuture.completedFuture(dlsn));
        assertEquals(dlsn, FutureUtils.result(writer.write(record)));
        verify(underlying, times(1)).write(eq(record));
    }

    @Test
    public void testWriteBulk() throws Exception {
        List<LogRecord> records = mock(List.class);
        List<CompletableFuture<DLSN>> futures = Lists.newArrayList();
        List<DLSN> dlsns = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            DLSN dlsn = mock(DLSN.class);
            dlsns.add(dlsn);
            futures.add(CompletableFuture.completedFuture(dlsn));
        }
        when(underlying.writeBulk(any(List.class)))
            .thenReturn(CompletableFuture.completedFuture(futures));
        assertEquals(
            dlsns,
            FutureUtils.result(Futures.collect(
                FutureUtils.result(writer.writeBulk(records)))));
        verify(underlying, times(1)).writeBulk(eq(records));
    }

    @Test
    public void testGetLastTxId() throws Exception {
        long txId = 123456L;
        when(underlying.getLastTxId()).thenReturn(txId);
        assertEquals(txId, writer.getLastTxId());
        verify(underlying, times(1)).getLastTxId();
    }

    @Test
    public void testTruncate() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        when(underlying.truncate(dlsn))
            .thenReturn(CompletableFuture.completedFuture(true));
        assertTrue(FutureUtils.result(writer.truncate(dlsn)));
        verify(underlying, times(1)).truncate(eq(dlsn));
    }

    @Test
    public void testGetStreamName() throws Exception {
        String streamName = "test-stream-name";
        when(underlying.getStreamName())
            .thenReturn(streamName);
        assertEquals(streamName, writer.getStreamName());
        verify(underlying, times(1)).getStreamName();
    }

    @Test
    public void testAsyncClose() throws Exception {
        when(underlying.asyncClose())
            .thenReturn(CompletableFuture.completedFuture(null));
        FutureUtils.result(writer.asyncClose());
        verify(underlying, times(1)).asyncClose();
    }

    @Test
    public void testAsyncAbort() throws Exception {
        when(underlying.asyncAbort())
            .thenReturn(CompletableFuture.completedFuture(null));
        FutureUtils.result(writer.asyncAbort());
        verify(underlying, times(1)).asyncAbort();
    }

}
