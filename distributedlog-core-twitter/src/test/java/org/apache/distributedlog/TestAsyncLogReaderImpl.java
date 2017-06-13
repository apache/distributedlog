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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.distributedlog.util.FutureUtils;
import org.junit.Test;

/**
 * Unit test of {@link AsyncLogReaderImpl}.
 */
public class TestAsyncLogReaderImpl {

    private final org.apache.distributedlog.api.AsyncLogReader underlying =
        mock(org.apache.distributedlog.api.AsyncLogReader.class);
    private final AsyncLogReaderImpl reader = new AsyncLogReaderImpl(underlying);

    @Test
    public void testRead() throws Exception {
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        when(underlying.readNext())
            .thenReturn(CompletableFuture.completedFuture(record));
        assertEquals(record, FutureUtils.result(reader.readNext()));
        verify(underlying, times(1)).readNext();
    }

    @Test
    public void testReadBulk() throws Exception {
        List<LogRecordWithDLSN> records = mock(List.class);
        when(underlying.readBulk(anyInt()))
            .thenReturn(CompletableFuture.completedFuture(records));
        assertEquals(records, FutureUtils.result(reader.readBulk(100)));
        verify(underlying, times(1)).readBulk(eq(100));
    }

    @Test
    public void testReadBulkWithWaitTime() throws Exception {
        List<LogRecordWithDLSN> records = mock(List.class);
        when(underlying.readBulk(anyInt(), anyLong(), any(TimeUnit.class)))
            .thenReturn(CompletableFuture.completedFuture(records));
        assertEquals(records, FutureUtils.result(reader.readBulk(100, 10, TimeUnit.MICROSECONDS)));
        verify(underlying, times(1))
            .readBulk(eq(100), eq(10L), eq(TimeUnit.MICROSECONDS));
    }

    @Test
    public void testGetStreamName() throws Exception {
        String streamName = "test-stream-name";
        when(underlying.getStreamName())
            .thenReturn(streamName);
        assertEquals(streamName, reader.getStreamName());
        verify(underlying, times(1)).getStreamName();
    }

    @Test
    public void testAsyncClose() throws Exception {
        when(underlying.asyncClose())
            .thenReturn(CompletableFuture.completedFuture(null));
        FutureUtils.result(reader.asyncClose());
        verify(underlying, times(1)).asyncClose();
    }

}
