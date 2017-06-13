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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.LogWriter;
import org.apache.distributedlog.api.subscription.SubscriptionsStore;
import org.apache.distributedlog.callback.LogSegmentListener;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.util.FutureUtils;
import org.junit.Test;

/**
 * Unit test of {@link DistributedLogManagerImpl}.
 */
public class TestDistributedLogManagerImpl {

    private final org.apache.distributedlog.api.DistributedLogManager impl =
        mock(org.apache.distributedlog.api.DistributedLogManager.class);
    private final DistributedLogManagerImpl manager = new DistributedLogManagerImpl(impl);

    @Test
    public void testGetStreamName() throws Exception {
        String name = "test-get-stream-name";
        when(impl.getStreamName()).thenReturn(name);
        assertEquals(name, manager.getStreamName());
        verify(impl, times(1)).getStreamName();
    }

    @Test
    public void testGetNamespaceDriver() throws Exception {
        NamespaceDriver driver = mock(NamespaceDriver.class);
        when(impl.getNamespaceDriver()).thenReturn(driver);
        assertEquals(driver, manager.getNamespaceDriver());
        verify(impl, times(1)).getNamespaceDriver();
    }

    @Test
    public void testGetLogSegments() throws Exception {
        List<LogSegmentMetadata> segments = mock(List.class);
        when(impl.getLogSegments()).thenReturn(segments);
        assertEquals(segments, manager.getLogSegments());
        verify(impl, times(1)).getLogSegments();
    }

    @Test
    public void testRegisterListener() throws Exception {
        LogSegmentListener listener = mock(LogSegmentListener.class);
        manager.registerListener(listener);
        verify(impl, times(1)).registerListener(listener);
    }

    @Test
    public void testUnregisterListener() throws Exception {
        LogSegmentListener listener = mock(LogSegmentListener.class);
        manager.unregisterListener(listener);
        verify(impl, times(1)).unregisterListener(listener);
    }

    @Test
    public void testOpenAsyncLogWriter() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        when(impl.openAsyncLogWriter()).thenReturn(CompletableFuture.completedFuture(writer));
        assertEquals(writer, ((AsyncLogWriterImpl) FutureUtils.result(manager.openAsyncLogWriter())).getImpl());
        verify(impl, times(1)).openAsyncLogWriter();
    }

    @Test
    public void testStartLogSegmentNonPartitioned() throws Exception {
        LogWriter writer = mock(LogWriter.class);
        when(impl.startLogSegmentNonPartitioned()).thenReturn(writer);
        assertEquals(writer, ((LogWriterImpl) manager.startLogSegmentNonPartitioned()).getImpl());
        verify(impl, times(1)).startLogSegmentNonPartitioned();
    }

    @Test
    public void testStartAsyncLogSegmentNonPartitioned() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        when(impl.startAsyncLogSegmentNonPartitioned()).thenReturn(writer);
        assertEquals(writer, ((AsyncLogWriterImpl) manager.startAsyncLogSegmentNonPartitioned()).getImpl());
        verify(impl, times(1)).startAsyncLogSegmentNonPartitioned();
    }

    @Test
    public void testGetAppendOnlyStreamWriter() throws Exception {
        AppendOnlyStreamWriter writer = mock(AppendOnlyStreamWriter.class);
        when(impl.getAppendOnlyStreamWriter()).thenReturn(writer);
        assertEquals(writer, manager.getAppendOnlyStreamWriter());
        verify(impl, times(1)).getAppendOnlyStreamWriter();
    }

    @Test
    public void testGetAppendOnlyStreamReader() throws Exception {
        AppendOnlyStreamReader writer = mock(AppendOnlyStreamReader.class);
        when(impl.getAppendOnlyStreamReader()).thenReturn(writer);
        assertEquals(writer, manager.getAppendOnlyStreamReader());
        verify(impl, times(1)).getAppendOnlyStreamReader();
    }

    @Test
    public void testGetInputStream() throws Exception {
        LogReader reader = mock(LogReader.class);
        when(impl.getInputStream(anyLong())).thenReturn(reader);
        assertEquals(reader, ((LogReaderImpl) manager.getInputStream(1234L)).getImpl());
        verify(impl, times(1)).getInputStream(eq(1234L));
    }

    @Test
    public void testGetInputStream2() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        LogReader reader = mock(LogReader.class);
        when(impl.getInputStream(eq(dlsn))).thenReturn(reader);
        assertEquals(reader, ((LogReaderImpl) manager.getInputStream(dlsn)).getImpl());
        verify(impl, times(1)).getInputStream(eq(dlsn));
    }

    @Test
    public void testOpenAsyncLogReader() throws Exception {
        org.apache.distributedlog.api.AsyncLogReader reader = mock(org.apache.distributedlog.api.AsyncLogReader.class);
        when(impl.openAsyncLogReader(eq(1234L))).thenReturn(CompletableFuture.completedFuture(reader));
        assertEquals(reader,
            ((AsyncLogReaderImpl) FutureUtils.result(manager.openAsyncLogReader(1234L))).getImpl());
        verify(impl, times(1)).openAsyncLogReader(eq(1234L));
    }

    @Test
    public void testOpenAsyncLogReader2() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        org.apache.distributedlog.api.AsyncLogReader reader = mock(org.apache.distributedlog.api.AsyncLogReader.class);
        when(impl.openAsyncLogReader(eq(dlsn))).thenReturn(CompletableFuture.completedFuture(reader));
        assertEquals(reader,
            ((AsyncLogReaderImpl) FutureUtils.result(manager.openAsyncLogReader(dlsn))).getImpl());
        verify(impl, times(1)).openAsyncLogReader(eq(dlsn));
    }

    @Test
    public void testGetAsyncLogReader() throws Exception {
        org.apache.distributedlog.api.AsyncLogReader reader = mock(org.apache.distributedlog.api.AsyncLogReader.class);
        when(impl.getAsyncLogReader(eq(1234L))).thenReturn(reader);
        assertEquals(reader,
            ((AsyncLogReaderImpl) manager.getAsyncLogReader(1234L)).getImpl());
        verify(impl, times(1)).getAsyncLogReader(eq(1234L));
    }

    @Test
    public void testGetAsyncLogReader2() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        org.apache.distributedlog.api.AsyncLogReader reader = mock(org.apache.distributedlog.api.AsyncLogReader.class);
        when(impl.getAsyncLogReader(eq(dlsn))).thenReturn(reader);
        assertEquals(reader,
            ((AsyncLogReaderImpl) manager.getAsyncLogReader(dlsn)).getImpl());
        verify(impl, times(1)).getAsyncLogReader(eq(dlsn));
    }

    @Test
    public void testOpenAsyncLogReaderWithLock() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        org.apache.distributedlog.api.AsyncLogReader reader = mock(org.apache.distributedlog.api.AsyncLogReader.class);
        when(impl.getAsyncLogReaderWithLock(eq(dlsn))).thenReturn(CompletableFuture.completedFuture(reader));
        assertEquals(reader,
            ((AsyncLogReaderImpl) FutureUtils.result(manager.getAsyncLogReaderWithLock(dlsn))).getImpl());
        verify(impl, times(1)).getAsyncLogReaderWithLock(eq(dlsn));
    }

    @Test
    public void testOpenAsyncLogReaderWithLock2() throws Exception {
        String subscriberId = "test-subscriber";
        DLSN dlsn = mock(DLSN.class);
        org.apache.distributedlog.api.AsyncLogReader reader = mock(org.apache.distributedlog.api.AsyncLogReader.class);
        when(impl.getAsyncLogReaderWithLock(eq(dlsn), eq(subscriberId)))
            .thenReturn(CompletableFuture.completedFuture(reader));
        assertEquals(reader,
            ((AsyncLogReaderImpl) FutureUtils.result(manager.getAsyncLogReaderWithLock(dlsn, subscriberId))).getImpl());
        verify(impl, times(1)).getAsyncLogReaderWithLock(eq(dlsn), eq(subscriberId));
    }

    @Test
    public void testOpenAsyncLogReaderWithLock3() throws Exception {
        String subscriberId = "test-subscriber";
        org.apache.distributedlog.api.AsyncLogReader reader = mock(org.apache.distributedlog.api.AsyncLogReader.class);
        when(impl.getAsyncLogReaderWithLock(eq(subscriberId)))
            .thenReturn(CompletableFuture.completedFuture(reader));
        assertEquals(reader,
            ((AsyncLogReaderImpl) FutureUtils.result(manager.getAsyncLogReaderWithLock(subscriberId))).getImpl());
        verify(impl, times(1)).getAsyncLogReaderWithLock(eq(subscriberId));
    }

    @Test
    public void testGetDLSNNotLessThanTxId() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        when(impl.getDLSNNotLessThanTxId(anyLong())).thenReturn(CompletableFuture.completedFuture(dlsn));
        assertEquals(dlsn, FutureUtils.result(manager.getDLSNNotLessThanTxId(1234L)));
        verify(impl, times(1)).getDLSNNotLessThanTxId(eq(1234L));
    }

    @Test
    public void testGetLastLogRecord() throws Exception {
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        when(impl.getLastLogRecord()).thenReturn(record);
        assertEquals(record, manager.getLastLogRecord());
        verify(impl, times(1)).getLastLogRecord();
    }

    @Test
    public void testFirstTxId() throws Exception {
        long txId = System.currentTimeMillis();
        when(impl.getFirstTxId()).thenReturn(txId);
        assertEquals(txId, manager.getFirstTxId());
        verify(impl, times(1)).getFirstTxId();
    }

    @Test
    public void testLastTxId() throws Exception {
        long txId = System.currentTimeMillis();
        when(impl.getLastTxId()).thenReturn(txId);
        assertEquals(txId, manager.getLastTxId());
        verify(impl, times(1)).getLastTxId();
    }

    @Test
    public void testLastDLSN() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        when(impl.getLastDLSN()).thenReturn(dlsn);
        assertEquals(dlsn, manager.getLastDLSN());
        verify(impl, times(1)).getLastDLSN();
    }

    @Test
    public void testGetLastLogRecordAsync() throws Exception {
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        when(impl.getLastLogRecordAsync()).thenReturn(CompletableFuture.completedFuture(record));
        assertEquals(record, FutureUtils.result(manager.getLastLogRecordAsync()));
        verify(impl, times(1)).getLastLogRecordAsync();
    }

    @Test
    public void testLastTxIdAsync() throws Exception {
        long txId = System.currentTimeMillis();
        when(impl.getLastTxIdAsync()).thenReturn(CompletableFuture.completedFuture(txId));
        assertEquals(txId, FutureUtils.result(manager.getLastTxIdAsync()).longValue());
        verify(impl, times(1)).getLastTxIdAsync();
    }

    @Test
    public void testLastDLSNAsync() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        when(impl.getLastDLSNAsync()).thenReturn(CompletableFuture.completedFuture(dlsn));
        assertEquals(dlsn, FutureUtils.result(manager.getLastDLSNAsync()));
        verify(impl, times(1)).getLastDLSNAsync();
    }

    @Test
    public void testFirstDLSNAsync() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        when(impl.getFirstDLSNAsync()).thenReturn(CompletableFuture.completedFuture(dlsn));
        assertEquals(dlsn, FutureUtils.result(manager.getFirstDLSNAsync()));
        verify(impl, times(1)).getFirstDLSNAsync();
    }

    @Test
    public void testGetLogRecordCount() throws Exception {
        long count = System.currentTimeMillis();
        when(impl.getLogRecordCount()).thenReturn(count);
        assertEquals(count, manager.getLogRecordCount());
        verify(impl, times(1)).getLogRecordCount();
    }

    @Test
    public void testGetLogRecordCountAsync() throws Exception {
        DLSN dlsn = mock(DLSN.class);
        long count = System.currentTimeMillis();
        when(impl.getLogRecordCountAsync(eq(dlsn))).thenReturn(CompletableFuture.completedFuture(count));
        assertEquals(count, FutureUtils.result(manager.getLogRecordCountAsync(dlsn)).longValue());
        verify(impl, times(1)).getLogRecordCountAsync(eq(dlsn));
    }

    @Test
    public void testRecover() throws Exception {
        manager.recover();
        verify(impl, times(1)).recover();
    }

    @Test
    public void testIsEndOfStreamMarked() throws Exception {
        when(impl.isEndOfStreamMarked()).thenReturn(true);
        assertTrue(manager.isEndOfStreamMarked());
        verify(impl, times(1)).isEndOfStreamMarked();
    }

    @Test
    public void testDelete() throws Exception {
        manager.delete();
        verify(impl, times(1)).delete();
    }

    @Test
    public void testPurgeLogsOlderThan() throws Exception {
        long minTxIdToKeep = System.currentTimeMillis();
        manager.purgeLogsOlderThan(minTxIdToKeep);
        verify(impl, times(1)).purgeLogsOlderThan(eq(minTxIdToKeep));
    }

    @Test
    public void testGetSubscriptionsStore() throws Exception {
        SubscriptionsStore ss = mock(SubscriptionsStore.class);
        when(impl.getSubscriptionsStore()).thenReturn(ss);
        assertEquals(ss, ((SubscriptionsStoreImpl) manager.getSubscriptionsStore()).getImpl());
        verify(impl, times(1)).getSubscriptionsStore();
    }

    @Test
    public void testClose() throws Exception {
        manager.close();
        verify(impl, times(1)).close();
    }

    @Test
    public void testAsyncClose() throws Exception {
        when(impl.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        FutureUtils.result(manager.asyncClose());
        verify(impl, times(1)).asyncClose();
    }
}
