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

import static org.apache.distributedlog.util.FutureUtils.newTFuture;

import com.twitter.util.Future;
import java.io.IOException;
import java.util.List;
import org.apache.distributedlog.callback.LogSegmentListener;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.subscription.SubscriptionsStore;

/**
 * The wrapper of {@link org.apache.distributedlog.api.DistributedLogManager}.
 */
public class DistributedLogManagerImpl implements DistributedLogManager {

    private final org.apache.distributedlog.api.DistributedLogManager impl;

    public DistributedLogManagerImpl(org.apache.distributedlog.api.DistributedLogManager impl) {
        this.impl = impl;
    }

    @Override
    public String getStreamName() {
        return impl.getStreamName();
    }

    @Override
    public NamespaceDriver getNamespaceDriver() {
        return impl.getNamespaceDriver();
    }

    @Override
    public List<LogSegmentMetadata> getLogSegments() throws IOException {
        return impl.getLogSegments();
    }

    @Override
    public void registerListener(LogSegmentListener listener) throws IOException {
        impl.registerListener(listener);
    }

    @Override
    public void unregisterListener(LogSegmentListener listener) {
        impl.unregisterListener(listener);
    }

    @Override
    public Future<AsyncLogWriter> openAsyncLogWriter() {
        return newTFuture(impl.openAsyncLogWriter()).map(AsyncLogWriterImpl.MAP_FUNC);
    }

    @Override
    public LogWriter startLogSegmentNonPartitioned() throws IOException {
        return new LogWriterImpl(impl.startLogSegmentNonPartitioned());
    }

    @Override
    public AsyncLogWriter startAsyncLogSegmentNonPartitioned() throws IOException {
        return new AsyncLogWriterImpl(impl.startAsyncLogSegmentNonPartitioned());
    }

    @Override
    public AppendOnlyStreamWriter getAppendOnlyStreamWriter() throws IOException {
        return impl.getAppendOnlyStreamWriter();
    }

    @Override
    public AppendOnlyStreamReader getAppendOnlyStreamReader() throws IOException {
        return impl.getAppendOnlyStreamReader();
    }

    @Override
    public LogReader getInputStream(long fromTxnId) throws IOException {
        return new LogReaderImpl(impl.getInputStream(fromTxnId));
    }

    @Override
    public LogReader getInputStream(DLSN fromDLSN) throws IOException {
        return new LogReaderImpl(impl.getInputStream(fromDLSN));
    }

    @Override
    public Future<AsyncLogReader> openAsyncLogReader(long fromTxnId) {
        return newTFuture(impl.openAsyncLogReader(fromTxnId)).map(AsyncLogReaderImpl.MAP_FUNC);
    }

    @Override
    public Future<AsyncLogReader> openAsyncLogReader(DLSN fromDLSN) {
        return newTFuture(impl.openAsyncLogReader(fromDLSN)).map(AsyncLogReaderImpl.MAP_FUNC);
    }

    @Override
    public AsyncLogReader getAsyncLogReader(long fromTxnId) throws IOException {
        return new AsyncLogReaderImpl(impl.getAsyncLogReader(fromTxnId));
    }

    @Override
    public AsyncLogReader getAsyncLogReader(DLSN fromDLSN) throws IOException {
        return new AsyncLogReaderImpl(impl.getAsyncLogReader(fromDLSN));
    }

    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(DLSN fromDLSN) {
        return newTFuture(impl.getAsyncLogReaderWithLock(fromDLSN)).map(AsyncLogReaderImpl.MAP_FUNC);
    }

    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(DLSN fromDLSN, String subscriberId) {
        return newTFuture(impl.getAsyncLogReaderWithLock(fromDLSN, subscriberId))
            .map(AsyncLogReaderImpl.MAP_FUNC);
    }

    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(String subscriberId) {
        return newTFuture(impl.getAsyncLogReaderWithLock(subscriberId))
            .map(AsyncLogReaderImpl.MAP_FUNC);
    }

    @Override
    public Future<DLSN> getDLSNNotLessThanTxId(long transactionId) {
        return newTFuture(impl.getDLSNNotLessThanTxId(transactionId));
    }

    @Override
    public LogRecordWithDLSN getLastLogRecord() throws IOException {
        return impl.getLastLogRecord();
    }

    @Override
    public long getFirstTxId() throws IOException {
        return impl.getFirstTxId();
    }

    @Override
    public long getLastTxId() throws IOException {
        return impl.getLastTxId();
    }

    @Override
    public DLSN getLastDLSN() throws IOException {
        return impl.getLastDLSN();
    }

    @Override
    public Future<LogRecordWithDLSN> getLastLogRecordAsync() {
        return newTFuture(impl.getLastLogRecordAsync());
    }

    @Override
    public Future<Long> getLastTxIdAsync() {
        return newTFuture(impl.getLastTxIdAsync());
    }

    @Override
    public Future<DLSN> getFirstDLSNAsync() {
        return newTFuture(impl.getFirstDLSNAsync());
    }

    @Override
    public Future<DLSN> getLastDLSNAsync() {
        return newTFuture(impl.getLastDLSNAsync());
    }

    @Override
    public long getLogRecordCount() throws IOException {
        return impl.getLogRecordCount();
    }

    @Override
    public Future<Long> getLogRecordCountAsync(DLSN beginDLSN) {
        return newTFuture(impl.getLogRecordCountAsync(beginDLSN));
    }

    @Override
    public void recover() throws IOException {
        impl.recover();
    }

    @Override
    public boolean isEndOfStreamMarked() throws IOException {
        return impl.isEndOfStreamMarked();
    }

    @Override
    public void delete() throws IOException {
        impl.delete();
    }

    @Override
    public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
        impl.purgeLogsOlderThan(minTxIdToKeep);
    }

    @Override
    public SubscriptionsStore getSubscriptionsStore() {
        return new SubscriptionsStoreImpl(impl.getSubscriptionsStore());
    }

    @Override
    public void close() throws IOException {
        impl.close();
    }

    @Override
    public Future<Void> asyncClose() {
        return newTFuture(impl.asyncClose());
    }
}
