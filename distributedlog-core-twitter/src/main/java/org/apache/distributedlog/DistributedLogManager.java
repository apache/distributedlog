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
package org.apache.distributedlog;

import com.twitter.util.Future;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.distributedlog.callback.LogSegmentListener;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.subscription.SubscriptionsStore;

/**
 * A DistributedLogManager is responsible for managing a single place of storing
 * edit logs. It may correspond to multiple files, a backup node, etc.
 * Even when the actual underlying storage is rolled, or failed and restored,
 * each conceptual place of storage corresponds to exactly one instance of
 * this class, which is created when the EditLog is first opened.
 */
public interface DistributedLogManager extends Closeable {

    /**
     * Get the name of the stream managed by this log manager.
     *
     * @return streamName
     */
    String getStreamName();

    /**
     * Get the namespace driver used by this manager.
     *
     * @return the namespace driver
     */
    NamespaceDriver getNamespaceDriver();

    /**
     * Get log segments.
     *
     * @return log segments
     * @throws IOException
     */
    List<LogSegmentMetadata> getLogSegments() throws IOException;

    /**
     * Register <i>listener</i> on log segment updates of this stream.
     *
     * @param listener
     *          listener to receive update log segment list.
     */
    void registerListener(LogSegmentListener listener) throws IOException;

    /**
     * Unregister <i>listener</i> on log segment updates from this stream.
     *
     * @param listener
     *          listener to receive update log segment list.
     */
    void unregisterListener(LogSegmentListener listener);

    /**
     * Open async log writer to write records to the log stream.
     *
     * @return result represents the open result
     */
    Future<AsyncLogWriter> openAsyncLogWriter();

    /**
     * Begin writing to the log stream identified by the name.
     *
     * @return the writer interface to generate log records
     */
    LogWriter startLogSegmentNonPartitioned() throws IOException;

    /**
     * Begin writing to the log stream identified by the name.
     *
     * @return the writer interface to generate log records
     */
    // @Deprecated
    AsyncLogWriter startAsyncLogSegmentNonPartitioned() throws IOException;

    /**
     * Begin appending to the end of the log stream which is being treated as a sequence of bytes.
     *
     * @return the writer interface to generate log records
     */
    AppendOnlyStreamWriter getAppendOnlyStreamWriter() throws IOException;

    /**
     * Get a reader to read a log stream as a sequence of bytes.
     *
     * @return the writer interface to generate log records
     */
    AppendOnlyStreamReader getAppendOnlyStreamReader() throws IOException;

    /**
     * Get the input stream starting with fromTxnId for the specified log.
     *
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
    LogReader getInputStream(long fromTxnId)
        throws IOException;

    LogReader getInputStream(DLSN fromDLSN) throws IOException;

    /**
     * Open an async log reader to read records from a log starting from <code>fromTxnId</code>.
     *
     * @param fromTxnId
     *          transaction id to start reading from
     * @return async log reader
     */
    Future<AsyncLogReader> openAsyncLogReader(long fromTxnId);

    /**
     * Open an async log reader to read records from a log starting from <code>fromDLSN</code>.
     *
     * @param fromDLSN
     *          dlsn to start reading from
     * @return async log reader
     */
    Future<AsyncLogReader> openAsyncLogReader(DLSN fromDLSN);

    // @Deprecated
    AsyncLogReader getAsyncLogReader(long fromTxnId) throws IOException;

    // @Deprecated
    AsyncLogReader getAsyncLogReader(DLSN fromDLSN) throws IOException;

    Future<AsyncLogReader> getAsyncLogReaderWithLock(DLSN fromDLSN);

    /**
     * Get a log reader with lock starting from <i>fromDLSN</i> and using <i>subscriberId</i>.
     * If two readers tried to open using same subscriberId, one would succeed, while the other
     * will be blocked until it gets the lock.
     *
     * @param fromDLSN
     *          start dlsn
     * @param subscriberId
     *          subscriber id
     * @return async log reader
     */
    Future<AsyncLogReader> getAsyncLogReaderWithLock(DLSN fromDLSN, String subscriberId);

    /**
     * Get a log reader using <i>subscriberId</i> with lock. The reader will start reading from
     * its last commit position recorded in subscription store. If no last commit position found
     * in subscription store, it would start reading from head of the stream.
     *
     * <p>If the two readers tried to open using same subscriberId, one would succeed, while the other
     * will be blocked until it gets the lock.
     *
     * @param subscriberId
     *          subscriber id
     * @return async log reader
     */
    Future<AsyncLogReader> getAsyncLogReaderWithLock(String subscriberId);

    /**
     * Get the {@link DLSN} of first log record whose transaction id is not less than <code>transactionId</code>.
     *
     * @param transactionId
     *          transaction id
     * @return dlsn of first log record whose transaction id is not less than transactionId.
     */
    Future<DLSN> getDLSNNotLessThanTxId(long transactionId);

    /**
     * Get the last log record in the stream.
     *
     * @return the last log record in the stream
     * @throws IOException if a stream cannot be found.
     */
    LogRecordWithDLSN getLastLogRecord()
        throws IOException;

    /**
     * Get the earliest Transaction Id available in the log.
     *
     * @return earliest transaction id
     * @throws IOException
     */
    long getFirstTxId() throws IOException;

    /**
     * Get Latest Transaction Id in the log.
     *
     * @return latest transaction id
     * @throws IOException
     */
    long getLastTxId() throws IOException;

    /**
     * Get Latest DLSN in the log.
     *
     * @return last dlsn
     * @throws IOException
     */
    DLSN getLastDLSN() throws IOException;

    /**
     * Get Latest log record with DLSN in the log - async.
     *
     * @return latest log record with DLSN
     */
    Future<LogRecordWithDLSN> getLastLogRecordAsync();

    /**
     * Get Latest Transaction Id in the log - async.
     *
     * @return latest transaction id
     */
    Future<Long> getLastTxIdAsync();

    /**
     * Get first DLSN in the log.
     *
     * @return first dlsn in the stream
     */
    Future<DLSN> getFirstDLSNAsync();

    /**
     * Get Latest DLSN in the log - async.
     *
     * @return latest transaction id
     */
    Future<DLSN> getLastDLSNAsync();

    /**
     * Get the number of log records in the active portion of the log.
     *
     * <p>Any log segments that have already been truncated will not be included.
     *
     * @return number of log records
     * @throws IOException
     */
    long getLogRecordCount() throws IOException;

    /**
     * Get the number of log records in the active portion of the log - async.
     *
     * <p>Any log segments that have already been truncated will not be included
     *
     * @return future number of log records
     * @throws IOException
     */
    Future<Long> getLogRecordCountAsync(final DLSN beginDLSN);

    /**
     * Run recovery on the log.
     *
     * @throws IOException
     */
    void recover() throws IOException;

    /**
     * Check if an end of stream marker was added to the stream
     * A stream with an end of stream marker cannot be appended to.
     *
     * @return true if the marker was added to the stream, false otherwise
     * @throws IOException
     */
    boolean isEndOfStreamMarked() throws IOException;

    /**
     * Delete the log.
     *
     * @throws IOException if the deletion fails
     */
    void delete() throws IOException;

    /**
     * The DistributedLogManager may archive/purge any logs for transactionId
     * less than or equal to minImageTxId.
     * This is to be used only when the client explicitly manages deletion. If
     * the cleanup policy is based on sliding time window, then this method need
     * not be called.
     *
     * @param minTxIdToKeep the earliest txid that must be retained
     * @throws IOException if purging fails
     */
    void purgeLogsOlderThan(long minTxIdToKeep) throws IOException;

    /**
     * Get the subscriptions store provided by the distributedlog manager.
     *
     * @return subscriptions store manages subscriptions for current stream.
     */
    SubscriptionsStore getSubscriptionsStore();

    /**
     * Closes this source and releases any system resources associated
     * with it. If the source is already closed then invoking this
     * method has no effect.
     *
     * @return future representing the close result.
     */
    Future<Void> asyncClose();

}
