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

import com.twitter.util.Future;
import java.util.List;

/**
 * A writer that appends log records asynchronously.
 */
public interface AsyncLogWriter {

    /**
     * Get the last committed transaction id.
     *
     * @return last committed transaction id.
     */
    long getLastTxId();

    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     * @return A Future which contains a DLSN if the record was successfully written
     * or an exception if the write fails
     */
    Future<DLSN> write(LogRecord record);

    /**
     * Write log records to the stream in bulk. Each future in the list represents the result of
     * one write operation. The size of the result list is equal to the size of the input list.
     * Buffers are written in order, and the list of result futures has the same order.
     *
     * @param record set of log records
     * @return A Future which contains a list of Future DLSNs if the record was successfully written
     * or an exception if the operation fails.
     */
    Future<List<Future<DLSN>>> writeBulk(List<LogRecord> record);

    /**
     * Truncate the log until <i>dlsn</i>.
     *
     * @param dlsn
     *          dlsn to truncate until.
     * @return A Future indicates whether the operation succeeds or not, or an exception
     * if the truncation fails.
     */
    Future<Boolean> truncate(DLSN dlsn);

    /**
     * Get the name of the stream this writer writes data to.
     */
    String getStreamName();

    /**
     * Closes this source and releases any system resources associated
     * with it. If the source is already closed then invoking this
     * method has no effect.
     *
     * @return future representing the close result.
     */
    Future<Void> asyncClose();

    /**
     * Aborts the object and releases any resources associated with it.
     * If the object is already aborted then invoking this method has no
     * effect.
     *
     * @return future represents the abort result
     */
    Future<Void> asyncAbort();
}
