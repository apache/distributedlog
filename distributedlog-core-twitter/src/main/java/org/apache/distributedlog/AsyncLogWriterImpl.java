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
import static org.apache.distributedlog.util.FutureUtils.newTFutureList;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.util.Future;
import java.util.List;
import scala.Function1;
import scala.runtime.AbstractFunction1;

/**
 * The implementation of {@link AsyncLogWriter} built over {@link org.apache.distributedlog.api.AsyncLogWriter}.
 */
class AsyncLogWriterImpl implements AsyncLogWriter {

    static final Function1<org.apache.distributedlog.api.AsyncLogWriter, AsyncLogWriter> MAP_FUNC =
        new AbstractFunction1<org.apache.distributedlog.api.AsyncLogWriter, AsyncLogWriter>() {
            @Override
            public AsyncLogWriter apply(org.apache.distributedlog.api.AsyncLogWriter writer) {
                return new AsyncLogWriterImpl(writer);
            }
        };

    private final org.apache.distributedlog.api.AsyncLogWriter impl;

    AsyncLogWriterImpl(org.apache.distributedlog.api.AsyncLogWriter impl) {
        this.impl = impl;
    }

    @VisibleForTesting
    org.apache.distributedlog.api.AsyncLogWriter getImpl() {
        return impl;
    }

    @Override
    public long getLastTxId() {
        return impl.getLastTxId();
    }

    @Override
    public Future<DLSN> write(LogRecord record) {
        return newTFuture(impl.write(record));
    }

    @Override
    public Future<List<Future<DLSN>>> writeBulk(List<LogRecord> record) {
        return newTFutureList(impl.writeBulk(record));
    }

    @Override
    public Future<Boolean> truncate(DLSN dlsn) {
        return newTFuture(impl.truncate(dlsn));
    }

    @Override
    public String getStreamName() {
        return impl.getStreamName();
    }

    @Override
    public Future<Void> asyncClose() {
        return newTFuture(impl.asyncClose());
    }

    @Override
    public Future<Void> asyncAbort() {
        return newTFuture(impl.asyncAbort());
    }
}
