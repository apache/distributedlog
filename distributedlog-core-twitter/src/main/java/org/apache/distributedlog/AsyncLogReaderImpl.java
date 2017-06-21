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

import com.google.common.annotations.VisibleForTesting;
import com.twitter.util.Future;
import java.util.List;
import java.util.concurrent.TimeUnit;
import scala.Function1;
import scala.runtime.AbstractFunction1;

/**
 * Implementation wrapper of {@link org.apache.distributedlog.api.AsyncLogReader}.
 */
class AsyncLogReaderImpl implements AsyncLogReader {

    static final Function1<org.apache.distributedlog.api.AsyncLogReader, AsyncLogReader> MAP_FUNC =
        new AbstractFunction1<org.apache.distributedlog.api.AsyncLogReader, AsyncLogReader>() {
            @Override
            public AsyncLogReader apply(org.apache.distributedlog.api.AsyncLogReader reader) {
                return new AsyncLogReaderImpl(reader);
            }
        };

    private final org.apache.distributedlog.api.AsyncLogReader impl;

    AsyncLogReaderImpl(org.apache.distributedlog.api.AsyncLogReader impl) {
        this.impl = impl;
    }

    @VisibleForTesting
    org.apache.distributedlog.api.AsyncLogReader getImpl() {
        return impl;
    }

    @Override
    public String getStreamName() {
        return impl.getStreamName();
    }

    @Override
    public Future<LogRecordWithDLSN> readNext() {
        return newTFuture(impl.readNext());
    }

    @Override
    public Future<List<LogRecordWithDLSN>> readBulk(int numEntries) {
        return newTFuture(impl.readBulk(numEntries));
    }

    @Override
    public Future<List<LogRecordWithDLSN>> readBulk(int numEntries, long waitTime, TimeUnit timeUnit) {
        return newTFuture(impl.readBulk(numEntries, waitTime, timeUnit));
    }

    @Override
    public Future<Void> asyncClose() {
        return newTFuture(impl.asyncClose());
    }
}
