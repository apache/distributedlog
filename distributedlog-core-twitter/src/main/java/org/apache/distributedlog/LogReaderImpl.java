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
import java.io.IOException;
import java.util.List;

/**
 * The wrapper over {@link org.apache.distributedlog.api.LogReader}.
 */
class LogReaderImpl implements LogReader {

    private final org.apache.distributedlog.api.LogReader reader;

    LogReaderImpl(org.apache.distributedlog.api.LogReader reader) {
        this.reader = reader;
    }

    @VisibleForTesting
    org.apache.distributedlog.api.LogReader getImpl() {
        return reader;
    }

    @Override
    public LogRecordWithDLSN readNext(boolean nonBlocking) throws IOException {
        return reader.readNext(nonBlocking);
    }

    @Override
    public List<LogRecordWithDLSN> readBulk(boolean nonBlocking, int numLogRecords) throws IOException {
        return reader.readBulk(nonBlocking, numLogRecords);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public Future<Void> asyncClose() {
        return newTFuture(reader.asyncClose());
    }
}
