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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;

/**
 * The wrapper of {@link org.apache.distributedlog.api.LogWriter}.
 */
class LogWriterImpl implements LogWriter {

    private final org.apache.distributedlog.api.LogWriter impl;

    LogWriterImpl(org.apache.distributedlog.api.LogWriter impl) {
        this.impl = impl;
    }

    @VisibleForTesting
    org.apache.distributedlog.api.LogWriter getImpl() {
        return impl;
    }

    @Override
    public void write(LogRecord record) throws IOException {
        impl.write(record);
    }

    @Override
    public int writeBulk(List<LogRecord> records) throws IOException {
        return impl.writeBulk(records);
    }

    @Override
    public long setReadyToFlush() throws IOException {
        return impl.flush();
    }

    @Override
    public long flushAndSync() throws IOException {
        return impl.commit();
    }

    @Override
    public void markEndOfStream() throws IOException {
        impl.markEndOfStream();
    }

    @Override
    public void close() throws IOException {
        impl.close();
    }

    @Override
    public void abort() throws IOException {
        impl.abort();
    }
}
