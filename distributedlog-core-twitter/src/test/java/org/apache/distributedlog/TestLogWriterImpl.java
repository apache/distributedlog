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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.junit.Test;

/**
 * Unit test of {@link LogWriterImpl}.
 */
public class TestLogWriterImpl {

    private final org.apache.distributedlog.api.LogWriter underlying =
        mock(org.apache.distributedlog.api.LogWriter.class);
    private final LogWriterImpl writer = new LogWriterImpl(underlying);

    @Test
    public void testWrite() throws Exception {
        LogRecord record = mock(LogRecord.class);
        writer.write(record);
        verify(underlying, times(1)).write(eq(record));
    }

    @Test
    public void testWriteBulk() throws Exception {
        List<LogRecord> records = mock(List.class);
        writer.writeBulk(records);
        verify(underlying, times(1)).writeBulk(eq(records));
    }

    @Test
    public void testSetReadyToFlush() throws Exception {
        writer.setReadyToFlush();
        verify(underlying, times(1)).setReadyToFlush();
    }

    @Test
    public void testFlushAndSync() throws Exception {
        writer.flushAndSync();
        verify(underlying, times(1)).flushAndSync();
    }

    @Test
    public void testMarkEndOfStream() throws Exception {
        writer.markEndOfStream();
        verify(underlying, times(1)).markEndOfStream();
    }

    @Test
    public void testClose() throws Exception {
        writer.close();
        verify(underlying, times(1)).close();
    }

    @Test
    public void testAbort() throws Exception {
        writer.abort();
        verify(underlying, times(1)).abort();
    }

}
