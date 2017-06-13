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
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.util.FutureUtils;
import org.junit.Test;

/**
 * Unit test of {@link LogReaderImpl}.
 */
public class TestLogReaderImpl {

    private final org.apache.distributedlog.api.LogReader underlying =
        mock(org.apache.distributedlog.api.LogReader.class);
    private final LogReaderImpl reader = new LogReaderImpl(underlying);

    @Test
    public void testReadNext() throws Exception {
        reader.readNext(false);
        verify(underlying, times(1)).readNext(eq(false));
    }

    @Test
    public void testReadBulk() throws Exception {
        reader.readBulk(false, 100);
        verify(underlying, times(1)).readBulk(eq(false), eq(100));
    }

    @Test
    public void testClose() throws Exception {
        reader.close();
        verify(underlying, times(1)).close();
    }

    @Test
    public void testAsyncClose() throws Exception {
        when(underlying.asyncClose())
            .thenReturn(CompletableFuture.completedFuture(null));
        FutureUtils.result(reader.asyncClose());
        verify(underlying, times(1)).asyncClose();
    }

}
