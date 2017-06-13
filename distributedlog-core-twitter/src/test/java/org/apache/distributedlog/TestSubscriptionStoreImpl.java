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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.api.subscription.SubscriptionsStore;
import org.apache.distributedlog.util.FutureUtils;
import org.junit.Test;

/**
 * Unit test of {@link SubscriptionsStoreImpl}.
 */
public class TestSubscriptionStoreImpl {

    private final SubscriptionsStore underlying = mock(SubscriptionsStore.class);
    private final SubscriptionsStoreImpl store = new SubscriptionsStoreImpl(underlying);

    @Test
    public void testGetLastCommitPosition() throws Exception {
        String subscriber = "test-subscriber";
        DLSN dlsn = mock(DLSN.class);
        when(underlying.getLastCommitPosition(anyString()))
            .thenReturn(CompletableFuture.completedFuture(dlsn));
        assertEquals(dlsn,
            FutureUtils.result(store.getLastCommitPosition(subscriber)));
        verify(underlying, times(1)).getLastCommitPosition(eq(subscriber));
    }

    @Test
    public void testGetLastCommitPositions() throws Exception {
        Map<String, DLSN> positions = mock(Map.class);
        when(underlying.getLastCommitPositions())
            .thenReturn(CompletableFuture.completedFuture(positions));
        assertEquals(positions, FutureUtils.result(store.getLastCommitPositions()));
        verify(underlying, times(1)).getLastCommitPositions();
    }

    @Test
    public void testAdvanceCommmitPosition() throws Exception {
        String subscriber = "test-subscriber";
        DLSN dlsn = mock(DLSN.class);
        when(underlying.advanceCommitPosition(anyString(), any(DLSN.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        FutureUtils.result(store.advanceCommitPosition(subscriber, dlsn));
        verify(underlying, times(1))
            .advanceCommitPosition(eq(subscriber), eq(dlsn));
    }

    @Test
    public void testDeleteSubscriber() throws Exception {
        String subscriber = "test-subscriber";
        when(underlying.deleteSubscriber(anyString()))
            .thenReturn(CompletableFuture.completedFuture(true));
        assertTrue(FutureUtils.result(store.deleteSubscriber(subscriber)));
        verify(underlying, times(1)).deleteSubscriber(eq(subscriber));
    }

    @Test
    public void testClose() throws Exception {
        store.close();
        verify(underlying, times(1)).close();
    }

}
