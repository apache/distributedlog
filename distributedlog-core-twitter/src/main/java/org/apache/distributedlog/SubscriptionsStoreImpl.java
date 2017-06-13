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
import java.util.Map;
import org.apache.distributedlog.subscription.SubscriptionsStore;

/**
 * A wrapper over {@link org.apache.distributedlog.api.subscription.SubscriptionsStore}.
 */
class SubscriptionsStoreImpl implements SubscriptionsStore {

    private final org.apache.distributedlog.api.subscription.SubscriptionsStore impl;

    SubscriptionsStoreImpl(org.apache.distributedlog.api.subscription.SubscriptionsStore impl) {
        this.impl = impl;
    }

    org.apache.distributedlog.api.subscription.SubscriptionsStore getImpl() {
        return impl;
    }

    @Override
    public Future<DLSN> getLastCommitPosition(String subscriberId) {
        return newTFuture(impl.getLastCommitPosition(subscriberId));
    }

    @Override
    public Future<Map<String, DLSN>> getLastCommitPositions() {
        return newTFuture(impl.getLastCommitPositions());
    }

    @Override
    public Future<Void> advanceCommitPosition(String subscriberId, DLSN newPosition) {
        return newTFuture(impl.advanceCommitPosition(subscriberId, newPosition));
    }

    @Override
    public Future<Boolean> deleteSubscriber(String subscriberId) {
        return newTFuture(impl.deleteSubscriber(subscriberId));
    }

    @Override
    public void close() throws IOException {
        impl.close();
    }
}
