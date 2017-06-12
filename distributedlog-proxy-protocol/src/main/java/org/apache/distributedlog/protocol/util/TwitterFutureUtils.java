/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.protocol.util;

import com.google.common.collect.Lists;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.Return;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.distributedlog.common.util.FutureUtils;

/**
 * Utils for Twitter's {@link com.twitter.util.Future}.
 */
public final class TwitterFutureUtils {

    private TwitterFutureUtils() {}

    public static <T> CompletableFuture<T> newJFuture(Promise<T> promise) {
        CompletableFuture<T> jFuture = FutureUtils.createFuture();
        jFuture.whenComplete((value, cause) -> {
            if (null != cause) {
                if (cause instanceof CompletionException) {
                    promise.setException(cause.getCause());
                } else {
                    promise.setException(cause);
                }
            } else {
                promise.setValue(value);
            }
        });
        return jFuture;
    }

    public static <T> Future<T> newTFuture(CompletableFuture<T> jFuture) {
        Promise<T> promise = new Promise<>();
        jFuture.whenComplete((value, cause) -> {
            if (null != cause) {
                if (cause instanceof CompletionException) {
                    promise.setException(cause.getCause());
                } else {
                    promise.setException(cause);
                }
            } else {
                promise.setValue(value);
            }
        });
        return promise;
    }

    public static <T> Future<List<Future<T>>> newTFutureList(
            CompletableFuture<List<CompletableFuture<T>>> jFutureList) {
        Promise<List<Future<T>>> promise = new Promise<>();
        jFutureList.whenComplete((value, cause) -> {
            if (null != cause) {
                if (cause instanceof CompletionException) {
                    promise.setException(cause.getCause());
                } else {
                    promise.setException(cause);
                }
            } else {
                promise.setValue(Lists.transform(
                    value,
                    future -> newTFuture(future)));
            }
        });
        return promise;
    }

    public static <T> void setValue(Promise<T> promise, T value) {
        promise.updateIfEmpty(new Return<T>(value));
    }

}
