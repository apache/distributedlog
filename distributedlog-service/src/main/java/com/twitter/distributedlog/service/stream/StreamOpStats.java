/**
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
package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.stats.BroadCastStatsLogger;
import com.twitter.distributedlog.service.streamset.Partition;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Encapsulate stream op stats construction to make it easier to access stream
 * op stats consistently from different scopes.
 */
public class StreamOpStats {
    private final StatsLogger baseStatsLogger;
    private final StatsLogger requestStatsLogger;
    private final StatsLogger recordsStatsLogger;
    private final StatsLogger requestDeniedStatsLogger;
    private final StatsLogger streamStatsLogger;

    public StreamOpStats(StatsLogger statsLogger,
                         StatsLogger perStreamStatsLogger) {
        this.baseStatsLogger = statsLogger;
        this.requestStatsLogger = statsLogger.scope("request");
        this.recordsStatsLogger = statsLogger.scope("records");
        this.requestDeniedStatsLogger = statsLogger.scope("denied");
        this.streamStatsLogger = perStreamStatsLogger;
    }

    public StatsLogger baseStatsLogger(String opName) {
        return baseStatsLogger;
    }

    public Counter baseCounter(String opName) {
        return baseStatsLogger.getCounter(opName);
    }

    public StatsLogger baseScope(String opName) {
        return baseStatsLogger.scope(opName);
    }

    public OpStatsLogger requestLatencyStat(String opName) {
        return requestStatsLogger.getOpStatsLogger(opName);
    }

    public StatsLogger requestScope(String scopeName) {
        return requestStatsLogger.scope(scopeName);
    }

    public Counter scopedRequestCounter(String opName, String counterName) {
        return requestScope(opName).getCounter(counterName);
    }

    public Counter requestCounter(String counterName) {
        return requestStatsLogger.getCounter(counterName);
    }

    public Counter requestPendingCounter(String counterName) {
        return requestCounter(counterName);
    }

    public Counter requestDeniedCounter(String counterName) {
        return requestDeniedStatsLogger.getCounter(counterName);
    }

    public Counter recordsCounter(String counterName) {
        return recordsStatsLogger.getCounter(counterName);
    }

    public StatsLogger streamRequestStatsLogger(Partition partition) {
        return BroadCastStatsLogger.masterslave(
            streamStatsLogger.scope(partition.getStream()).scope("partition")
                .scope(partition.getPaddedId()), streamStatsLogger.scope(partition.getStream())
                .scope("aggregate"));
    }

    public StatsLogger streamRequestScope(Partition partition, String scopeName) {
        return streamRequestStatsLogger(partition).scope(scopeName);
    }

    public OpStatsLogger streamRequestLatencyStat(Partition partition, String opName) {
        return streamRequestStatsLogger(partition).getOpStatsLogger(opName);
    }

    public Counter streamRequestCounter(Partition partition, String opName, String counterName) {
        return streamRequestScope(partition, opName).getCounter(counterName);
    }
}
