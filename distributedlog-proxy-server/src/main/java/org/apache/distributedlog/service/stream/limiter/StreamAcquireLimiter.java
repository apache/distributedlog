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
package org.apache.distributedlog.service.stream.limiter;

import org.apache.distributedlog.exceptions.OverCapacityException;
import org.apache.distributedlog.exceptions.TooManyStreamsException;
import org.apache.distributedlog.limiter.RequestLimiter;
import org.apache.distributedlog.rate.MovingAverageRate;
import org.apache.distributedlog.service.stream.StreamManager;
import org.apache.distributedlog.service.stream.StreamOp;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A special limiter on limiting acquiring new streams.
 */
public class StreamAcquireLimiter implements RequestLimiter<StreamOp> {
    private final StreamManager streamManager;
    private final MovingAverageRate serviceRps;
    private final double serviceRpsLimit;
    private final Counter overlimitCounter;

    public StreamAcquireLimiter(StreamManager streamManager,
                                MovingAverageRate serviceRps,
                                double serviceRpsLimit,
                                StatsLogger statsLogger) {
        this.streamManager = streamManager;
        this.serviceRps = serviceRps;
        this.serviceRpsLimit = serviceRpsLimit;
        this.overlimitCounter = statsLogger.getCounter("overlimit");
    }

    @Override
    public void apply(StreamOp op) throws OverCapacityException {
        String streamName = op.streamName();
        if (serviceRpsLimit > -1 && serviceRps.get() > serviceRpsLimit && !streamManager.isAcquired(streamName)) {
            overlimitCounter.inc();
            throw new TooManyStreamsException("Request rate is too high to accept new stream " + streamName + ".");
        }
    }
}
