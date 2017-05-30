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

import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.OverCapacityException;
import org.apache.distributedlog.limiter.ChainedRequestLimiter;
import org.apache.distributedlog.limiter.ComposableRequestLimiter.OverlimitFunction;
import org.apache.distributedlog.limiter.RequestLimiter;
import org.apache.distributedlog.service.stream.StreamOp;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A dynamic request limiter on limiting stream operations.
 */
public class StreamRequestLimiter extends DynamicRequestLimiter<StreamOp> {
    private final DynamicDistributedLogConfiguration dynConf;
    private final StatsLogger limiterStatLogger;
    private final String streamName;

    public StreamRequestLimiter(String streamName,
                                DynamicDistributedLogConfiguration dynConf,
                                StatsLogger statsLogger,
                                Feature disabledFeature) {
        super(dynConf, statsLogger, disabledFeature);
        this.limiterStatLogger = statsLogger;
        this.dynConf = dynConf;
        this.streamName = streamName;
        this.limiter = build();
    }

    @Override
    public RequestLimiter<StreamOp> build() {

        // RPS hard, soft limits
        RequestLimiterBuilder rpsHardLimiterBuilder = RequestLimiterBuilder.newRpsLimiterBuilder()
            .statsLogger(limiterStatLogger.scope("rps_hard_limit"))
            .limit(dynConf.getRpsHardWriteLimit())
            .overlimit(new OverlimitFunction<StreamOp>() {
                @Override
                public void apply(StreamOp op) throws OverCapacityException {
                    throw new OverCapacityException("Being rate limited: RPS limit exceeded for stream " + streamName);
                }
            });
        RequestLimiterBuilder rpsSoftLimiterBuilder = RequestLimiterBuilder.newRpsLimiterBuilder()
            .statsLogger(limiterStatLogger.scope("rps_soft_limit"))
            .limit(dynConf.getRpsSoftWriteLimit());

        // BPS hard, soft limits
        RequestLimiterBuilder bpsHardLimiterBuilder = RequestLimiterBuilder.newBpsLimiterBuilder()
            .statsLogger(limiterStatLogger.scope("bps_hard_limit"))
            .limit(dynConf.getBpsHardWriteLimit())
            .overlimit(new OverlimitFunction<StreamOp>() {
                @Override
                public void apply(StreamOp op) throws OverCapacityException {
                    throw new OverCapacityException("Being rate limited: BPS limit exceeded for stream " + streamName);
                }
            });
        RequestLimiterBuilder bpsSoftLimiterBuilder = RequestLimiterBuilder.newBpsLimiterBuilder()
            .statsLogger(limiterStatLogger.scope("bps_soft_limit"))
            .limit(dynConf.getBpsSoftWriteLimit());

        ChainedRequestLimiter.Builder<StreamOp> builder = new ChainedRequestLimiter.Builder<StreamOp>();
        builder.addLimiter(rpsSoftLimiterBuilder.build());
        builder.addLimiter(rpsHardLimiterBuilder.build());
        builder.addLimiter(bpsSoftLimiterBuilder.build());
        builder.addLimiter(bpsHardLimiterBuilder.build());
        builder.statsLogger(limiterStatLogger);
        return builder.build();
    }
}
