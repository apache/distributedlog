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
package org.apache.distributedlog.service.stream.admin;

import com.google.common.base.Stopwatch;
import org.apache.distributedlog.exceptions.ChecksumFailedException;
import org.apache.distributedlog.exceptions.DLException;
import org.apache.distributedlog.protocol.util.ProtocolUtils;
import org.apache.distributedlog.service.ResponseUtils;
import org.apache.distributedlog.service.stream.StreamManager;
import org.apache.distributedlog.thrift.service.WriteResponse;
import com.twitter.util.Future;
import com.twitter.util.FutureTransformer;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * Stream admin op.
 */
public abstract class StreamAdminOp implements AdminOp<WriteResponse> {

    protected final String stream;
    protected final StreamManager streamManager;
    protected final OpStatsLogger opStatsLogger;
    protected final Stopwatch stopwatch = Stopwatch.createUnstarted();
    protected final Long checksum;
    protected final Feature checksumDisabledFeature;

    protected StreamAdminOp(String stream,
                            StreamManager streamManager,
                            OpStatsLogger statsLogger,
                            Long checksum,
                            Feature checksumDisabledFeature) {
        this.stream = stream;
        this.streamManager = streamManager;
        this.opStatsLogger = statsLogger;
        // start here in case the operation is failed before executing.
        stopwatch.reset().start();
        this.checksum = checksum;
        this.checksumDisabledFeature = checksumDisabledFeature;
    }

    protected Long computeChecksum() {
        return ProtocolUtils.streamOpCRC32(stream);
    }

    @Override
    public void preExecute() throws DLException {
        if (!checksumDisabledFeature.isAvailable() && null != checksum) {
            Long serverChecksum = computeChecksum();
            if (null != serverChecksum && !checksum.equals(serverChecksum)) {
                throw new ChecksumFailedException();
            }
        }
    }

    /**
     * Execute the operation.
     *
     * @return execute operation
     */
    protected abstract Future<WriteResponse> executeOp();

    @Override
    public Future<WriteResponse> execute() {
        return executeOp().transformedBy(new FutureTransformer<WriteResponse, WriteResponse>() {

            @Override
            public WriteResponse map(WriteResponse response) {
                opStatsLogger.registerSuccessfulEvent(
                        stopwatch.elapsed(TimeUnit.MICROSECONDS));
                return response;
            }

            @Override
            public WriteResponse handle(Throwable cause) {
                opStatsLogger.registerFailedEvent(
                        stopwatch.elapsed(TimeUnit.MICROSECONDS));
                return ResponseUtils.write(ResponseUtils.exceptionToHeader(cause));
            }

        });
    }
}
