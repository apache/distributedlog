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
package org.apache.distributedlog.service.stream;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.distributedlog.protocol.util.TwitterFutureUtils.newTFuture;

import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.acl.AccessControlManager;
import org.apache.distributedlog.exceptions.DLException;
import org.apache.distributedlog.exceptions.RequestDeniedException;
import org.apache.distributedlog.service.ResponseUtils;
import org.apache.distributedlog.thrift.service.WriteResponse;
import org.apache.distributedlog.common.util.Sequencer;
import com.twitter.util.Future;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.runtime.AbstractFunction1;

/**
 * Heartbeat Operation.
 */
public class HeartbeatOp extends AbstractWriteOp {

    static final byte[] HEARTBEAT_DATA = "heartbeat".getBytes(UTF_8);

    private final AccessControlManager accessControlManager;
    private final Counter deniedHeartbeatCounter;
    private final byte dlsnVersion;

    private boolean writeControlRecord = false;

    public HeartbeatOp(String stream,
                       StatsLogger statsLogger,
                       StatsLogger perStreamStatsLogger,
                       byte dlsnVersion,
                       Long checksum,
                       Feature checksumDisabledFeature,
                       AccessControlManager accessControlManager) {
        super(stream, requestStat(statsLogger, "heartbeat"), checksum, checksumDisabledFeature);
        StreamOpStats streamOpStats = new StreamOpStats(statsLogger, perStreamStatsLogger);
        this.deniedHeartbeatCounter = streamOpStats.requestDeniedCounter("heartbeat");
        this.dlsnVersion = dlsnVersion;
        this.accessControlManager = accessControlManager;
    }

    public HeartbeatOp setWriteControlRecord(boolean writeControlRecord) {
        this.writeControlRecord = writeControlRecord;
        return this;
    }

    @Override
    protected Future<WriteResponse> executeOp(AsyncLogWriter writer,
                                              Sequencer sequencer,
                                              Object txnLock) {
        // write a control record if heartbeat is the first request of the recovered log segment.
        if (writeControlRecord) {
            long txnId;
            Future<DLSN> writeResult;
            synchronized (txnLock) {
                txnId = sequencer.nextId();
                LogRecord hbRecord = new LogRecord(txnId, HEARTBEAT_DATA);
                hbRecord.setControl();
                writeResult = newTFuture(writer.write(hbRecord));
            }
            return writeResult.map(new AbstractFunction1<DLSN, WriteResponse>() {
                @Override
                public WriteResponse apply(DLSN value) {
                    return ResponseUtils.writeSuccess().setDlsn(value.serialize(dlsnVersion));
                }
            });
        } else {
            return Future.value(ResponseUtils.writeSuccess());
        }
    }

    @Override
    public void preExecute() throws DLException {
        if (!accessControlManager.allowAcquire(stream)) {
            deniedHeartbeatCounter.inc();
            throw new RequestDeniedException(stream, "heartbeat");
        }
        super.preExecute();
    }
}
