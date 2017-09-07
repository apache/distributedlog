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

package org.apache.distributedlog.tests.backward;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;

import java.net.URI;
import org.apache.distributedlog.AsyncLogReader;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogManager;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.namespace.DistributedLogNamespace;
import org.apache.distributedlog.namespace.DistributedLogNamespaceBuilder;
import org.apache.distributedlog.util.FutureUtils;
import org.apache.distributedlog.util.Utils;

/**
 * A test program to read records.
 */
public class ReaderTest {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("ReaderTest <uri> <stream> <num_records> <start_tx_id>");
            return;
        }

        URI uri = URI.create(args[0]);
        String streamName = args[1];
        int numRecords = Integer.parseInt(args[2]);
        final long startTxId = Long.parseLong(args[3]);

        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setOutputBufferSize(0)
            .setPeriodicFlushFrequencyMilliSeconds(2);

        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
            .uri(uri)
            .conf(conf)
            .build();
        try {
            try (DistributedLogManager manager = namespace.openLog(streamName)) {
                AsyncLogReader reader = FutureUtils.result(manager.openAsyncLogReader(startTxId));
                try {
                    System.out.println("Try to read " + numRecords + " records from stream " + streamName + " .");
                    for (int i = 0; i < numRecords; ++i) {
                        LogRecord record = FutureUtils.result(reader.readNext());
                        String data = new String(record.getPayload(), UTF_8);

                        System.out.println("Read record : " + data);

                        String expectedData = "record-" + (startTxId + i);
                        checkArgument(expectedData.equals(data),
                            "Expected = " + expectedData + ", Actual = " + data);
                        long expectedTxId = startTxId + i;
                        checkArgument(expectedTxId == record.getTransactionId(),
                            "Expected TxId = " + expectedTxId + ", Actual TxId = " + record.getTransactionId());
                    }
                    System.out.println("Successfully read " + numRecords + " records to stream " + streamName + " .");
                } finally {
                    Utils.close(reader);
                }
            }
        } finally {
            namespace.close();
        }
    }

}
