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

import java.net.URI;
import org.apache.distributedlog.AsyncLogWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogManager;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.namespace.DistributedLogNamespace;
import org.apache.distributedlog.namespace.DistributedLogNamespaceBuilder;
import org.apache.distributedlog.util.FutureUtils;
import org.apache.distributedlog.util.Utils;

/**
 * A test program to write records.
 */
public class WriterTest {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("WriterTest <uri> <stream> <num_records>");
            return;
        }

        URI uri = URI.create(args[0]);
        String streamName = args[1];
        int numRecords = Integer.parseInt(args[2]);

        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setOutputBufferSize(0)
            .setPeriodicFlushFrequencyMilliSeconds(2);

        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
            .uri(uri)
            .conf(conf)
            .build();
        try {
            try (DistributedLogManager manager = namespace.openLog(streamName)) {
                AsyncLogWriter writer = FutureUtils.result(manager.openAsyncLogWriter());
                try {
                    long txid = writer.getLastTxId();
                    if (txid < 0L) {
                        txid = 0L;
                    }

                    System.out.println("Publishing " + numRecords + " records to stream " + streamName + " .");

                    for (int i = 1; i <= numRecords; ++i) {
                        String content = "record-" + (txid + i);
                        LogRecord record = new LogRecord(txid + i, content.getBytes(UTF_8));
                        FutureUtils.result(writer.write(record));
                        System.out.println("Write record : " + content);
                    }

                    System.out.println("Successfully published " + numRecords + " records to stream " + streamName + " .");
                } finally {
                    Utils.close(writer);
                }
            }
        } finally {
            namespace.close();
        }
    }

}
