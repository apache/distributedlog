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
package org.apache.distributedlog.basic;

import static com.google.common.base.Charsets.UTF_8;

import com.google.common.collect.Lists;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Await;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordSet;
import org.apache.distributedlog.io.CompressionCodec.Type;
import org.apache.distributedlog.service.DistributedLogClient;
import org.apache.distributedlog.service.DistributedLogClientBuilder;
import org.apache.distributedlog.util.FutureEventListener;
import org.apache.distributedlog.util.FutureUtils;

/**
 * Write multiple record atomically
 */
public class AtomicWriter {

    private final static String HELP = "AtomicWriter <finagle-name> <stream> <message>[,<message>]";

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println(HELP);
            return;
        }

        String finagleNameStr = args[0];
        String streamName = args[1];
        String[] messages = new String[args.length - 2];
        System.arraycopy(args, 2, messages, 0, messages.length);

        DistributedLogClient client = DistributedLogClientBuilder.newBuilder()
                .clientId(ClientId$.MODULE$.apply("atomic-writer"))
                .name("atomic-writer")
                .thriftmux(true)
                .finagleNameStr(finagleNameStr)
                .build();

        final LogRecordSet.Writer recordSetWriter = LogRecordSet.newWriter(16 * 1024, Type.NONE);
        List<CompletableFuture<DLSN>> writeFutures = Lists.newArrayListWithExpectedSize(messages.length);
        for (String msg : messages) {
            final String message = msg;
            ByteBuffer msgBuf = ByteBuffer.wrap(msg.getBytes(UTF_8));
            CompletableFuture<DLSN> writeFuture = FutureUtils.createFuture();
            writeFuture.whenComplete(new FutureEventListener<DLSN>() {
                @Override
                public void onFailure(Throwable cause) {
                    System.out.println("Encountered error on writing data");
                    cause.printStackTrace(System.err);
                    Runtime.getRuntime().exit(0);
                }

                @Override
                public void onSuccess(DLSN dlsn) {
                    System.out.println("Write '" + message + "' as record " + dlsn);
                }
            });
            recordSetWriter.writeRecord(msgBuf, writeFuture);
            writeFutures.add(writeFuture);
        }
        Await.result(
            client.writeRecordSet(streamName, recordSetWriter)
                .addEventListener(new com.twitter.util.FutureEventListener<DLSN>() {
                    @Override
                    public void onFailure(Throwable cause) {
                        recordSetWriter.abortTransmit(cause);
                        System.out.println("Encountered error on writing data");
                        cause.printStackTrace(System.err);
                        Runtime.getRuntime().exit(0);
                    }

                    @Override
                    public void onSuccess(DLSN dlsn) {
                        recordSetWriter.completeTransmit(
                                dlsn.getLogSegmentSequenceNo(),
                                dlsn.getEntryId(),
                                dlsn.getSlotId());
                    }
                })
        );
        FutureUtils.result(FutureUtils.collect(writeFutures));
        client.close();
    }
}
