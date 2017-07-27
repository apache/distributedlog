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
package org.apache.distributedlog.benchmark;

import static com.google.common.base.Preconditions.checkArgument;

import com.twitter.common.zookeeper.ServerSet;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.benchmark.utils.ShiftableRateLimiter;
import org.apache.distributedlog.client.DistributedLogMultiStreamWriter;
import org.apache.distributedlog.client.serverset.DLZkServerSet;
import org.apache.distributedlog.exceptions.DLException;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.service.DistributedLogClient;
import org.apache.distributedlog.service.DistributedLogClientBuilder;
import org.apache.distributedlog.thrift.service.StatusCode;
import org.apache.distributedlog.common.util.SchedulerUtils;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Duration$;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark for distributedlog proxy client.
 */
public class WriterWorker implements Worker {

    static final Logger LOG = LoggerFactory.getLogger(WriterWorker.class);

    final String streamPrefix;
    final int startStreamId;
    final int endStreamId;
    final int writeConcurrency;
    final int messageSizeBytes;
    final int hostConnectionCoreSize;
    final int hostConnectionLimit;
    final ExecutorService executorService;
    final ShiftableRateLimiter rateLimiter;
    final URI dlUri;
    final DLZkServerSet[] serverSets;
    final List<String> finagleNames;
    final Random random;
    final List<String> streamNames;
    final int numStreams;
    final int batchSize;
    final boolean thriftmux;
    final boolean handshakeWithClientInfo;
    final int sendBufferSize;
    final int recvBufferSize;
    final boolean enableBatching;
    final int batchBufferSize;
    final int batchFlushIntervalMicros;
    private final String routingServiceFinagleName;

    volatile boolean running = true;

    final StatsReceiver statsReceiver;
    final StatsLogger statsLogger;
    final OpStatsLogger requestStat;
    final StatsLogger exceptionsLogger;
    final StatsLogger dlErrorCodeLogger;

    // callback thread
    final ExecutorService executor;

    public WriterWorker(String streamPrefix,
                        URI uri,
                        int startStreamId,
                        int endStreamId,
                        ShiftableRateLimiter rateLimiter,
                        int writeConcurrency,
                        int messageSizeBytes,
                        int batchSize,
                        int hostConnectionCoreSize,
                        int hostConnectionLimit,
                        List<String> serverSetPaths,
                        List<String> finagleNames,
                        StatsReceiver statsReceiver,
                        StatsLogger statsLogger,
                        boolean thriftmux,
                        boolean handshakeWithClientInfo,
                        int sendBufferSize,
                        int recvBufferSize,
                        boolean enableBatching,
                        int batchBufferSize,
                        int batchFlushIntervalMicros,
                        String routingServiceFinagleName) {
        checkArgument(startStreamId <= endStreamId);
        checkArgument(!finagleNames.isEmpty() || !serverSetPaths.isEmpty());
        this.streamPrefix = streamPrefix;
        this.dlUri = uri;
        this.startStreamId = startStreamId;
        this.endStreamId = endStreamId;
        this.rateLimiter = rateLimiter;
        this.writeConcurrency = writeConcurrency;
        this.messageSizeBytes = messageSizeBytes;
        this.statsReceiver = statsReceiver;
        this.statsLogger = statsLogger;
        this.requestStat = this.statsLogger.getOpStatsLogger("requests");
        this.exceptionsLogger = statsLogger.scope("exceptions");
        this.dlErrorCodeLogger = statsLogger.scope("dl_error_code");
        this.executorService = Executors.newCachedThreadPool();
        this.random = new Random(System.currentTimeMillis());
        this.batchSize = batchSize;
        this.hostConnectionCoreSize = hostConnectionCoreSize;
        this.hostConnectionLimit = hostConnectionLimit;
        this.thriftmux = thriftmux;
        this.handshakeWithClientInfo = handshakeWithClientInfo;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
        this.enableBatching = enableBatching;
        this.batchBufferSize = batchBufferSize;
        this.batchFlushIntervalMicros = batchFlushIntervalMicros;
        this.finagleNames = finagleNames;
        this.serverSets = createServerSets(serverSetPaths);
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.routingServiceFinagleName = routingServiceFinagleName;

        // Streams
        streamNames = new ArrayList<String>(endStreamId - startStreamId);
        for (int i = startStreamId; i < endStreamId; i++) {
            streamNames.add(String.format("%s_%d", streamPrefix, i));
        }
        numStreams = streamNames.size();
        LOG.info("Writing to {} streams : {}", numStreams, streamNames);
    }

    protected DLZkServerSet[] createServerSets(List<String> serverSetPaths) {
        DLZkServerSet[] serverSets = new DLZkServerSet[serverSetPaths.size()];
        for (int i = 0; i < serverSets.length; i++) {
            String serverSetPath = serverSetPaths.get(i);
            serverSets[i] = DLZkServerSet.of(URI.create(serverSetPath), 60000);
        }
        return serverSets;
    }

    @Override
    public void close() throws IOException {
        this.running = false;
        SchedulerUtils.shutdownScheduler(this.executorService, 2, TimeUnit.MINUTES);
        for (DLZkServerSet serverSet: serverSets) {
            serverSet.close();
        }
    }

    private DistributedLogClient buildDlogClient() {
        ClientBuilder clientBuilder = ClientBuilder.get()
            .hostConnectionLimit(hostConnectionLimit)
            .hostConnectionCoresize(hostConnectionCoreSize)
            .tcpConnectTimeout(Duration$.MODULE$.fromMilliseconds(200))
            .connectTimeout(Duration$.MODULE$.fromMilliseconds(200))
            .requestTimeout(Duration$.MODULE$.fromSeconds(10))
            .sendBufferSize(sendBufferSize)
            .recvBufferSize(recvBufferSize);

        ClientId clientId = ClientId$.MODULE$.apply("dlog_loadtest_writer");

        DistributedLogClientBuilder builder = DistributedLogClientBuilder.newBuilder()
            .clientId(clientId)
            .clientBuilder(clientBuilder)
            .thriftmux(thriftmux)
            .redirectBackoffStartMs(100)
            .redirectBackoffMaxMs(500)
            .requestTimeoutMs(10000)
            .statsReceiver(statsReceiver)
            .streamNameRegex("^" + streamPrefix + "_[0-9]+$")
            .handshakeWithClientInfo(handshakeWithClientInfo)
            .periodicHandshakeIntervalMs(TimeUnit.SECONDS.toMillis(30))
            .periodicOwnershipSyncIntervalMs(TimeUnit.MINUTES.toMillis(5))
            .periodicDumpOwnershipCache(true)
            .handshakeTracing(true)
            .serverRoutingServiceFinagleNameStr(routingServiceFinagleName)
            .name("writer");

        if (!finagleNames.isEmpty()) {
            String local = finagleNames.get(0);
            String[] remotes = new String[finagleNames.size() - 1];
            finagleNames.subList(1, finagleNames.size()).toArray(remotes);

            builder = builder.finagleNameStrs(local, remotes);
        } else if (serverSets.length != 0){
            ServerSet local = serverSets[0].getServerSet();
            ServerSet[] remotes = new ServerSet[serverSets.length - 1];
            for (int i = 1; i < serverSets.length; i++) {
                remotes[i - 1] = serverSets[i].getServerSet();
            }
            builder = builder.serverSets(local, remotes);
        } else {
            builder = builder.uri(dlUri);
        }

        return builder.build();
    }

    ByteBuffer buildBuffer(long requestMillis, int messageSizeBytes) {
        ByteBuffer data;
        try {
            data = ByteBuffer.wrap(Utils.generateMessage(requestMillis, messageSizeBytes));
            return data;
        } catch (TException e) {
            LOG.error("Error generating message : ", e);
            return null;
        }
    }

    List<ByteBuffer> buildBufferList(int batchSize, long requestMillis, int messageSizeBytes) {
        ArrayList<ByteBuffer> bufferList = new ArrayList<ByteBuffer>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            ByteBuffer buf = buildBuffer(requestMillis, messageSizeBytes);
            if (null == buf) {
                return null;
            }
            bufferList.add(buf);
        }
        return bufferList;
    }

    class TimedRequestHandler implements FutureEventListener<DLSN>, Runnable {
        final String streamName;
        final long requestMillis;
        DLSN dlsn = null;
        Throwable cause = null;

        TimedRequestHandler(String streamName,
                            long requestMillis) {
            this.streamName = streamName;
            this.requestMillis = requestMillis;
        }
        @Override
        public void onSuccess(DLSN value) {
            dlsn = value;
            executor.submit(this);
        }
        @Override
        public void onFailure(Throwable cause) {
            this.cause = cause;
            executor.submit(this);
        }

        @Override
        public void run() {
            if (null != dlsn) {
                requestStat.registerSuccessfulEvent(
                  System.currentTimeMillis() - requestMillis, TimeUnit.MILLISECONDS);
            } else {
                LOG.error("Failed to publish to {} : ", streamName, cause);
                requestStat.registerFailedEvent(
                  System.currentTimeMillis() - requestMillis, TimeUnit.MILLISECONDS);
                exceptionsLogger.getCounter(cause.getClass().getName()).inc();
                if (cause instanceof DLException) {
                    DLException dle = (DLException) cause;
                    dlErrorCodeLogger.getCounter(StatusCode.findByValue(dle.getCode()).toString()).inc();
                }
            }
        }
    }

    class Writer implements Runnable {

        final int idx;
        final DistributedLogClient dlc;
        DistributedLogMultiStreamWriter writer = null;
        final ShiftableRateLimiter limiter;

        Writer(int idx) {
            this.idx = idx;
            this.dlc = buildDlogClient();
            if (enableBatching) {
                writer = DistributedLogMultiStreamWriter.newBuilder()
                        .client(this.dlc)
                        .streams(streamNames)
                        .compressionCodec(CompressionCodec.Type.NONE)
                        .flushIntervalMicros(batchFlushIntervalMicros)
                        .bufferSize(batchBufferSize)
                        .firstSpeculativeTimeoutMs(9000)
                        .maxSpeculativeTimeoutMs(9000)
                        .requestTimeoutMs(10000)
                        .speculativeBackoffMultiplier(2)
                        .build();
            }
            this.limiter = rateLimiter.duplicate();
        }

        @Override
        public void run() {
            LOG.info("Started writer {}.", idx);
            while (running) {
                this.limiter.getLimiter().acquire();
                final String streamName = streamNames.get(random.nextInt(numStreams));
                final long requestMillis = System.currentTimeMillis();
                final ByteBuffer data = buildBuffer(requestMillis, messageSizeBytes);
                if (null == data) {
                    break;
                }
                if (null != writer) {
                    writer.write(data).addEventListener(
                            new TimedRequestHandler(streamName, requestMillis));
                } else {
                    dlc.write(streamName, data).addEventListener(
                            new TimedRequestHandler(streamName, requestMillis));
                }
            }
            if (null != writer) {
                writer.close();
            }
            dlc.close();
        }
    }

    class BulkWriter implements Runnable {

        final int idx;
        final DistributedLogClient dlc;

        BulkWriter(int idx) {
            this.idx = idx;
            this.dlc = buildDlogClient();
        }

        @Override
        public void run() {
            LOG.info("Started writer {}.", idx);
            while (running) {
                rateLimiter.getLimiter().acquire(batchSize);
                String streamName = streamNames.get(random.nextInt(numStreams));
                final long requestMillis = System.currentTimeMillis();
                final List<ByteBuffer> data = buildBufferList(batchSize, requestMillis, messageSizeBytes);
                if (null == data) {
                    break;
                }
                List<Future<DLSN>> results = dlc.writeBulk(streamName, data);
                for (Future<DLSN> result : results) {
                    result.addEventListener(new TimedRequestHandler(streamName, requestMillis));
                }
            }
            dlc.close();
        }
    }

    @Override
    public void run() {
        LOG.info("Starting writer (concurrency = {}, prefix = {}, batchSize = {})",
                 new Object[] { writeConcurrency, streamPrefix, batchSize });
        try {
            for (int i = 0; i < writeConcurrency; i++) {
                Runnable writer = null;
                if (batchSize > 0) {
                    writer = new BulkWriter(i);
                } else {
                    writer = new Writer(i);
                }
                executorService.submit(writer);
            }
        } catch (Throwable t) {
            LOG.error("Unhandled exception caught", t);
        }
    }
}
