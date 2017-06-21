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
package org.apache.distributedlog.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.net.InetSocketAddressHelper;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.acl.AccessControlManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.client.resolver.DefaultRegionResolver;
import org.apache.distributedlog.client.resolver.RegionResolver;
import org.apache.distributedlog.client.routing.RoutingService;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.DLException;
import org.apache.distributedlog.exceptions.RegionUnavailableException;
import org.apache.distributedlog.exceptions.ServiceUnavailableException;
import org.apache.distributedlog.exceptions.StreamUnavailableException;
import org.apache.distributedlog.exceptions.TooManyStreamsException;
import org.apache.distributedlog.feature.AbstractFeatureProvider;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.common.rate.MovingAverageRate;
import org.apache.distributedlog.common.rate.MovingAverageRateFactory;
import org.apache.distributedlog.service.config.ServerConfiguration;
import org.apache.distributedlog.service.config.StreamConfigProvider;
import org.apache.distributedlog.service.placement.LeastLoadPlacementPolicy;
import org.apache.distributedlog.service.placement.LoadAppraiser;
import org.apache.distributedlog.service.placement.PlacementPolicy;
import org.apache.distributedlog.service.placement.ZKPlacementStateManager;
import org.apache.distributedlog.service.stream.BulkWriteOp;
import org.apache.distributedlog.service.stream.DeleteOp;
import org.apache.distributedlog.service.stream.HeartbeatOp;
import org.apache.distributedlog.service.stream.ReleaseOp;
import org.apache.distributedlog.service.stream.Stream;
import org.apache.distributedlog.service.stream.StreamFactory;
import org.apache.distributedlog.service.stream.StreamFactoryImpl;
import org.apache.distributedlog.service.stream.StreamManager;
import org.apache.distributedlog.service.stream.StreamManagerImpl;
import org.apache.distributedlog.service.stream.StreamOp;
import org.apache.distributedlog.service.stream.StreamOpStats;
import org.apache.distributedlog.service.stream.TruncateOp;
import org.apache.distributedlog.service.stream.WriteOp;
import org.apache.distributedlog.service.stream.WriteOpWithPayload;
import org.apache.distributedlog.service.stream.admin.CreateOp;
import org.apache.distributedlog.service.stream.admin.StreamAdminOp;
import org.apache.distributedlog.service.stream.limiter.ServiceRequestLimiter;
import org.apache.distributedlog.service.streamset.StreamPartitionConverter;
import org.apache.distributedlog.service.utils.ServerUtils;
import org.apache.distributedlog.thrift.service.BulkWriteResponse;
import org.apache.distributedlog.thrift.service.ClientInfo;
import org.apache.distributedlog.thrift.service.DistributedLogService;
import org.apache.distributedlog.thrift.service.HeartbeatOptions;
import org.apache.distributedlog.thrift.service.ResponseHeader;
import org.apache.distributedlog.thrift.service.ServerInfo;
import org.apache.distributedlog.thrift.service.ServerStatus;
import org.apache.distributedlog.thrift.service.StatusCode;
import org.apache.distributedlog.thrift.service.WriteContext;
import org.apache.distributedlog.thrift.service.WriteResponse;
import org.apache.distributedlog.util.ConfUtils;
import org.apache.distributedlog.util.OrderedScheduler;
import org.apache.distributedlog.common.util.SchedulerUtils;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.ScheduledThreadPoolTimer;
import com.twitter.util.Timer;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

/**
 * Implementation of distributedlog thrift service.
 */
public class DistributedLogServiceImpl implements DistributedLogService.ServiceIface,
                                                  FatalErrorHandler {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLogServiceImpl.class);

    private static final int MOVING_AVERAGE_WINDOW_SECS = 60;

    private final ServerConfiguration serverConfig;
    private final DistributedLogConfiguration dlConfig;
    private final Namespace dlNamespace;
    private final int serverRegionId;
    private final PlacementPolicy placementPolicy;
    private ServerStatus serverStatus = ServerStatus.WRITE_AND_ACCEPT;
    private final ReentrantReadWriteLock closeLock =
            new ReentrantReadWriteLock();
    private final CountDownLatch keepAliveLatch;
    private final byte dlsnVersion;
    private final String clientId;
    private final OrderedScheduler scheduler;
    private final AccessControlManager accessControlManager;
    private final StreamConfigProvider streamConfigProvider;
    private final StreamManager streamManager;
    private final StreamFactory streamFactory;
    private final RoutingService routingService;
    private final RegionResolver regionResolver;
    private final MovingAverageRateFactory movingAvgFactory;
    private final MovingAverageRate windowedRps;
    private final MovingAverageRate windowedBps;
    private final ServiceRequestLimiter limiter;
    private final Timer timer;
    private final HashedWheelTimer requestTimer;

    // Features
    private final FeatureProvider featureProvider;
    private final Feature featureRegionStopAcceptNewStream;
    private final Feature featureChecksumDisabled;
    private final Feature limiterDisabledFeature;

    // Stats
    private final StatsLogger statsLogger;
    private final StatsLogger perStreamStatsLogger;
    private final StreamPartitionConverter streamPartitionConverter;
    private final StreamOpStats streamOpStats;
    private final Counter bulkWritePendingStat;
    private final Counter writePendingStat;
    private final Counter redirects;
    private final Counter receivedRecordCounter;
    private final StatsLogger statusCodeStatLogger;
    private final ConcurrentHashMap<StatusCode, Counter> statusCodeCounters =
            new ConcurrentHashMap<StatusCode, Counter>();
    private final Counter statusCodeTotal;
    private final Gauge<Number> proxyStatusGauge;
    private final Gauge<Number> movingAvgRpsGauge;
    private final Gauge<Number> movingAvgBpsGauge;
    private final Gauge<Number> streamAcquiredGauge;
    private final Gauge<Number> streamCachedGauge;
    private final int shard;

    DistributedLogServiceImpl(ServerConfiguration serverConf,
                              DistributedLogConfiguration dlConf,
                              DynamicDistributedLogConfiguration dynDlConf,
                              StreamConfigProvider streamConfigProvider,
                              URI uri,
                              StreamPartitionConverter converter,
                              RoutingService routingService,
                              StatsLogger statsLogger,
                              StatsLogger perStreamStatsLogger,
                              CountDownLatch keepAliveLatch,
                              LoadAppraiser loadAppraiser)
            throws IOException {
        // Configuration.
        this.serverConfig = serverConf;
        this.dlConfig = dlConf;
        this.perStreamStatsLogger = perStreamStatsLogger;
        this.dlsnVersion = serverConf.getDlsnVersion();
        this.serverRegionId = serverConf.getRegionId();
        this.streamPartitionConverter = converter;
        int serverPort = serverConf.getServerPort();
        this.shard = serverConf.getServerShardId();
        int numThreads = serverConf.getServerThreads();
        this.clientId = DLSocketAddress.toLockId(DLSocketAddress.getSocketAddress(serverPort), shard);
        String allocatorPoolName = ServerUtils.getLedgerAllocatorPoolName(
            serverRegionId,
            shard,
            serverConf.isUseHostnameAsAllocatorPoolName());
        dlConf.setLedgerAllocatorPoolName(allocatorPoolName);
        this.featureProvider = AbstractFeatureProvider.getFeatureProvider("", dlConf, statsLogger.scope("features"));
        if (this.featureProvider instanceof AbstractFeatureProvider) {
            ((AbstractFeatureProvider) featureProvider).start();
        }

        // Build the namespace
        this.dlNamespace = NamespaceBuilder.newBuilder()
                .conf(dlConf)
                .uri(uri)
                .statsLogger(statsLogger)
                .featureProvider(this.featureProvider)
                .clientId(clientId)
                .regionId(serverRegionId)
                .build();
        this.accessControlManager = this.dlNamespace.createAccessControlManager();
        this.keepAliveLatch = keepAliveLatch;
        this.streamConfigProvider = streamConfigProvider;

        // Stats pertaining to stream op execution
        this.streamOpStats = new StreamOpStats(statsLogger, perStreamStatsLogger);

        // Executor Service.
        this.scheduler = OrderedScheduler.newBuilder()
                .corePoolSize(numThreads)
                .name("DistributedLogService-Executor")
                .build();

        // Timer, kept separate to ensure reliability of timeouts.
        this.requestTimer = new HashedWheelTimer(
            new ThreadFactoryBuilder().setNameFormat("DLServiceTimer-%d").build(),
            dlConf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
            dlConf.getTimeoutTimerNumTicks());

        // Creating and managing Streams
        this.streamFactory = new StreamFactoryImpl(clientId,
                streamOpStats,
                serverConf,
                dlConf,
                featureProvider,
                streamConfigProvider,
                converter,
                dlNamespace,
                scheduler,
                this,
                requestTimer);
        this.streamManager = new StreamManagerImpl(
                clientId,
                dlConf,
                scheduler,
                streamFactory,
                converter,
                streamConfigProvider,
                dlNamespace);
        this.routingService = routingService;
        this.regionResolver = new DefaultRegionResolver();

        // Service features
        this.featureRegionStopAcceptNewStream = this.featureProvider.getFeature(
                ServerFeatureKeys.REGION_STOP_ACCEPT_NEW_STREAM.name().toLowerCase());
        this.featureChecksumDisabled = this.featureProvider.getFeature(
                ServerFeatureKeys.SERVICE_CHECKSUM_DISABLED.name().toLowerCase());
        this.limiterDisabledFeature = this.featureProvider.getFeature(
                ServerFeatureKeys.SERVICE_GLOBAL_LIMITER_DISABLED.name().toLowerCase());

        // Resource limiting
        this.timer = new ScheduledThreadPoolTimer(1, "timer", true);
        this.movingAvgFactory = new MovingAverageRateFactory(scheduler);
        this.windowedRps = movingAvgFactory.create(MOVING_AVERAGE_WINDOW_SECS);
        this.windowedBps = movingAvgFactory.create(MOVING_AVERAGE_WINDOW_SECS);
        this.limiter = new ServiceRequestLimiter(
                dynDlConf,
                streamOpStats.baseScope("service_limiter"),
                windowedRps,
                windowedBps,
                streamManager,
                limiterDisabledFeature);

        this.placementPolicy = new LeastLoadPlacementPolicy(
            loadAppraiser,
            routingService,
            dlNamespace,
            new ZKPlacementStateManager(uri, dlConf, statsLogger),
            Duration.fromSeconds(serverConf.getResourcePlacementRefreshInterval()),
            statsLogger);
        logger.info("placement started");

        // Stats
        this.statsLogger = statsLogger;

        // Gauges for server status/health
        this.proxyStatusGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return ServerStatus.DOWN == serverStatus ? -1 : (featureRegionStopAcceptNewStream.isAvailable()
                    ? 3 : (ServerStatus.WRITE_AND_ACCEPT == serverStatus ? 1 : 2));
            }
        };
        this.movingAvgRpsGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return windowedRps.get();
            }
        };
        this.movingAvgBpsGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return windowedBps.get();
            }
        };
        // Gauges for streams
        this.streamAcquiredGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return streamManager.numAcquired();
            }
        };
        this.streamCachedGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return streamManager.numCached();
            }
        };

        // Stats on server
        statsLogger.registerGauge("proxy_status", proxyStatusGauge);
        // Global moving average rps
        statsLogger.registerGauge("moving_avg_rps", movingAvgRpsGauge);
        // Global moving average bps
        statsLogger.registerGauge("moving_avg_bps", movingAvgBpsGauge);
        // Stats on requests
        this.bulkWritePendingStat = streamOpStats.requestPendingCounter("bulkWritePending");
        this.writePendingStat = streamOpStats.requestPendingCounter("writePending");
        this.redirects = streamOpStats.requestCounter("redirect");
        this.statusCodeStatLogger = streamOpStats.requestScope("statuscode");
        this.statusCodeTotal = streamOpStats.requestCounter("statuscode_count");
        this.receivedRecordCounter = streamOpStats.recordsCounter("received");

        // Stats for streams
        StatsLogger streamsStatsLogger = statsLogger.scope("streams");
        streamsStatsLogger.registerGauge("acquired", this.streamAcquiredGauge);
        streamsStatsLogger.registerGauge("cached", this.streamCachedGauge);

        // Setup complete
        logger.info("Running distributedlog server : client id {}, allocator pool {}, perstream stat {},"
            + " dlsn version {}.",
            new Object[] { clientId, allocatorPoolName, serverConf.isPerStreamStatEnabled(), dlsnVersion });
    }

    private void countStatusCode(StatusCode code) {
        Counter counter = statusCodeCounters.get(code);
        if (null == counter) {
            counter = statusCodeStatLogger.getCounter(code.name());
            Counter oldCounter = statusCodeCounters.putIfAbsent(code, counter);
            if (null != oldCounter) {
                counter = oldCounter;
            }
        }
        counter.inc();
        statusCodeTotal.inc();
    }

    @Override
    public Future<ServerInfo> handshake() {
        return handshakeWithClientInfo(new ClientInfo());
    }

    @Override
    public Future<ServerInfo> handshakeWithClientInfo(ClientInfo clientInfo) {
        ServerInfo serverInfo = new ServerInfo();
        closeLock.readLock().lock();
        try {
            serverInfo.setServerStatus(serverStatus);
        } finally {
            closeLock.readLock().unlock();
        }

        if (clientInfo.isSetGetOwnerships() && !clientInfo.isGetOwnerships()) {
            return Future.value(serverInfo);
        }

        Optional<String> regex = Optional.absent();
        if (clientInfo.isSetStreamNameRegex()) {
            regex = Optional.of(clientInfo.getStreamNameRegex());
        }

        Map<String, String> ownershipMap = streamManager.getStreamOwnershipMap(regex);
        serverInfo.setOwnerships(ownershipMap);
        return Future.value(serverInfo);
    }

    @VisibleForTesting
    Stream getLogWriter(String stream) throws IOException {
        Stream writer = streamManager.getStream(stream);
        if (null == writer) {
            closeLock.readLock().lock();
            try {
                if (featureRegionStopAcceptNewStream.isAvailable()) {
                    // accept new stream is disabled in current dc
                    throw new RegionUnavailableException("Region is unavailable right now.");
                } else if (!(ServerStatus.WRITE_AND_ACCEPT == serverStatus)) {
                    // if it is closed, we would not acquire stream again.
                    return null;
                }
                writer = streamManager.getOrCreateStream(stream, true);
            } finally {
                closeLock.readLock().unlock();
            }
        }
        return writer;
    }

    // Service interface methods

    @Override
    public Future<WriteResponse> write(final String stream, ByteBuffer data) {
        receivedRecordCounter.inc();
        return doWrite(stream, data, null /* checksum */, false);
    }

    @Override
    public Future<BulkWriteResponse> writeBulkWithContext(final String stream,
                                                          List<ByteBuffer> data,
                                                          WriteContext ctx) {
        bulkWritePendingStat.inc();
        receivedRecordCounter.add(data.size());
        BulkWriteOp op = new BulkWriteOp(stream, data, statsLogger, perStreamStatsLogger, streamPartitionConverter,
            getChecksum(ctx), featureChecksumDisabled, accessControlManager);
        executeStreamOp(op);
        return op.result().ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                bulkWritePendingStat.dec();
                return null;
            }
        });
    }

    @Override
    public Future<WriteResponse> writeWithContext(final String stream, ByteBuffer data, WriteContext ctx) {
        return doWrite(stream, data, getChecksum(ctx), ctx.isIsRecordSet());
    }

    @Override
    public Future<WriteResponse> heartbeat(String stream, WriteContext ctx) {
        HeartbeatOp op = new HeartbeatOp(stream, statsLogger, perStreamStatsLogger, dlsnVersion, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> heartbeatWithOptions(String stream, WriteContext ctx, HeartbeatOptions options) {
        HeartbeatOp op = new HeartbeatOp(stream, statsLogger, perStreamStatsLogger, dlsnVersion, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        if (options.isSendHeartBeatToReader()) {
            op.setWriteControlRecord(true);
        }
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> truncate(String stream, String dlsn, WriteContext ctx) {
        TruncateOp op = new TruncateOp(
            stream,
            DLSN.deserialize(dlsn),
            statsLogger,
            perStreamStatsLogger,
            getChecksum(ctx),
            featureChecksumDisabled,
            accessControlManager);
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> delete(String stream, WriteContext ctx) {
        DeleteOp op = new DeleteOp(stream, statsLogger, perStreamStatsLogger, streamManager, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> release(String stream, WriteContext ctx) {
        ReleaseOp op = new ReleaseOp(stream, statsLogger, perStreamStatsLogger, streamManager, getChecksum(ctx),
            featureChecksumDisabled, accessControlManager);
        executeStreamOp(op);
        return op.result();
    }

    @Override
    public Future<WriteResponse> create(String stream, WriteContext ctx) {
        CreateOp op = new CreateOp(stream, statsLogger, streamManager, getChecksum(ctx), featureChecksumDisabled);
        return executeStreamAdminOp(op);
    }

    //
    // Ownership RPC
    //

    @Override
    public Future<WriteResponse> getOwner(String streamName, WriteContext ctx) {
        if (streamManager.isAcquired(streamName)) {
            // the stream is already acquired
            return Future.value(new WriteResponse(ResponseUtils.ownerToHeader(clientId)));
        }

        return placementPolicy.placeStream(streamName).map(new Function<String, WriteResponse>() {
            @Override
            public WriteResponse apply(String server) {
                String host = DLSocketAddress.toLockId(InetSocketAddressHelper.parse(server), -1);
                return new WriteResponse(ResponseUtils.ownerToHeader(host));
            }
        });
    }


    //
    // Admin RPCs
    //

    @Override
    public Future<Void> setAcceptNewStream(boolean enabled) {
        closeLock.writeLock().lock();
        try {
            logger.info("Set AcceptNewStream = {}", enabled);
            if (ServerStatus.DOWN != serverStatus) {
                if (enabled) {
                    serverStatus = ServerStatus.WRITE_AND_ACCEPT;
                } else {
                    serverStatus = ServerStatus.WRITE_ONLY;
                }
            }
        } finally {
            closeLock.writeLock().unlock();
        }
        return Future.Void();
    }

    private Future<WriteResponse> doWrite(final String name,
                                          ByteBuffer data,
                                          Long checksum,
                                          boolean isRecordSet) {
        writePendingStat.inc();
        receivedRecordCounter.inc();
        WriteOp op = newWriteOp(name, data, checksum, isRecordSet);
        executeStreamOp(op);
        return op.result().ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                writePendingStat.dec();
                return null;
            }
        });
    }

    private Long getChecksum(WriteContext ctx) {
        return ctx.isSetCrc32() ? ctx.getCrc32() : null;
    }

    private Future<WriteResponse> executeStreamAdminOp(final StreamAdminOp op) {
        try {
            op.preExecute();
        } catch (DLException dle) {
            return Future.exception(dle);
        }
        return op.execute();
    }

    private void executeStreamOp(final StreamOp op) {

        // Must attach this as early as possible--returning before this point will cause us to
        // lose the status code.
        op.responseHeader().addEventListener(new FutureEventListener<ResponseHeader>() {
            @Override
            public void onSuccess(ResponseHeader header) {
                if (header.getLocation() != null || header.getCode() == StatusCode.FOUND) {
                    redirects.inc();
                }
                countStatusCode(header.getCode());
            }
            @Override
            public void onFailure(Throwable cause) {
            }
        });

        try {
            // Apply the request limiter
            limiter.apply(op);

            // Execute per-op pre-exec code
            op.preExecute();

        } catch (TooManyStreamsException e) {
            // Translate to StreamUnavailableException to ensure that the client will redirect
            // to a different host. Ideally we would be able to return TooManyStreamsException,
            // but the way exception handling works right now we can't control the handling in
            // the client because client changes deploy very slowly.
            op.fail(new StreamUnavailableException(e.getMessage()));
            return;
        } catch (Exception e) {
            op.fail(e);
            return;
        }

        Stream stream;
        try {
            stream = getLogWriter(op.streamName());
        } catch (RegionUnavailableException rue) {
            // redirect the requests to other region
            op.fail(new RegionUnavailableException("Region " + serverRegionId + " is unavailable."));
            return;
        } catch (IOException e) {
            op.fail(e);
            return;
        }
        if (null == stream) {
            // redirect the requests when stream is unavailable.
            op.fail(new ServiceUnavailableException("Server " + clientId + " is closed."));
            return;
        }

        if (op instanceof WriteOpWithPayload) {
            WriteOpWithPayload writeOp = (WriteOpWithPayload) op;
            windowedBps.add(writeOp.getPayloadSize());
            windowedRps.inc();
        }

        stream.submit(op);
    }

    void shutdown() {
        try {
            closeLock.writeLock().lock();
            try {
                if (ServerStatus.DOWN == serverStatus) {
                    return;
                }
                serverStatus = ServerStatus.DOWN;
            } finally {
                closeLock.writeLock().unlock();
            }

            streamManager.close();
            movingAvgFactory.close();
            limiter.close();

            Stopwatch closeStreamsStopwatch = Stopwatch.createStarted();

            Future<List<Void>> closeResult = streamManager.closeStreams();
            logger.info("Waiting for closing all streams ...");
            try {
                Await.result(closeResult, Duration.fromTimeUnit(5, TimeUnit.MINUTES));
                logger.info("Closed all streams in {} millis.",
                        closeStreamsStopwatch.elapsed(TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                logger.warn("Interrupted on waiting for closing all streams : ", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warn("Sorry, we didn't close all streams gracefully in 5 minutes : ", e);
            }

            // shutdown the dl namespace
            logger.info("Closing distributedlog namespace ...");
            dlNamespace.close();
            logger.info("Closed distributedlog namespace .");

            // Stop the feature provider
            if (this.featureProvider instanceof AbstractFeatureProvider) {
                ((AbstractFeatureProvider) featureProvider).stop();
            }

            // Stop the timer.
            timer.stop();
            placementPolicy.close();

            // clean up gauge
            unregisterGauge();

            // shutdown the executor after requesting closing streams.
            SchedulerUtils.shutdownScheduler(scheduler, 60, TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.info("Exception while shutting down distributedlog service.");
        } finally {
            // release the keepAliveLatch in case shutdown is called from a shutdown hook.
            keepAliveLatch.countDown();
            logger.info("Finished shutting down distributedlog service.");
        }
    }

    protected void startPlacementPolicy() {
        this.placementPolicy.start(shard == 0);
    }

    @Override
    public void notifyFatalError() {
        triggerShutdown();
    }

    private void triggerShutdown() {
        // release the keepAliveLatch to let the main thread shutdown the whole service.
        logger.info("Releasing KeepAlive Latch to trigger shutdown ...");
        keepAliveLatch.countDown();
        logger.info("Released KeepAlive Latch. Main thread will shut the service down.");
    }

    // Test methods.

    private DynamicDistributedLogConfiguration getDynConf(String streamName) {
        Optional<DynamicDistributedLogConfiguration> dynDlConf =
                streamConfigProvider.getDynamicStreamConfig(streamName);
        if (dynDlConf.isPresent()) {
            return dynDlConf.get();
        } else {
            return ConfUtils.getConstDynConf(dlConfig);
        }
    }

    /**
     * clean up the gauge before we close to help GC.
     */
    private void unregisterGauge(){
        this.statsLogger.unregisterGauge("proxy_status", this.proxyStatusGauge);
        this.statsLogger.unregisterGauge("moving_avg_rps", this.movingAvgRpsGauge);
        this.statsLogger.unregisterGauge("moving_avg_bps", this.movingAvgBpsGauge);
        this.statsLogger.unregisterGauge("acquired", this.streamAcquiredGauge);
        this.statsLogger.unregisterGauge("cached", this.streamCachedGauge);
    }

    @VisibleForTesting
    Stream newStream(String name) throws IOException {
        return streamManager.getOrCreateStream(name, false);
    }

    @VisibleForTesting
    WriteOp newWriteOp(String stream, ByteBuffer data, Long checksum) {
        return newWriteOp(stream, data, checksum, false);
    }

    @VisibleForTesting
    RoutingService getRoutingService() {
        return this.routingService;
    }

    @VisibleForTesting
    DLSocketAddress getServiceAddress() throws IOException {
        return DLSocketAddress.deserialize(clientId);
    }

    WriteOp newWriteOp(String stream,
                       ByteBuffer data,
                       Long checksum,
                       boolean isRecordSet) {
        return new WriteOp(stream, data, statsLogger, perStreamStatsLogger, streamPartitionConverter,
            serverConfig, dlsnVersion, checksum, isRecordSet, featureChecksumDisabled,
            accessControlManager);
    }

    @VisibleForTesting
    Future<List<Void>> closeStreams() {
        return streamManager.closeStreams();
    }

    @VisibleForTesting
    public Namespace getDistributedLogNamespace() {
        return dlNamespace;
    }

    @VisibleForTesting
    StreamManager getStreamManager() {
        return streamManager;
    }
}
