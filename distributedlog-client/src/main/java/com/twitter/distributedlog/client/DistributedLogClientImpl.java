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
package com.twitter.distributedlog.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordSet;
import com.twitter.distributedlog.LogRecordSetBuffer;
import com.twitter.distributedlog.StatusCode;
import com.twitter.distributedlog.client.functions.Functions;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.distributedlog.service.protocol.HeartbeatOptions;
import com.twitter.distributedlog.service.protocol.ResponseHeader;
import com.twitter.distributedlog.service.protocol.ServerInfo;
import com.twitter.distributedlog.service.protocol.WriteContext;
import com.twitter.distributedlog.service.protocol.WriteResponse;
import com.twitter.distributedlog.util.ProtocolUtils;
import com.twitter.distributedlog.client.proxy.HostProvider;
import com.twitter.distributedlog.client.proxy.ProxyClient;
import com.twitter.distributedlog.client.proxy.ProxyClientManager;
import com.twitter.distributedlog.client.proxy.ProxyListener;
import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import com.twitter.distributedlog.client.ownership.OwnershipCache;
import com.twitter.distributedlog.client.resolver.RegionResolver;
import com.twitter.distributedlog.client.finagle.routing.RoutingService;
import com.twitter.distributedlog.client.finagle.routing.RoutingService.RoutingContext;
import com.twitter.distributedlog.client.stats.ClientStats;
import com.twitter.distributedlog.client.stats.OpStats;
import com.twitter.distributedlog.exceptions.DLClientClosedException;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.service.DLSocketAddress;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.finagle.CancelledRequestException;
import com.twitter.finagle.ConnectionFailedException;
import com.twitter.finagle.Failure;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.RequestTimeoutException;
import com.twitter.finagle.ServiceException;
import com.twitter.finagle.ServiceTimeoutException;
import com.twitter.finagle.WriteException;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.twitter.distributedlog.StatusCode.*;

/**
 * Implementation of distributedlog client
 */
public class DistributedLogClientImpl implements DistributedLogClient, MonitorServiceClient,
        RoutingService.RoutingListener, ProxyListener, HostProvider {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogClientImpl.class);

    private final String clientName;
    private final ClientConfig clientConfig;
    private final RoutingService routingService;
    private final ProxyClient.Builder clientBuilder;
    private final boolean streamFailfast;
    private final Pattern streamNameRegexPattern;

    // Timer
    private final HashedWheelTimer dlTimer;

    // region resolver
    private final RegionResolver regionResolver;

    // Ownership maintenance
    private final OwnershipCache ownershipCache;
    // Channel/Client management
    private final ProxyClientManager clientManager;

    // Close Status
    private boolean closed = false;
    private final ReentrantReadWriteLock closeLock =
            new ReentrantReadWriteLock();

    abstract class StreamOp implements TimerTask {
        final String stream;

        final AtomicInteger tries = new AtomicInteger(0);
        final RoutingContext routingContext = RoutingContext.of(regionResolver);
        final WriteContext ctx = new WriteContext();
        final Stopwatch stopwatch;
        final OpStats opStats;
        SocketAddress nextAddressToSend;

        StreamOp(final String stream, final OpStats opStats) {
            this.stream = stream;
            this.stopwatch = Stopwatch.createStarted();
            this.opStats = opStats;
        }

        void send(SocketAddress address) {
            long elapsedMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (clientConfig.getMaxRedirects() > 0 &&
                    tries.get() >= clientConfig.getMaxRedirects()) {
                fail(address, new RequestTimeoutException(Duration.fromMilliseconds(elapsedMs),
                        "Exhausted max redirects in " + elapsedMs + " ms"));
                return;
            } else if (clientConfig.getRequestTimeoutMs() > 0 &&
                    elapsedMs >= clientConfig.getRequestTimeoutMs()) {
                fail(address, new RequestTimeoutException(Duration.fromMilliseconds(elapsedMs),
                        "Exhausted max request timeout " + clientConfig.getRequestTimeoutMs()
                                + " in " + elapsedMs + " ms"));
                return;
            }
            synchronized (this) {
                String addrStr = address.toString();
                if (ctx.isHostTried(addrStr)) {
                    nextAddressToSend = address;
                    dlTimer.newTimeout(this,
                            Math.min(clientConfig.getRedirectBackoffMaxMs(),
                                    tries.get() * clientConfig.getRedirectBackoffStartMs()),
                            TimeUnit.MILLISECONDS);
                } else {
                    doSend(address);
                }
            }
        }

        abstract Future<ResponseHeader> sendRequest(ProxyClient sc);

        void doSend(SocketAddress address) {
            ctx.addTriedHost(address.toString());
            if (clientConfig.isChecksumEnabled()) {
                Long crc32 = computeChecksum();
                if (null != crc32) {
                    ctx.setCrc32(crc32);
                }
            }
            tries.incrementAndGet();
            sendWriteRequest(address, this);
        }

        void beforeComplete(ProxyClient sc, ResponseHeader responseHeader) {
            ownershipCache.updateOwner(stream, sc.getAddress());
        }

        void complete(SocketAddress address) {
            stopwatch.stop();
            opStats.completeRequest(address,
                    stopwatch.elapsed(TimeUnit.MICROSECONDS), tries.get());
        }

        void fail(SocketAddress address, Throwable t) {
            stopwatch.stop();
            opStats.failRequest(address,
                    stopwatch.elapsed(TimeUnit.MICROSECONDS), tries.get());
        }

        Long computeChecksum() {
            return null;
        }

        @Override
        synchronized public void run(Timeout timeout) throws Exception {
            if (!timeout.isCancelled() && null != nextAddressToSend) {
                doSend(nextAddressToSend);
            } else {
                fail(null, new CancelledRequestException());
            }
        }
    }

    abstract class AbstractWriteOp extends StreamOp {

        final Promise<WriteResponse> result = new Promise<WriteResponse>();
        Long crc32 = null;

        AbstractWriteOp(final String name, final OpStats opStats) {
            super(name, opStats);
        }

        void complete(SocketAddress address, WriteResponse response) {
            super.complete(address);
            result.setValue(response);
        }

        @Override
        void fail(SocketAddress address, Throwable t) {
            super.fail(address, t);
            result.setException(t);
        }

        @Override
        Long computeChecksum() {
            if (null == crc32) {
                crc32 = ProtocolUtils.streamOpCRC32(stream);
            }
            return crc32;
        }

        @Override
        Future<ResponseHeader> sendRequest(final ProxyClient sc) {
            return this.sendWriteRequest(sc).addEventListener(new FutureEventListener<WriteResponse>() {
                @Override
                public void onSuccess(WriteResponse response) {
                    if (response.getHeader().getCode() == StatusCode.SUCCESS) {
                        beforeComplete(sc, response.getHeader());
                        AbstractWriteOp.this.complete(sc.getAddress(), response);
                    }
                }
                @Override
                public void onFailure(Throwable cause) {
                    // handled by the ResponseHeader listener
                }
            }).map(new AbstractFunction1<WriteResponse, ResponseHeader>() {
                @Override
                public ResponseHeader apply(WriteResponse response) {
                    return response.getHeader();
                }
            });
        }

        abstract Future<WriteResponse> sendWriteRequest(ProxyClient sc);
    }

    class WriteOp extends AbstractWriteOp {
        final ByteBuffer data;

        WriteOp(final String name, final ByteBuffer data) {
            super(name, clientStats.getOpStats("write"));
            this.data = data;
        }

        @Override
        Future<WriteResponse> sendWriteRequest(ProxyClient sc) {
            return sc.write(stream, data, ctx);
        }

        @Override
        Long computeChecksum() {
            if (null == crc32) {
                byte[] dataBytes = new byte[data.remaining()];
                data.duplicate().get(dataBytes);
                crc32 = ProtocolUtils.writeOpCRC32(stream, dataBytes);
            }
            return crc32;
        }

        Future<DLSN> result() {
            return result.map(Functions.EXTRACT_DLSN_FUNC);
        }
    }

    class TruncateOp extends AbstractWriteOp {
        final DLSN dlsn;

        TruncateOp(String name, DLSN dlsn) {
            super(name, clientStats.getOpStats("truncate"));
            this.dlsn = dlsn;
        }

        @Override
        Long computeChecksum() {
            if (null == crc32) {
                crc32 = ProtocolUtils.truncateOpCRC32(stream, dlsn);
            }
            return crc32;
        }

        @Override
        Future<WriteResponse> sendWriteRequest(ProxyClient sc) {
            return sc.truncate(stream, dlsn, ctx);
        }

        Future<Boolean> result() {
            return result.map(new AbstractFunction1<WriteResponse, Boolean>() {
                @Override
                public Boolean apply(WriteResponse response) {
                    return true;
                }
            });
        }
    }

    class WriteRecordSetOp extends WriteOp {

        WriteRecordSetOp(String name, LogRecordSetBuffer recordSet) {
            super(name, recordSet.getBuffer());
            ctx.setRecordSet(true);
        }

    }

    class ReleaseOp extends AbstractWriteOp {

        ReleaseOp(String name) {
            super(name, clientStats.getOpStats("release"));
        }

        @Override
        Future<WriteResponse> sendWriteRequest(ProxyClient sc) {
            return sc.release(stream, ctx);
        }

        @Override
        void beforeComplete(ProxyClient sc, ResponseHeader header) {
            ownershipCache.removeOwnerFromStream(stream, sc.getAddress(), "Stream Deleted");
        }

        Future<Void> result() {
            return result.map(new AbstractFunction1<WriteResponse, Void>() {
                @Override
                public Void apply(WriteResponse response) {
                    return null;
                }
            });
        }
    }

    class DeleteOp extends AbstractWriteOp {

        DeleteOp(String name) {
            super(name, clientStats.getOpStats("delete"));
        }

        @Override
        Future<WriteResponse> sendWriteRequest(ProxyClient sc) {
            return sc.delete(stream, ctx);
        }

        @Override
        void beforeComplete(ProxyClient sc, ResponseHeader header) {
            ownershipCache.removeOwnerFromStream(stream, sc.getAddress(), "Stream Deleted");
        }

        Future<Void> result() {
            return result.map(new AbstractFunction1<WriteResponse, Void>() {
                @Override
                public Void apply(WriteResponse v1) {
                    return null;
                }
            });
        }
    }

    class CreateOp extends AbstractWriteOp {

        CreateOp(String name) {
            super(name, clientStats.getOpStats("create"));
        }

        @Override
        Future<WriteResponse> sendWriteRequest(ProxyClient sc) {
            return sc.create(stream, ctx);
        }

        @Override
        void beforeComplete(ProxyClient sc, ResponseHeader header) {
            ownershipCache.updateOwner(stream, sc.getAddress());
        }

        Future<Void> result() {
            return result.map(new AbstractFunction1<WriteResponse, Void>() {
                @Override
                public Void apply(WriteResponse v1) {
                    return null;
                }
            }).voided();
        }
    }

    class HeartbeatOp extends AbstractWriteOp {
        HeartbeatOptions options;

        HeartbeatOp(String name, boolean sendReaderHeartBeat) {
            super(name, clientStats.getOpStats("heartbeat"));
            options = new HeartbeatOptions(Optional.fromNullable(sendReaderHeartBeat));
        }

        @Override
        Future<WriteResponse> sendWriteRequest(ProxyClient sc) {
            return sc.heartbeat(stream, ctx, options);
        }

        Future<Void> result() {
            return result.map(new AbstractFunction1<WriteResponse, Void>() {
                @Override
                public Void apply(WriteResponse response) {
                    return null;
                }
            });
        }
    }

    // Stats
    private final ClientStats clientStats;

    public DistributedLogClientImpl(String name,
                                    RoutingService routingService,
                                    ClientConfig clientConfig,
                                    ProxyClient.Builder proxyClientBuilder,
                                    StatsReceiver statsReceiver,
                                    StatsReceiver streamStatsReceiver,
                                    ClientStats clientStats,
                                    RegionResolver regionResolver,
                                    boolean enableRegionStats) {
        this.clientName = name;
        this.routingService = routingService;
        this.clientConfig = clientConfig;
        this.streamFailfast = clientConfig.getStreamFailfast();
        this.streamNameRegexPattern = Pattern.compile(clientConfig.getStreamNameRegex());
        this.regionResolver = regionResolver;
        // Build the timer
        this.dlTimer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("DLClient-" + name + "-timer-%d").build(),
                this.clientConfig.getRedirectBackoffStartMs(),
                TimeUnit.MILLISECONDS);
        // register routing listener
        this.routingService.registerListener(this);
        // build the ownership cache
        this.ownershipCache = new OwnershipCache(this.clientConfig, this.dlTimer, statsReceiver, streamStatsReceiver);
        // Client Stats
        this.clientStats = clientStats;
        // Client Manager
        this.clientBuilder = proxyClientBuilder;
        this.clientManager = new ProxyClientManager(
                this.clientConfig,  // client config
                this.clientBuilder, // client builder
                this.dlTimer,       // timer
                this,               // host provider
                clientStats);       // client stats
        this.clientManager.registerProxyListener(this);

        // Cache Stats
        StatsReceiver cacheStatReceiver = statsReceiver.scope("cache");
        Seq<String> numCachedStreamsGaugeName =
                scala.collection.JavaConversions.asScalaBuffer(Arrays.asList("num_streams")).toList();
        cacheStatReceiver.provideGauge(numCachedStreamsGaugeName, new Function0<Object>() {
            @Override
            public Object apply() {
                return (float) ownershipCache.getNumCachedStreams();
            }
        });
        Seq<String> numCachedHostsGaugeName =
                scala.collection.JavaConversions.asScalaBuffer(Arrays.asList("num_hosts")).toList();
        cacheStatReceiver.provideGauge(numCachedHostsGaugeName, new Function0<Object>() {
            @Override
            public Object apply() {
                return (float) clientManager.getNumProxies();
            }
        });

        logger.info("Build distributedlog client : name = {}, routing_service = {}, stats_receiver = {}, thriftmux = {}",
                    new Object[] { name, routingService.getClass(), statsReceiver.getClass(), clientConfig.getThriftMux() });
    }

    @Override
    public Set<SocketAddress> getHosts() {
        Set<SocketAddress> hosts = Sets.newHashSet();
        // use both routing service and ownership cache for the handshaking source
        hosts.addAll(this.routingService.getHosts());
        hosts.addAll(this.ownershipCache.getStreamOwnershipDistribution().keySet());
        return hosts;
    }

    @Override
    public void onHandshakeSuccess(SocketAddress address, ProxyClient client, ServerInfo serverInfo) {
        if (null != serverInfo && serverInfo.isServerDown()) {
            logger.info("{} is detected as DOWN during handshaking", address);
            // server is shutting down
            handleServiceUnavailable(address, client, Optional.<StreamOp>absent());
            return;
        }

        if (null != serverInfo && serverInfo.getOwnerships().isPresent()) {
            Map<String, String> ownerships = serverInfo.getOwnerships().get();
            logger.debug("Handshaked with {} : {} ownerships returned.", address, ownerships.size());
            for (Map.Entry<String, String> entry : ownerships.entrySet()) {
                Matcher matcher = streamNameRegexPattern.matcher(entry.getKey());
                if (!matcher.matches()) {
                    continue;
                }
                updateOwnership(entry.getKey(), entry.getValue());
            }
        } else {
            logger.debug("Handshaked with {} : no ownerships returned", address);
        }
    }

    @Override
    public void onHandshakeFailure(SocketAddress address, ProxyClient client, Throwable cause) {
        cause = showRootCause(Optional.<StreamOp>absent(), cause);
        handleRequestException(address, client, Optional.<StreamOp>absent(), cause);
    }

    @VisibleForTesting
    public void handshake() {
        clientManager.handshake();
        logger.info("Handshaked with {} hosts, cached {} streams",
                clientManager.getNumProxies(), ownershipCache.getNumCachedStreams());
    }

    @Override
    public void onServerLeft(SocketAddress address) {
        onServerLeft(address, null);
    }

    private void onServerLeft(SocketAddress address, ProxyClient sc) {
        ownershipCache.removeAllStreamsFromOwner(address);
        if (null == sc) {
            clientManager.removeClient(address);
        } else {
            clientManager.removeClient(address, sc);
        }
    }

    @Override
    public void onServerJoin(SocketAddress address) {
        clientManager.createClient(address);
    }

    public void close() {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }
        clientManager.close();
        routingService.unregisterListener(this);
        routingService.stopService();
        dlTimer.stop();
    }

    @Override
    public Future<Void> check(String stream) {
        final HeartbeatOp op = new HeartbeatOp(stream, false);
        sendRequest(op);
        return op.result();
    }

    @Override
    public Future<Void> heartbeat(String stream) {
        final HeartbeatOp op = new HeartbeatOp(stream, true);
        sendRequest(op);
        return op.result();
    }

    @Override
    public Map<SocketAddress, Set<String>> getStreamOwnershipDistribution() {
        return ownershipCache.getStreamOwnershipDistribution();
    }

    @Override
    public Future<Void> setAcceptNewStream(boolean enabled) {
        Map<SocketAddress, ProxyClient> snapshot = clientManager.getAllClients();
        List<Future<Void>> futures = new ArrayList<Future<Void>>(snapshot.size());
        for (Map.Entry<SocketAddress, ProxyClient> entry : snapshot.entrySet()) {
            futures.add(entry.getValue().setAcceptNewStream(enabled));
        }
        return Future.collect(futures).map(new Function<List<Void>, Void>() {
            @Override
            public Void apply(List<Void> list) {
                return null;
            }
        });
    }

    @Override
    public Future<DLSN> write(String stream, ByteBuffer data) {
        final WriteOp op = new WriteOp(stream, data);
        sendRequest(op);
        return op.result();
    }

    @Override
    public Future<DLSN> writeRecordSet(String stream, final LogRecordSetBuffer recordSet) {
        final WriteRecordSetOp op = new WriteRecordSetOp(stream, recordSet);
        sendRequest(op);
        return op.result();
    }

    private void transmitRecordSet(String stream, final LogRecordSetBuffer recordSetBuffer) {
        writeRecordSet(stream, recordSetBuffer).addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                recordSetBuffer.abortTransmit(cause);
            }

            @Override
            public void onSuccess(DLSN dlsn) {
                recordSetBuffer.completeTransmit(
                        dlsn.getLogSegmentSequenceNo(),
                        dlsn.getEntryId(),
                        dlsn.getSlotId());
            }
        });
    }

    @Override
    public List<Future<DLSN>> writeBulk(String stream, List<ByteBuffer> data) {
        LogRecordSet.Writer recordSetBuffer = LogRecordSet.newWriter(
                16 * 1024, CompressionCodec.Type.NONE);

        List<Future<DLSN>> futures = Lists.newArrayListWithExpectedSize(data.size());
        Throwable cause = null;
        for (ByteBuffer buffer : data) {
            Promise<DLSN> writePromise = new Promise<DLSN>();
            futures.add(writePromise);
            // already encountered errors
            if (null != cause) {
                writePromise.setException(cause);
                continue;
            }

            int logRecordSize = buffer.remaining();
            if (logRecordSize > LogRecord.MAX_LOGRECORD_SIZE) {
                writePromise.setException(new LogRecordTooLongException(
                        "Log record of size " + logRecordSize + " written when only "
                        + LogRecord.MAX_LOGRECORD_SIZE + " is allowed"));
                continue;
            }

            // if exceed max number of bytes
            if (recordSetBuffer.getNumBytes() + logRecordSize > LogRecord.MAX_LOGRECORDSET_SIZE) {
                transmitRecordSet(stream, recordSetBuffer);
                recordSetBuffer = LogRecordSet.newWriter(
                        16 * 1024, CompressionCodec.Type.NONE);
            }

            // append a new record
            try {
                recordSetBuffer.writeRecord(buffer, writePromise);
            } catch (LogRecordTooLongException e) {
                writePromise.setException(e);
            } catch (com.twitter.distributedlog.exceptions.WriteException e) {
                cause = e;
                writePromise.setException(cause);
            }
        }

        transmitRecordSet(stream, recordSetBuffer);
        return futures;
    }

    @Override
    public Future<Boolean> truncate(String stream, DLSN dlsn) {
        final TruncateOp op = new TruncateOp(stream, dlsn);
        sendRequest(op);
        return op.result();
    }

    @Override
    public Future<Void> delete(String stream) {
        final DeleteOp op = new DeleteOp(stream);
        sendRequest(op);
        return op.result();
    }

    @Override
    public Future<Void> release(String stream) {
        final ReleaseOp op = new ReleaseOp(stream);
        sendRequest(op);
        return op.result();
    }

    @Override
    public Future<Void> create(String stream) {
        final CreateOp op = new CreateOp(stream);
        sendRequest(op);
        return op.result();
    }

    private void sendRequest(final StreamOp op) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                op.fail(null, new DLClientClosedException("Client " + clientName + " is closed."));
            } else {
                doSend(op, null);
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * Send the stream operation by routing service, excluding previous address if it is not null.
     *
     * @param op
     *          stream operation.
     * @param previousAddr
     *          previous tried address.
     */
    private void doSend(final StreamOp op, final SocketAddress previousAddr) {
        if (null != previousAddr) {
            op.routingContext.addTriedHost(previousAddr, StatusCode.WRITE_EXCEPTION);
        }
        // Get host first
        SocketAddress address = ownershipCache.getOwner(op.stream);
        if (null == address || op.routingContext.isTriedHost(address)) {
            // pickup host by hashing
            try {
                address = routingService.getHost(op.stream, op.routingContext);
            } catch (NoBrokersAvailableException nbae) {
                op.fail(null, nbae);
                return;
            }
        }
        op.send(address);
    }

    private void sendWriteRequest(final SocketAddress addr, final StreamOp op) {
        // Get corresponding finagle client
        final ProxyClient sc = clientManager.getClient(addr);
        final long startTimeNanos = System.nanoTime();
        // write the request to that host.
        op.sendRequest(sc).addEventListener(new FutureEventListener<ResponseHeader>() {
            @Override
            public void onSuccess(ResponseHeader header) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Received response; header: {}", header);
                }
                clientStats.completeProxyRequest(addr, header.getCode(), startTimeNanos);
                // update routing context
                op.routingContext.addTriedHost(addr, header.getCode());
                switch (header.getCode()) {
                    case SUCCESS:
                        // success handling is done per stream op
                        break;
                    case FOUND:
                        handleRedirectResponse(header, op, addr);
                        break;
                    // for overcapacity, dont report failure since this normally happens quite a bit
                    case OVER_CAPACITY:
                        logger.debug("Failed to write request to {} : {}", op.stream, header);
                        op.fail(addr, DLException.of(header.getCode(), header.getErrMsg(), header.getLocation()));
                        break;
                    // for responses that indicate the requests definitely failed,
                    // we should fail them immediately (e.g. TOO_LARGE_RECORD, METADATA_EXCEPTION)
                    case NOT_IMPLEMENTED:
                    case METADATA_EXCEPTION:
                    case LOG_EMPTY:
                    case LOG_NOT_FOUND:
                    case TRUNCATED_TRANSACTION:
                    case END_OF_STREAM:
                    case TRANSACTION_OUT_OF_ORDER:
                    case INVALID_STREAM_NAME:
                    case REQUEST_DENIED:
                    case TOO_LARGE_RECORD:
                    case CHECKSUM_FAILED:
                    // status code NOT_READY is returned if failfast is enabled in the server. don't redirect
                    // since the proxy may still own the stream.
                    case STREAM_NOT_READY:
                        op.fail(addr, DLException.of(header.getCode(), header.getErrMsg(), header.getLocation()));
                        break;
                    case SERVICE_UNAVAILABLE:
                        handleServiceUnavailable(addr, sc, Optional.of(op));
                        break;
                    case REGION_UNAVAILABLE:
                        // region is unavailable, redirect the request to hosts in other region
                        redirect(op, null);
                        break;
                    // Proxy was overloaded and refused to try to acquire the stream. Don't remove ownership, since
                    // we didn't have it in the first place.
                    case TOO_MANY_STREAMS:
                        handleRedirectableError(addr, op, header);
                        break;
                    case STREAM_UNAVAILABLE:
                    case ZOOKEEPER_ERROR:
                    case LOCKING_EXCEPTION:
                    case UNEXPECTED:
                    case INTERRUPTED:
                    case BK_TRANSMIT_ERROR:
                    case FLUSH_TIMEOUT:
                    default:
                        // when we are receiving these exceptions from proxy, it means proxy or the stream is closed
                        // redirect the request.
                        ownershipCache.removeOwnerFromStream(op.stream, addr, StatusCode.getStatusName(header.getCode()));
                        handleRedirectableError(addr, op, header);
                        break;
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                Optional<StreamOp> opOptional = Optional.of(op);
                cause = showRootCause(opOptional, cause);
                clientStats.failProxyRequest(addr, cause, startTimeNanos);
                handleRequestException(addr, sc, opOptional, cause);
            }
        });
    }

    // Response Handlers

    Throwable showRootCause(Optional<StreamOp> op, Throwable cause) {
        if (cause instanceof Failure) {
            Failure failure = (Failure) cause;
            if (failure.isFlagged(Failure.Wrapped())) {
                try {
                    // if it is a wrapped failure, unwrap it first
                    cause = failure.show();
                } catch (IllegalArgumentException iae) {
                    if (op.isPresent()) {
                        logger.warn("Failed to unwrap finagle failure of stream {} : ", op.get().stream, iae);
                    } else {
                        logger.warn("Failed to unwrap finagle failure : ", iae);
                    }
                }
            }
        }
        return cause;
    }

    private void handleRedirectableError(SocketAddress addr,
                                         StreamOp op,
                                         ResponseHeader header) {
        if (streamFailfast) {
            op.fail(addr, DLException.of(header.getCode(), header.getErrMsg(), header.getLocation()));
        } else {
            redirect(op, null);
        }
    }

    void handleServiceUnavailable(SocketAddress addr,
                                  ProxyClient sc,
                                  Optional<StreamOp> op) {
        // service is unavailable, remove it out of routing service
        routingService.removeHost(addr, new ServiceUnavailableException(addr + " is unavailable now."));
        onServerLeft(addr);
        if (op.isPresent()) {
            ownershipCache.removeOwnerFromStream(op.get().stream, addr, addr + " is unavailable now.");
            // redirect the request to other host.
            redirect(op.get(), null);
        }
    }

    void handleRequestException(SocketAddress addr,
                                ProxyClient sc,
                                Optional<StreamOp> op,
                                Throwable cause) {
        boolean resendOp = false;
        boolean removeOwnerFromStream = false;
        SocketAddress previousAddr = addr;
        String reason = cause.getMessage();
        if (cause instanceof ConnectionFailedException || cause instanceof java.net.ConnectException) {
            routingService.removeHost(addr, cause);
            onServerLeft(addr, sc);
            removeOwnerFromStream = true;
            // redirect the request to other host.
            resendOp = true;
        } else if (cause instanceof ChannelException) {
            // java.net.ConnectException typically means connection is refused remotely
            // no process listening on remote address/port.
            if (cause.getCause() instanceof java.net.ConnectException) {
                routingService.removeHost(addr, cause.getCause());
                onServerLeft(addr);
                reason = cause.getCause().getMessage();
            } else {
                routingService.removeHost(addr, cause);
                reason = cause.getMessage();
            }
            removeOwnerFromStream = true;
            // redirect the request to other host.
            resendOp = true;
        } else if (cause instanceof ServiceTimeoutException) {
            // redirect the request to itself again, which will backoff for a while
            resendOp = true;
            previousAddr = null;
        } else if (cause instanceof WriteException) {
            // redirect the request to other host.
            resendOp = true;
        } else if (cause instanceof ServiceException) {
            // redirect the request to other host.
            clientManager.removeClient(addr, sc);
            resendOp = true;
        } else if (cause instanceof Failure) {
            handleFinagleFailure((Failure) cause, op, addr);
        } else {
            // Default handler
            handleException(cause, op, addr);
        }

        if (op.isPresent()) {
            if (removeOwnerFromStream) {
                ownershipCache.removeOwnerFromStream(op.get().stream, addr, reason);
            }
            if (resendOp) {
                doSend(op.get(), previousAddr);
            }
        }
    }

    /**
     * Redirect the request to new proxy <i>newAddr</i>. If <i>newAddr</i> is null,
     * it would pick up a host from routing service.
     *
     * @param op
     *          stream operation
     * @param newAddr
     *          new proxy address
     */
    void redirect(StreamOp op, SocketAddress newAddr) {
        ownershipCache.getOwnershipStatsLogger().onRedirect(op.stream);
        if (null != newAddr) {
            logger.debug("Redirect request {} to new owner {}.", op, newAddr);
            op.send(newAddr);
        } else {
            doSend(op, null);
        }
    }

    void handleFinagleFailure(Failure failure,
                              Optional<StreamOp> op,
                              SocketAddress addr) {
        if (failure.isFlagged(Failure.Restartable())) {
            if (op.isPresent()) {
                // redirect the request to other host
                doSend(op.get(), addr);
            }
        } else {
            // fail the request if it is other types of failures
            handleException(failure, op, addr);
        }
    }

    void handleException(Throwable cause,
                         Optional<StreamOp> op,
                         SocketAddress addr) {
        // RequestTimeoutException: fail it and let client decide whether to retry or not.

        // FailedFastException:
        // We don't actually know when FailedFastException will be thrown
        // so properly we just throw it back to application to let application
        // handle it.

        // Other Exceptions: as we don't know how to handle them properly so throw them to client
        if (op.isPresent()) {
            logger.error("Failed to write request to {} @ {} : {}",
                    new Object[]{op.get().stream, addr, cause.toString()});
            op.get().fail(addr, cause);
        }
    }

    void handleRedirectResponse(ResponseHeader header, StreamOp op, SocketAddress curAddr) {
        SocketAddress ownerAddr = null;
        if (header.getLocation().isPresent()) {
            String owner = header.getLocation().get();
            try {
                ownerAddr = DLSocketAddress.deserialize(owner).getSocketAddress();
                // if we are receiving a direct request to same host, we won't try the same host.
                // as the proxy will shut itself down if it redirects client to itself.
                if (curAddr.equals(ownerAddr)) {
                    logger.warn("Request to stream {} is redirected to same server {}!", op.stream, curAddr);
                    ownerAddr = null;
                } else {
                    // update ownership when redirects.
                    ownershipCache.updateOwner(op.stream, ownerAddr);
                }
            } catch (IOException e) {
                ownerAddr = null;
            }
        }
        redirect(op, ownerAddr);
    }

    void updateOwnership(String stream, String location) {
        try {
            SocketAddress ownerAddr = DLSocketAddress.deserialize(location).getSocketAddress();
            // update ownership
            ownershipCache.updateOwner(stream, ownerAddr);
        } catch (IOException e) {
            logger.warn("Invalid ownership {} found for stream {} : ",
                        new Object[] { location, stream, e });
        }
    }

}
