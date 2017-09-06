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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.AlreadyClosedException;
import org.apache.distributedlog.exceptions.DLException;
import org.apache.distributedlog.exceptions.OverCapacityException;
import org.apache.distributedlog.exceptions.OwnershipAcquireFailedException;
import org.apache.distributedlog.exceptions.StatusCode;
import org.apache.distributedlog.exceptions.StreamNotReadyException;
import org.apache.distributedlog.exceptions.StreamUnavailableException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.io.Abortables;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.protocol.util.TwitterFutureUtils;
import org.apache.distributedlog.service.FatalErrorHandler;
import org.apache.distributedlog.service.ServerFeatureKeys;
import org.apache.distributedlog.service.config.ServerConfiguration;
import org.apache.distributedlog.service.config.StreamConfigProvider;
import org.apache.distributedlog.service.stream.limiter.StreamRequestLimiter;
import org.apache.distributedlog.service.streamset.Partition;
import org.apache.distributedlog.common.stats.BroadCastStatsLogger;
import org.apache.distributedlog.common.concurrent.FutureUtils;
import org.apache.distributedlog.util.OrderedScheduler;
import org.apache.distributedlog.util.TimeSequencer;
import org.apache.distributedlog.util.Utils;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Timer;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/**
 * Implementation of {@link Stream}.
 */
public class StreamImpl implements Stream {

    private static final Logger logger = LoggerFactory.getLogger(StreamImpl.class);

    /**
     * The status of the stream.
     *
     * <p>The status change of the stream should just go in one direction. If a stream hits
     * any error, the stream should be put in error state. If a stream is in error state,
     * it should be removed and not reused anymore.
     */
    public enum StreamStatus {
        UNINITIALIZED(-1),
        INITIALIZING(0),
        INITIALIZED(1),
        CLOSING(-4),
        CLOSED(-5),
        // if a stream is in error state, it should be abort during closing.
        ERROR(-6);

        final int code;

        StreamStatus(int code) {
            this.code = code;
        }

        int getCode() {
            return code;
        }

        public static boolean isUnavailable(StreamStatus status) {
            return StreamStatus.ERROR == status || StreamStatus.CLOSING == status || StreamStatus.CLOSED == status;
        }
    }

    private final String name;
    private final Partition partition;
    private DistributedLogManager manager;

    private volatile AsyncLogWriter writer;
    private volatile StreamStatus status;
    private volatile String owner;
    private volatile Throwable lastException;
    private volatile Queue<StreamOp> pendingOps = new ArrayDeque<StreamOp>();

    private final Promise<Void> closePromise = new Promise<Void>();
    private final Object txnLock = new Object();
    private final TimeSequencer sequencer = new TimeSequencer();
    private final StreamRequestLimiter limiter;
    private final DynamicDistributedLogConfiguration dynConf;
    private final DistributedLogConfiguration dlConfig;
    private final Namespace dlNamespace;
    private final String clientId;
    private final OrderedScheduler scheduler;
    private final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final Feature featureRateLimitDisabled;
    private final StreamManager streamManager;
    private final StreamConfigProvider streamConfigProvider;
    private final FatalErrorHandler fatalErrorHandler;
    private final long streamProbationTimeoutMs;
    private final long serviceTimeoutMs;
    private final long writerCloseTimeoutMs;
    private final boolean failFastOnStreamNotReady;
    private final HashedWheelTimer requestTimer;
    private final Timer futureTimer;

    // Stats
    private final StatsLogger streamLogger;
    private final StatsLogger streamExceptionStatLogger;
    private final StatsLogger limiterStatLogger;
    private final Counter serviceTimeout;
    private final OpStatsLogger streamAcquireStat;
    private final OpStatsLogger writerCloseStatLogger;
    private final Counter pendingOpsCounter;
    private final Counter unexpectedExceptions;
    private final Counter writerCloseTimeoutCounter;
    private final StatsLogger exceptionStatLogger;
    private final ConcurrentHashMap<String, Counter> exceptionCounters =
        new ConcurrentHashMap<String, Counter>();
    private final Gauge<Number> streamStatusGauge;

    // Since we may create and discard streams at initialization if there's a race,
    // must not do any expensive initialization here (particularly any locking or
    // significant resource allocation etc.).
    StreamImpl(final String name,
               final Partition partition,
               String clientId,
               StreamManager streamManager,
               StreamOpStats streamOpStats,
               ServerConfiguration serverConfig,
               DistributedLogConfiguration dlConfig,
               DynamicDistributedLogConfiguration streamConf,
               FeatureProvider featureProvider,
               StreamConfigProvider streamConfigProvider,
               Namespace dlNamespace,
               OrderedScheduler scheduler,
               FatalErrorHandler fatalErrorHandler,
               HashedWheelTimer requestTimer,
               Timer futureTimer) {
        this.clientId = clientId;
        this.dlConfig = dlConfig;
        this.streamManager = streamManager;
        this.name = name;
        this.partition = partition;
        this.status = StreamStatus.UNINITIALIZED;
        this.lastException = new IOException("Fail to write record to stream " + name);
        this.streamConfigProvider = streamConfigProvider;
        this.dlNamespace = dlNamespace;
        this.featureRateLimitDisabled = featureProvider.getFeature(
            ServerFeatureKeys.SERVICE_RATE_LIMIT_DISABLED.name().toLowerCase());
        this.scheduler = scheduler;
        this.serviceTimeoutMs = serverConfig.getServiceTimeoutMs();
        this.streamProbationTimeoutMs = serverConfig.getStreamProbationTimeoutMs();
        this.writerCloseTimeoutMs = serverConfig.getWriterCloseTimeoutMs();
        this.failFastOnStreamNotReady = dlConfig.getFailFastOnStreamNotReady();
        this.fatalErrorHandler = fatalErrorHandler;
        this.dynConf = streamConf;
        StatsLogger limiterStatsLogger = BroadCastStatsLogger.two(
            streamOpStats.baseScope("stream_limiter"),
            streamOpStats.streamRequestScope(partition, "limiter"));
        this.limiter = new StreamRequestLimiter(name, dynConf, limiterStatsLogger, featureRateLimitDisabled);
        this.requestTimer = requestTimer;
        this.futureTimer = futureTimer;

        // Stats
        this.streamLogger = streamOpStats.streamRequestStatsLogger(partition);
        this.limiterStatLogger = streamOpStats.baseScope("request_limiter");
        this.streamExceptionStatLogger = streamLogger.scope("exceptions");
        this.serviceTimeout = streamOpStats.baseCounter("serviceTimeout");
        StatsLogger streamsStatsLogger = streamOpStats.baseScope("streams");
        this.streamAcquireStat = streamsStatsLogger.getOpStatsLogger("acquire");
        this.pendingOpsCounter = streamOpStats.baseCounter("pending_ops");
        this.unexpectedExceptions = streamOpStats.baseCounter("unexpected_exceptions");
        this.exceptionStatLogger = streamOpStats.requestScope("exceptions");
        this.writerCloseStatLogger = streamsStatsLogger.getOpStatsLogger("writer_close");
        this.writerCloseTimeoutCounter = streamsStatsLogger.getCounter("writer_close_timeouts");
        // Gauges
        this.streamStatusGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return StreamStatus.UNINITIALIZED.getCode();
            }
            @Override
            public Number getSample() {
                return status.getCode();
            }
        };
    }

    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public String getStreamName() {
        return name;
    }

    @Override
    public DynamicDistributedLogConfiguration getStreamConfiguration() {
        return dynConf;
    }

    @Override
    public Partition getPartition() {
        return partition;
    }

    private DistributedLogManager openLog(String name) throws IOException {
        java.util.Optional<DistributedLogConfiguration> dlConf = java.util.Optional.empty();
        java.util.Optional<DynamicDistributedLogConfiguration> dynDlConf = java.util.Optional.of(dynConf);
        java.util.Optional<StatsLogger> perStreamStatsLogger = java.util.Optional.of(streamLogger);
        return dlNamespace.openLog(name, dlConf, dynDlConf, perStreamStatsLogger);
    }

    // Expensive initialization, only called once per stream.
    @Override
    public void initialize() throws IOException {
        manager = openLog(name);

        // Better to avoid registering the gauge multiple times, so do this in init
        // which only gets called once.
        streamLogger.registerGauge("stream_status", this.streamStatusGauge);

        // Signal initialization is complete, should be last in this method.
        status = StreamStatus.INITIALIZING;
    }

    @Override
    public String toString() {
        return String.format("Stream:%s, %s, %s Status:%s", name, manager, writer, status);
    }

    @Override
    public void start() {
        // acquire the stream
        acquireStream().addEventListener(new FutureEventListener<Boolean>() {
                @Override
                public void onSuccess(Boolean success) {
                    if (!success) {
                        // failed to acquire the stream. set the stream in error status and close it.
                        setStreamInErrorStatus();
                        requestClose("Failed to acquire the ownership");
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    // unhandled exceptions
                    logger.error("Stream {} threw unhandled exception : ", name, cause);
                    // failed to acquire the stream. set the stream in error status and close it.
                    setStreamInErrorStatus();
                    requestClose("Unhandled exception");
                }
            });
    }

    //
    // Stats Operations
    //

    void countException(Throwable t, StatsLogger streamExceptionLogger) {
        String exceptionName = null == t ? "null" : t.getClass().getName();
        Counter counter = exceptionCounters.get(exceptionName);
        if (null == counter) {
            counter = exceptionStatLogger.getCounter(exceptionName);
            Counter oldCounter = exceptionCounters.putIfAbsent(exceptionName, counter);
            if (null != oldCounter) {
                counter = oldCounter;
            }
        }
        counter.inc();
        streamExceptionLogger.getCounter(exceptionName).inc();
    }

    boolean isCriticalException(Throwable cause) {
        return !(cause instanceof OwnershipAcquireFailedException);
    }

    //
    // Service Timeout:
    // - schedule a timeout function to handle operation timeouts: {@link #handleServiceTimeout(String)}
    // - if the operation is completed within timeout period, cancel the timeout.
    //

    void scheduleTimeout(final StreamOp op) {
        final Timeout timeout = requestTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                if (!timeout.isCancelled()) {
                    serviceTimeout.inc();
                    handleServiceTimeout("Operation " + op.getClass().getName() + " timeout");
                }
            }
        }, serviceTimeoutMs, TimeUnit.MILLISECONDS);
        op.responseHeader().ensure(new Function0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                timeout.cancel();
                return null;
            }
        });
    }

    /**
     * Close the stream and schedule cache eviction at some point in the future.
     * We delay this as a way to place the stream in a probationary state--cached
     * in the proxy but unusable.
     * This mechanism helps the cluster adapt to situations where a proxy has
     * persistent connectivity/availability issues, because it keeps an affected
     * stream off the proxy for a period of time, hopefully long enough for the
     * issues to be resolved, or for whoop to kick in and kill the shard.
     */
    void handleServiceTimeout(String reason) {
        synchronized (this) {
            if (StreamStatus.isUnavailable(status)) {
                return;
            }
            // Mark stream in error state
            setStreamInErrorStatus();
        }

        // Async close request, and schedule eviction when its done.
        Future<Void> closeFuture = requestClose(reason, false /* dont remove */);
        closeFuture.onSuccess(new AbstractFunction1<Void, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Void result) {
                streamManager.scheduleRemoval(StreamImpl.this, streamProbationTimeoutMs);
                return BoxedUnit.UNIT;
            }
        });
    }

    //
    // Submit the operation to the stream.
    //

    /**
     * Execute the StreamOp. If reacquire is needed, this may initiate reacquire and queue the op for
     * execution once complete.
     *
     * @param op
     *          stream operation to execute.
     */
    @Override
    public void submit(StreamOp op) {
        try {
            limiter.apply(op);
        } catch (OverCapacityException ex) {
            op.fail(ex);
            return;
        }

        // Timeout stream op if requested.
        if (serviceTimeoutMs > 0) {
            scheduleTimeout(op);
        }

        boolean completeOpNow = false;
        boolean success = true;
        if (StreamStatus.isUnavailable(status)) {
            // Stream is closed, fail the op immediately
            op.fail(new StreamUnavailableException("Stream " + name + " is closed."));
            return;
        } else if (StreamStatus.INITIALIZED == status && writer != null) {
            completeOpNow = true;
            success = true;
        } else {
            synchronized (this) {
                if (StreamStatus.isUnavailable(status)) {
                    // Stream is closed, fail the op immediately
                    op.fail(new StreamUnavailableException("Stream " + name + " is closed."));
                    return;
                } else if (StreamStatus.INITIALIZED == status) {
                    completeOpNow = true;
                    success = true;
                } else if (failFastOnStreamNotReady) {
                    op.fail(new StreamNotReadyException("Stream " + name + " is not ready; status = " + status));
                    return;
                } else { // the stream is still initializing
                    pendingOps.add(op);
                    pendingOpsCounter.inc();
                    if (1 == pendingOps.size()) {
                        if (op instanceof HeartbeatOp) {
                            ((HeartbeatOp) op).setWriteControlRecord(true);
                        }
                    }
                }
            }
        }
        if (completeOpNow) {
            executeOp(op, success);
        }
    }

    //
    // Execute operations and handle exceptions on operations
    //

    /**
     * Execute the <i>op</i> immediately.
     *
     * @param op
     *          stream operation to execute.
     * @param success
     *          whether the operation is success or not.
     */
    void executeOp(final StreamOp op, boolean success) {
        final AsyncLogWriter writer;
        final Throwable lastException;
        synchronized (this) {
            writer = this.writer;
            lastException = this.lastException;
        }
        if (null != writer && success) {
            op.execute(writer, sequencer, txnLock)
                    .addEventListener(new FutureEventListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    // nop
                }
                @Override
                public void onFailure(Throwable cause) {
                    boolean countAsException = true;
                    if (cause instanceof DLException) {
                        final DLException dle = (DLException) cause;
                        switch (dle.getCode()) {
                        case StatusCode.FOUND:
                            assert(cause instanceof OwnershipAcquireFailedException);
                            countAsException = false;
                            handleExceptionOnStreamOp(op, cause);
                            break;
                        case StatusCode.ALREADY_CLOSED:
                            assert(cause instanceof AlreadyClosedException);
                            op.fail(cause);
                            handleAlreadyClosedException((AlreadyClosedException) cause);
                            break;
                        // exceptions that mostly from client (e.g. too large record)
                        case StatusCode.NOT_IMPLEMENTED:
                        case StatusCode.METADATA_EXCEPTION:
                        case StatusCode.LOG_EMPTY:
                        case StatusCode.LOG_NOT_FOUND:
                        case StatusCode.TRUNCATED_TRANSACTION:
                        case StatusCode.END_OF_STREAM:
                        case StatusCode.TRANSACTION_OUT_OF_ORDER:
                        case StatusCode.INVALID_STREAM_NAME:
                        case StatusCode.TOO_LARGE_RECORD:
                        case StatusCode.STREAM_NOT_READY:
                        case StatusCode.OVER_CAPACITY:
                            op.fail(cause);
                            break;
                        // the DL writer hits exception, simple set the stream to error status
                        // and fail the request
                        default:
                            handleExceptionOnStreamOp(op, cause);
                            break;
                        }
                    } else {
                        handleExceptionOnStreamOp(op, cause);
                    }
                    if (countAsException) {
                        countException(cause, streamExceptionStatLogger);
                    }
                }
            });
        } else {
            if (null != lastException) {
                op.fail(lastException);
            } else {
                op.fail(new StreamUnavailableException("Stream " + name + " is closed."));
            }
        }
    }

    /**
     * Handle exception when executing <i>op</i>.
     *
     * @param op
     *          stream operation executing
     * @param cause
     *          exception received when executing <i>op</i>
     */
    private void handleExceptionOnStreamOp(StreamOp op, final Throwable cause) {
        AsyncLogWriter oldWriter = null;
        boolean statusChanged = false;
        synchronized (this) {
            if (StreamStatus.INITIALIZED == status) {
                oldWriter = setStreamStatus(StreamStatus.ERROR, StreamStatus.INITIALIZED, null, cause);
                statusChanged = true;
            }
        }
        if (statusChanged) {
            Abortables.asyncAbort(oldWriter, false);
            if (isCriticalException(cause)) {
                logger.error("Failed to write data into stream {} : ", name, cause);
            } else {
                logger.warn("Failed to write data into stream {} : {}", name, cause.getMessage());
            }
            requestClose("Failed to write data into stream " + name + " : " + cause.getMessage());
        }
        op.fail(cause);
    }

    /**
     * Handling already closed exception.
     */
    private void handleAlreadyClosedException(AlreadyClosedException ace) {
        unexpectedExceptions.inc();
        logger.error("Encountered unexpected exception when writing data into stream {} : ", name, ace);
        fatalErrorHandler.notifyFatalError();
    }

    //
    // Acquire streams
    //

    Future<Boolean> acquireStream() {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final Promise<Boolean> acquirePromise = new Promise<Boolean>();
        manager.openAsyncLogWriter().whenCompleteAsync(
            new org.apache.distributedlog.common.concurrent.FutureEventListener<AsyncLogWriter>() {

            @Override
            public void onSuccess(AsyncLogWriter w) {
                onAcquireStreamSuccess(w, stopwatch, acquirePromise);
            }

            @Override
            public void onFailure(Throwable cause) {
                onAcquireStreamFailure(cause, stopwatch, acquirePromise);
            }

        }, scheduler.chooseExecutor(getStreamName()));
        return acquirePromise;
    }

    private void onAcquireStreamSuccess(AsyncLogWriter w,
                                        Stopwatch stopwatch,
                                        Promise<Boolean> acquirePromise) {
        synchronized (txnLock) {
            sequencer.setLastId(w.getLastTxId());
        }
        AsyncLogWriter oldWriter;
        Queue<StreamOp> oldPendingOps;
        boolean success;
        synchronized (StreamImpl.this) {
            oldWriter = setStreamStatus(StreamStatus.INITIALIZED,
                    StreamStatus.INITIALIZING, w, null);
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<StreamOp>();
            success = true;
        }
        // check if the stream is allowed to be acquired
        if (!streamManager.allowAcquire(StreamImpl.this)) {
            if (null != oldWriter) {
                Abortables.asyncAbort(oldWriter, true);
            }
            int maxAcquiredPartitions = dynConf.getMaxAcquiredPartitionsPerProxy();
            StreamUnavailableException sue = new StreamUnavailableException("Stream " + partition.getStream()
                    + " is not allowed to acquire more than " + maxAcquiredPartitions + " partitions");
            countException(sue, exceptionStatLogger);
            logger.error("Failed to acquire stream {} because it is unavailable : {}",
                    name, sue.getMessage());
            synchronized (this) {
                oldWriter = setStreamStatus(StreamStatus.ERROR,
                        StreamStatus.INITIALIZED, null, sue);
                // we don't switch the pending ops since they are already switched
                // when setting the status to initialized
                success = false;
            }
        }
        processPendingRequestsAfterAcquire(success, oldWriter, oldPendingOps, stopwatch, acquirePromise);
    }

    private void onAcquireStreamFailure(Throwable cause,
                                        Stopwatch stopwatch,
                                        Promise<Boolean> acquirePromise) {
        AsyncLogWriter oldWriter;
        Queue<StreamOp> oldPendingOps;
        boolean success;
        if (cause instanceof AlreadyClosedException) {
            countException(cause, streamExceptionStatLogger);
            handleAlreadyClosedException((AlreadyClosedException) cause);
            return;
        } else {
            if (isCriticalException(cause)) {
                countException(cause, streamExceptionStatLogger);
                logger.error("Failed to acquire stream {} : ", name, cause);
            } else {
                logger.warn("Failed to acquire stream {} : {}", name, cause.getMessage());
            }
            synchronized (StreamImpl.this) {
                oldWriter = setStreamStatus(StreamStatus.ERROR,
                        StreamStatus.INITIALIZING, null, cause);
                oldPendingOps = pendingOps;
                pendingOps = new ArrayDeque<StreamOp>();
                success = false;
            }
        }
        processPendingRequestsAfterAcquire(success, oldWriter, oldPendingOps, stopwatch, acquirePromise);
    }

    /**
     * Process the pending request after acquired stream.
     *
     * @param success whether the acquisition succeed or not
     * @param oldWriter the old writer to abort
     * @param oldPendingOps the old pending ops to execute
     * @param stopwatch stopwatch to measure the time spent on acquisition
     * @param acquirePromise the promise to complete the acquire operation
     */
    void processPendingRequestsAfterAcquire(boolean success,
                                            AsyncLogWriter oldWriter,
                                            Queue<StreamOp> oldPendingOps,
                                            Stopwatch stopwatch,
                                            Promise<Boolean> acquirePromise) {
        if (success) {
            streamAcquireStat.registerSuccessfulEvent(
              stopwatch.elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
        } else {
            streamAcquireStat.registerFailedEvent(
              stopwatch.elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
        }
        for (StreamOp op : oldPendingOps) {
            executeOp(op, success);
            pendingOpsCounter.dec();
        }
        Abortables.asyncAbort(oldWriter, true);
        TwitterFutureUtils.setValue(acquirePromise, success);
    }

    //
    // Stream Status Changes
    //

    synchronized void setStreamInErrorStatus() {
        if (StreamStatus.CLOSING == status || StreamStatus.CLOSED == status) {
            return;
        }
        this.status = StreamStatus.ERROR;
    }

    /**
     * Update the stream status. The changes are only applied when there isn't status changed.
     *
     * @param newStatus
     *          new status
     * @param oldStatus
     *          old status
     * @param writer
     *          new log writer
     * @param t
     *          new exception
     * @return old writer if it exists
     */
    synchronized AsyncLogWriter setStreamStatus(StreamStatus newStatus,
                                                StreamStatus oldStatus,
                                                AsyncLogWriter writer,
                                                Throwable t) {
        if (oldStatus != this.status) {
            logger.info("Stream {} status already changed from {} -> {} when trying to change it to {}",
                    new Object[] { name, oldStatus, this.status, newStatus });
            return null;
        }

        String owner = null;
        if (t instanceof OwnershipAcquireFailedException) {
            owner = ((OwnershipAcquireFailedException) t).getCurrentOwner();
        }

        AsyncLogWriter oldWriter = this.writer;
        this.writer = writer;
        if (null != owner && owner.equals(clientId)) {
            unexpectedExceptions.inc();
            logger.error("I am waiting myself {} to release lock on stream {}, so have to shut myself down :",
                         new Object[] { owner, name, t });
            // I lost the ownership but left a lock over zookeeper
            // I should not ask client to redirect to myself again as I can't handle it :(
            // shutdown myself
            fatalErrorHandler.notifyFatalError();
            this.owner = null;
        } else {
            this.owner = owner;
        }
        this.lastException = t;
        this.status = newStatus;
        if (StreamStatus.INITIALIZED == newStatus) {
            streamManager.notifyAcquired(this);
            logger.info("Inserted acquired stream {} -> writer {}", name, this);
        } else {
            streamManager.notifyReleased(this);
            logger.info("Removed acquired stream {} -> writer {}", name, this);
        }
        return oldWriter;
    }

    //
    // Stream Close Functions
    //

    void close(DistributedLogManager dlm) {
        if (null != dlm) {
            try {
                dlm.close();
            } catch (IOException ioe) {
                logger.warn("Failed to close dlm for {} : ", name, ioe);
            }
        }
    }

    @Override
    public Future<Void> requestClose(String reason) {
        return requestClose(reason, true);
    }

    Future<Void> requestClose(String reason, boolean uncache) {
        final boolean abort;
        closeLock.writeLock().lock();
        try {
            if (StreamStatus.CLOSING == status
                || StreamStatus.CLOSED == status) {
                return closePromise;
            }
            logger.info("Request to close stream {} : {}", getStreamName(), reason);
            // if the stream isn't closed from INITIALIZED state, we abort the stream instead of closing it.
            abort = StreamStatus.INITIALIZED != status;
            status = StreamStatus.CLOSING;
            streamManager.notifyReleased(this);
        } finally {
            closeLock.writeLock().unlock();
        }
        // we will fail the requests that are coming in between closing and closed only
        // after the async writer is closed. so we could clear up the lock before redirect
        // them.
        close(abort, uncache);
        return closePromise;
    }

    @Override
    public void delete() throws IOException {
        if (null != writer) {
            Utils.close(writer);
            synchronized (this) {
                writer = null;
                lastException = new StreamUnavailableException("Stream was deleted");
            }
        }
        if (null == manager) {
            throw new UnexpectedException("No stream " + name + " to delete");
        }
        manager.delete();
    }

    /**
     * Post action executed after closing.
     */
    private void postClose(boolean uncache) {
        closeManagerAndErrorOutPendingRequests();
        unregisterGauge();
        if (uncache) {
            if (null != owner) {
                long probationTimeoutMs = 2 * dlConfig.getZKSessionTimeoutMilliseconds() / 3;
                streamManager.scheduleRemoval(this, probationTimeoutMs);
            } else {
                streamManager.notifyRemoved(this);
                logger.info("Removed cached stream {}.", getStreamName());
            }
        }
        TwitterFutureUtils.setValue(closePromise, null);
    }

    /**
     * Shouldn't call close directly. The callers should call #requestClose instead
     *
     * @param shouldAbort shall we abort the stream instead of closing
     */
    private Future<Void> close(boolean shouldAbort, final boolean uncache) {
        boolean abort;
        closeLock.writeLock().lock();
        try {
            if (StreamStatus.CLOSED == status) {
                return closePromise;
            }
            abort = shouldAbort || (StreamStatus.INITIALIZED != status && StreamStatus.CLOSING != status);
            status = StreamStatus.CLOSED;
            streamManager.notifyReleased(this);
        } finally {
            closeLock.writeLock().unlock();
        }
        logger.info("Closing stream {} ...", name);
        // Close the writers to release the locks before failing the requests
        CompletableFuture<Void> closeWriterFuture;
        if (abort) {
            closeWriterFuture = Abortables.asyncAbort(writer, true);
        } else {
            closeWriterFuture = Utils.asyncClose(writer, true);
        }
        // close the manager and error out pending requests after close writer
        Duration closeWaitDuration;
        if (writerCloseTimeoutMs <= 0) {
            closeWaitDuration = Duration.Top();
        } else {
            closeWaitDuration = Duration.fromMilliseconds(writerCloseTimeoutMs);
        }

        CompletableFuture<Void> maskedFuture = FutureUtils.createFuture();
        FutureUtils.proxyTo(
            FutureUtils.stats(
                closeWriterFuture,
                writerCloseStatLogger,
                Stopwatch.createStarted()
            ),
            maskedFuture);

        FutureUtils.within(
            maskedFuture,
            closeWaitDuration.inMillis(),
            TimeUnit.MILLISECONDS,
            new java.util.concurrent.TimeoutException("Timeout on closing"),
            scheduler,
            name
        ).whenCompleteAsync(
            new org.apache.distributedlog.common.concurrent.FutureEventListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    postClose(uncache);
                }

                @Override
                public void onFailure(Throwable cause) {
                    if (cause instanceof java.util.concurrent.TimeoutException) {
                        writerCloseTimeoutCounter.inc();
                    }
                }
            },
            scheduler.chooseExecutor(name)
        );
        return closePromise;
    }

    private void closeManagerAndErrorOutPendingRequests() {
        close(manager);
        // Failed the pending requests.
        Queue<StreamOp> oldPendingOps;
        synchronized (this) {
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<StreamOp>();
        }
        StreamUnavailableException closingException =
                new StreamUnavailableException("Stream " + name + " is closed.");
        for (StreamOp op : oldPendingOps) {
            op.fail(closingException);
            pendingOpsCounter.dec();
        }
        limiter.close();
        logger.info("Closed stream {}.", name);
    }

    /**
     * clean up the gauge to help GC.
     */
    private void unregisterGauge(){
        streamLogger.unregisterGauge("stream_status", this.streamStatusGauge);
    }

    // Test-only apis

    @VisibleForTesting
    public int numPendingOps() {
        Queue<StreamOp> queue = pendingOps;
        return null == queue ? 0 : queue.size();
    }

    @VisibleForTesting
    public StreamStatus getStatus() {
        return status;
    }

    @VisibleForTesting
    public void setStatus(StreamStatus status) {
        this.status = status;
    }

    @VisibleForTesting
    public AsyncLogWriter getWriter() {
        return writer;
    }

    @VisibleForTesting
    public DistributedLogManager getManager() {
        return manager;
    }

    @VisibleForTesting
    public Throwable getLastException() {
        return lastException;
    }

    @VisibleForTesting
    public Future<Void> getCloseFuture() {
        return closePromise;
    }
}
