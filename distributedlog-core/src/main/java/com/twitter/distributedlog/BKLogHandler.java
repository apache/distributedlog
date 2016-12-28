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
package com.twitter.distributedlog;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.callback.LogSegmentNamesListener;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.exceptions.LogSegmentNotFoundException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadata;
import com.twitter.distributedlog.io.AsyncAbortable;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataCache;
import com.twitter.distributedlog.logsegment.PerStreamLogSegmentCache;
import com.twitter.distributedlog.logsegment.LogSegmentFilter;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The base class about log handler on managing log segments.
 *
 * <h3>Metrics</h3>
 * The log handler is a base class on managing log segments. so all the metrics
 * here are related to log segments retrieval and exposed under `logsegments`.
 * These metrics are all OpStats, in the format of <code>`scope`/logsegments/`op`</code>.
 * <p>
 * Those operations are:
 * <ul>
 * <li>get_inprogress_segment: time between the inprogress log segment created and
 * the handler read it.
 * <li>get_completed_segment: time between a log segment is turned to completed and
 * the handler read it.
 * <li>negative_get_inprogress_segment: record the negative values for `get_inprogress_segment`.
 * <li>negative_get_completed_segment: record the negative values for `get_completed_segment`.
 * <li>recover_last_entry: recovering last entry from a log segment
 * <li>recover_scanned_entries: the number of entries that are scanned during recovering.
 * </ul>
 * @see BKLogWriteHandler
 * @see BKLogReadHandler
 */
public abstract class BKLogHandler implements AsyncCloseable, AsyncAbortable {
    static final Logger LOG = LoggerFactory.getLogger(BKLogHandler.class);

    protected final ZKLogMetadata logMetadata;
    protected final DistributedLogConfiguration conf;
    protected final ZooKeeperClient zooKeeperClient;
    protected final BookKeeperClient bookKeeperClient;
    protected final LogSegmentMetadataStore metadataStore;
    protected final LogSegmentMetadataCache metadataCache;
    protected final int firstNumEntriesPerReadLastRecordScan;
    protected final int maxNumEntriesPerReadLastRecordScan;
    protected volatile long lastLedgerRollingTimeMillis = -1;
    protected final OrderedScheduler scheduler;
    protected final StatsLogger statsLogger;
    protected final AlertStatsLogger alertStatsLogger;
    protected volatile boolean reportGetSegmentStats = false;
    private final String lockClientId;
    protected final AtomicReference<IOException> metadataException = new AtomicReference<IOException>(null);

    // Maintain the list of log segments per stream
    protected final PerStreamLogSegmentCache logSegmentCache;



    // trace
    protected final long metadataLatencyWarnThresholdMillis;

    // Stats
    private final OpStatsLogger getInprogressSegmentStat;
    private final OpStatsLogger getCompletedSegmentStat;
    private final OpStatsLogger negativeGetInprogressSegmentStat;
    private final OpStatsLogger negativeGetCompletedSegmentStat;
    private final OpStatsLogger recoverLastEntryStats;
    private final OpStatsLogger recoverScannedEntriesStats;

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogHandler(ZKLogMetadata metadata,
                 DistributedLogConfiguration conf,
                 ZooKeeperClientBuilder zkcBuilder,
                 BookKeeperClientBuilder bkcBuilder,
                 LogSegmentMetadataStore metadataStore,
                 LogSegmentMetadataCache metadataCache,
                 OrderedScheduler scheduler,
                 StatsLogger statsLogger,
                 AlertStatsLogger alertStatsLogger,
                 String lockClientId) {
        Preconditions.checkNotNull(zkcBuilder);
        Preconditions.checkNotNull(bkcBuilder);
        this.logMetadata = metadata;
        this.conf = conf;
        this.scheduler = scheduler;
        this.statsLogger = statsLogger;
        this.alertStatsLogger = alertStatsLogger;
        this.logSegmentCache = new PerStreamLogSegmentCache(
                metadata.getLogName(),
                conf.isLogSegmentSequenceNumberValidationEnabled());

        firstNumEntriesPerReadLastRecordScan = conf.getFirstNumEntriesPerReadLastRecordScan();
        maxNumEntriesPerReadLastRecordScan = conf.getMaxNumEntriesPerReadLastRecordScan();
        this.zooKeeperClient = zkcBuilder.build();
        LOG.debug("Using ZK Path {}", logMetadata.getLogRootPath());
        this.bookKeeperClient = bkcBuilder.build();
        this.metadataStore = metadataStore;
        this.metadataCache = metadataCache;
        this.lockClientId = lockClientId;

        // Traces
        this.metadataLatencyWarnThresholdMillis = conf.getMetadataLatencyWarnThresholdMillis();

        // Stats
        StatsLogger segmentsLogger = statsLogger.scope("logsegments");
        getInprogressSegmentStat = segmentsLogger.getOpStatsLogger("get_inprogress_segment");
        getCompletedSegmentStat = segmentsLogger.getOpStatsLogger("get_completed_segment");
        negativeGetInprogressSegmentStat = segmentsLogger.getOpStatsLogger("negative_get_inprogress_segment");
        negativeGetCompletedSegmentStat = segmentsLogger.getOpStatsLogger("negative_get_completed_segment");
        recoverLastEntryStats = segmentsLogger.getOpStatsLogger("recover_last_entry");
        recoverScannedEntriesStats = segmentsLogger.getOpStatsLogger("recover_scanned_entries");
    }

    BKLogHandler checkMetadataException() throws IOException {
        if (null != metadataException.get()) {
            throw metadataException.get();
        }
        return this;
    }

    public void reportGetSegmentStats(boolean enabled) {
        this.reportGetSegmentStats = enabled;
    }

    public String getLockClientId() {
        return lockClientId;
    }

    public Future<LogRecordWithDLSN> asyncGetFirstLogRecord() {
        final Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();
        checkLogStreamExistsAsync().addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                readLogSegmentsFromStore(
                        LogSegmentMetadata.COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null
                ).addEventListener(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {

                    @Override
                    public void onSuccess(Versioned<List<LogSegmentMetadata>> ledgerList) {
                        if (ledgerList.getValue().isEmpty()) {
                            promise.setException(new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
                            return;
                        }
                        Future<LogRecordWithDLSN> firstRecord = null;
                        for (LogSegmentMetadata ledger : ledgerList.getValue()) {
                            if (!ledger.isTruncated() && (ledger.getRecordCount() > 0 || ledger.isInProgress())) {
                                firstRecord = asyncReadFirstUserRecord(ledger, DLSN.InitialDLSN);
                                break;
                            }
                        }
                        if (null != firstRecord) {
                            promise.become(firstRecord);
                        } else {
                            promise.setException(new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
                        }
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.setException(cause);
                    }
                });
            }

            @Override
            public void onFailure(Throwable cause) {
                promise.setException(cause);
            }
        });
        return promise;
    }

    public Future<LogRecordWithDLSN> getLastLogRecordAsync(final boolean recover, final boolean includeEndOfStream) {
        final Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();
        checkLogStreamExistsAsync().addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                readLogSegmentsFromStore(
                        LogSegmentMetadata.DESC_COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null
                ).addEventListener(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {

                    @Override
                    public void onSuccess(Versioned<List<LogSegmentMetadata>> ledgerList) {
                        if (ledgerList.getValue().isEmpty()) {
                            promise.setException(
                                    new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
                            return;
                        }
                        asyncGetLastLogRecord(
                                ledgerList.getValue().iterator(),
                                promise,
                                recover,
                                false,
                                includeEndOfStream);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.setException(cause);
                    }
                });
            }

            @Override
            public void onFailure(Throwable cause) {
                promise.setException(cause);
            }
        });
        return promise;
    }

    private void asyncGetLastLogRecord(final Iterator<LogSegmentMetadata> ledgerIter,
                                       final Promise<LogRecordWithDLSN> promise,
                                       final boolean fence,
                                       final boolean includeControlRecord,
                                       final boolean includeEndOfStream) {
        if (ledgerIter.hasNext()) {
            LogSegmentMetadata metadata = ledgerIter.next();
            asyncReadLastRecord(metadata, fence, includeControlRecord, includeEndOfStream).addEventListener(
                    new FutureEventListener<LogRecordWithDLSN>() {
                        @Override
                        public void onSuccess(LogRecordWithDLSN record) {
                            if (null == record) {
                                asyncGetLastLogRecord(ledgerIter, promise, fence, includeControlRecord, includeEndOfStream);
                            } else {
                                promise.setValue(record);
                            }
                        }

                        @Override
                        public void onFailure(Throwable cause) {
                            promise.setException(cause);
                        }
                    }
            );
        } else {
            promise.setException(new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
        }
    }

    private Future<LogRecordWithDLSN> asyncReadFirstUserRecord(LogSegmentMetadata ledger, DLSN beginDLSN) {
        final LedgerHandleCache handleCache =
                LedgerHandleCache.newBuilder().bkc(bookKeeperClient).conf(conf).build();
        return ReadUtils.asyncReadFirstUserRecord(
                getFullyQualifiedName(),
                ledger,
                firstNumEntriesPerReadLastRecordScan,
                maxNumEntriesPerReadLastRecordScan,
                new AtomicInteger(0),
                scheduler,
                handleCache,
                beginDLSN
        ).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCache.clear();
                return BoxedUnit.UNIT;
            }
        });
    }

    /**
     * This is a helper method to compactly return the record count between two records, the first denoted by
     * beginDLSN and the second denoted by endPosition. Its up to the caller to ensure that endPosition refers to
     * position in the same ledger as beginDLSN.
     */
    private Future<Long> asyncGetLogRecordCount(LogSegmentMetadata ledger, final DLSN beginDLSN, final long endPosition) {
        return asyncReadFirstUserRecord(ledger, beginDLSN).map(new Function<LogRecordWithDLSN, Long>() {
            public Long apply(final LogRecordWithDLSN beginRecord) {
                long recordCount = 0;
                if (null != beginRecord) {
                    recordCount = endPosition + 1 - beginRecord.getLastPositionWithinLogSegment();
                }
                return recordCount;
            }
        });
    }

    /**
     * Ledger metadata tells us how many records are in each completed segment, but for the first and last segments
     * we may have to crack open the entry and count. For the first entry, we need to do so because beginDLSN may be
     * an interior entry. For the last entry, if it is inprogress, we need to recover it and find the last user
     * entry.
     */
    private Future<Long> asyncGetLogRecordCount(final LogSegmentMetadata ledger, final DLSN beginDLSN) {
        if (ledger.isInProgress() && ledger.isDLSNinThisSegment(beginDLSN)) {
            return asyncReadLastUserRecord(ledger).flatMap(new Function<LogRecordWithDLSN, Future<Long>>() {
                public Future<Long> apply(final LogRecordWithDLSN endRecord) {
                    if (null != endRecord) {
                        return asyncGetLogRecordCount(ledger, beginDLSN, endRecord.getLastPositionWithinLogSegment() /* end position */);
                    } else {
                        return Future.value((long) 0);
                    }
                }
            });
        } else if (ledger.isInProgress()) {
            return asyncReadLastUserRecord(ledger).map(new Function<LogRecordWithDLSN, Long>() {
                public Long apply(final LogRecordWithDLSN endRecord) {
                    if (null != endRecord) {
                        return (long) endRecord.getLastPositionWithinLogSegment();
                    } else {
                        return (long) 0;
                    }
                }
            });
        } else if (ledger.isDLSNinThisSegment(beginDLSN)) {
            return asyncGetLogRecordCount(ledger, beginDLSN, ledger.getRecordCount() /* end position */);
        } else {
            return Future.value((long) ledger.getRecordCount());
        }
    }

    /**
     * Get a count of records between beginDLSN and the end of the stream.
     *
     * @param beginDLSN dlsn marking the start of the range
     * @return the count of records present in the range
     */
    public Future<Long> asyncGetLogRecordCount(final DLSN beginDLSN) {

        return checkLogStreamExistsAsync().flatMap(new Function<Void, Future<Long>>() {
            public Future<Long> apply(Void done) {

                return readLogSegmentsFromStore(
                        LogSegmentMetadata.COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null
                ).flatMap(new Function<Versioned<List<LogSegmentMetadata>>, Future<Long>>() {
                    public Future<Long> apply(Versioned<List<LogSegmentMetadata>> ledgerList) {

                        List<Future<Long>> futureCounts = new ArrayList<Future<Long>>(ledgerList.getValue().size());
                        for (LogSegmentMetadata ledger : ledgerList.getValue()) {
                            if (ledger.getLogSegmentSequenceNumber() >= beginDLSN.getLogSegmentSequenceNo()) {
                                futureCounts.add(asyncGetLogRecordCount(ledger, beginDLSN));
                            }
                        }
                        return Future.collect(futureCounts).map(new Function<List<Long>, Long>() {
                            public Long apply(List<Long> counts) {
                                return sum(counts);
                            }
                        });
                    }
                });
            }
        });
    }

    private Long sum(List<Long> values) {
        long sum = 0;
        for (Long value : values) {
            sum += value;
        }
        return sum;
    }

    Future<Void> checkLogStreamExistsAsync() {
        final Promise<Void> promise = new Promise<Void>();
        try {
            final ZooKeeper zk = zooKeeperClient.get();
            zk.sync(logMetadata.getLogSegmentsPath(), new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int syncRc, String path, Object syncCtx) {
                    if (KeeperException.Code.NONODE.intValue() == syncRc) {
                        promise.setException(new LogNotFoundException(
                                String.format("Log %s does not exist or has been deleted", getFullyQualifiedName())));
                        return;
                    } else if (KeeperException.Code.OK.intValue() != syncRc){
                        promise.setException(new ZKException("Error on checking log existence for " + getFullyQualifiedName(),
                                KeeperException.create(KeeperException.Code.get(syncRc))));
                        return;
                    }
                    zk.exists(logMetadata.getLogSegmentsPath(), false, new AsyncCallback.StatCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, Stat stat) {
                            if (KeeperException.Code.OK.intValue() == rc) {
                                promise.setValue(null);
                            } else if (KeeperException.Code.NONODE.intValue() == rc) {
                                promise.setException(new LogNotFoundException(String.format("Log %s does not exist or has been deleted", getFullyQualifiedName())));
                            } else {
                                promise.setException(new ZKException("Error on checking log existence for " + getFullyQualifiedName(),
                                        KeeperException.create(KeeperException.Code.get(rc))));
                            }
                        }
                    }, null);
                }
            }, null);

        } catch (InterruptedException ie) {
            LOG.error("Interrupted while reading {}", logMetadata.getLogSegmentsPath(), ie);
            promise.setException(new DLInterruptedException("Interrupted while checking "
                    + logMetadata.getLogSegmentsPath(), ie));
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.setException(e);
        }
        return promise;
    }

    @Override
    public Future<Void> asyncAbort() {
        return asyncClose();
    }

    public Future<LogRecordWithDLSN> asyncReadLastUserRecord(final LogSegmentMetadata l) {
        return asyncReadLastRecord(l, false, false, false);
    }

    public Future<LogRecordWithDLSN> asyncReadLastRecord(final LogSegmentMetadata l,
                                                         final boolean fence,
                                                         final boolean includeControl,
                                                         final boolean includeEndOfStream) {
        final AtomicInteger numRecordsScanned = new AtomicInteger(0);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final LedgerHandleCache handleCache =
                LedgerHandleCache.newBuilder().bkc(bookKeeperClient).conf(conf).build();
        return ReadUtils.asyncReadLastRecord(
                getFullyQualifiedName(),
                l,
                fence,
                includeControl,
                includeEndOfStream,
                firstNumEntriesPerReadLastRecordScan,
                maxNumEntriesPerReadLastRecordScan,
                numRecordsScanned,
                scheduler,
                handleCache
        ).addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onSuccess(LogRecordWithDLSN value) {
                recoverLastEntryStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                recoverScannedEntriesStats.registerSuccessfulEvent(numRecordsScanned.get());
            }

            @Override
            public void onFailure(Throwable cause) {
                recoverLastEntryStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCache.clear();
                return BoxedUnit.UNIT;
            }
        });
    }

    protected void setLastLedgerRollingTimeMillis(long rollingTimeMillis) {
        if (lastLedgerRollingTimeMillis < rollingTimeMillis) {
            lastLedgerRollingTimeMillis = rollingTimeMillis;
        }
    }

    public String getFullyQualifiedName() {
        return logMetadata.getFullyQualifiedName();
    }

    // Log Segments Related Functions
    //
    // ***Note***
    // Get log segment list should go through #getCachedLogSegments as we need to assign start sequence id
    // for inprogress log segment so the reader could generate the right sequence id.
    //
    // ***PerStreamCache vs LogSegmentMetadataCache **
    // The per stream cache maintains the list of segments per stream, while the metadata cache
    // maintains log segments. The metadata cache is just to reduce the access to zookeeper, it is
    // okay that some of the log segments are not in the cache; however the per stream cache can not
    // have any gaps between log segment sequence numbers which it has to be accurate.

    /**
     * Get the cached log segments.
     *
     * @param comparator the comparator to sort the returned log segments.
     * @return list of sorted log segments
     * @throws UnexpectedException if unexpected condition detected.
     */
    protected List<LogSegmentMetadata> getCachedLogSegments(Comparator<LogSegmentMetadata> comparator)
        throws UnexpectedException {
        try {
            return logSegmentCache.getLogSegments(comparator);
        } catch (UnexpectedException ue) {
            // the log segments cache went wrong
            LOG.error("Unexpected exception on getting log segments from the cache for stream {}",
                    getFullyQualifiedName(), ue);
            metadataException.compareAndSet(null, ue);
            throw ue;
        }
    }

    /**
     * Add the segment <i>metadata</i> for <i>name</i> in the cache.
     *
     * @param name
     *          segment znode name.
     * @param metadata
     *          segment metadata.
     */
    protected void addLogSegmentToCache(String name, LogSegmentMetadata metadata) {
        metadataCache.put(metadata.getZkPath(), metadata);
        logSegmentCache.add(name, metadata);
        // update the last ledger rolling time
        if (!metadata.isInProgress() && (lastLedgerRollingTimeMillis < metadata.getCompletionTime())) {
            lastLedgerRollingTimeMillis = metadata.getCompletionTime();
        }

        if (reportGetSegmentStats) {
            // update stats
            long ts = System.currentTimeMillis();
            if (metadata.isInProgress()) {
                // as we used timestamp as start tx id we could take it as start time
                // NOTE: it is a hack here.
                long elapsedMillis = ts - metadata.getFirstTxId();
                long elapsedMicroSec = TimeUnit.MILLISECONDS.toMicros(elapsedMillis);
                if (elapsedMicroSec > 0) {
                    if (elapsedMillis > metadataLatencyWarnThresholdMillis) {
                        LOG.warn("{} received inprogress log segment in {} millis: {}",
                                 new Object[] { getFullyQualifiedName(), elapsedMillis, metadata });
                    }
                    getInprogressSegmentStat.registerSuccessfulEvent(elapsedMicroSec);
                } else {
                    negativeGetInprogressSegmentStat.registerSuccessfulEvent(-elapsedMicroSec);
                }
            } else {
                long elapsedMillis = ts - metadata.getCompletionTime();
                long elapsedMicroSec = TimeUnit.MILLISECONDS.toMicros(elapsedMillis);
                if (elapsedMicroSec > 0) {
                    if (elapsedMillis > metadataLatencyWarnThresholdMillis) {
                        LOG.warn("{} received completed log segment in {} millis : {}",
                                 new Object[] { getFullyQualifiedName(), elapsedMillis, metadata });
                    }
                    getCompletedSegmentStat.registerSuccessfulEvent(elapsedMicroSec);
                } else {
                    negativeGetCompletedSegmentStat.registerSuccessfulEvent(-elapsedMicroSec);
                }
            }
        }
    }

    /**
     * Read log segment <i>name</i> from the cache.
     *
     * @param name name of the log segment
     * @return log segment metadata
     */
    protected LogSegmentMetadata readLogSegmentFromCache(String name) {
        return logSegmentCache.get(name);
    }

    /**
     * Remove the log segment <i>name</i> from the cache.
     *
     * @param name name of the log segment.
     * @return log segment metadata
     */
    protected LogSegmentMetadata removeLogSegmentFromCache(String name) {
        metadataCache.invalidate(name);
        return logSegmentCache.remove(name);
    }

    /**
     * Update the log segment cache with updated mapping
     *
     * @param logSegmentsRemoved log segments removed
     * @param logSegmentsAdded log segments added
     */
    protected void updateLogSegmentCache(Set<String> logSegmentsRemoved,
                                         Map<String, LogSegmentMetadata> logSegmentsAdded) {
        for (String segmentName : logSegmentsRemoved) {
            metadataCache.invalidate(segmentName);
        }
        for (Map.Entry<String, LogSegmentMetadata> entry : logSegmentsAdded.entrySet()) {
            metadataCache.put(entry.getKey(), entry.getValue());
        }
        logSegmentCache.update(logSegmentsRemoved, logSegmentsAdded);
    }

    /**
     * Read the log segments from the store and register a listener
     * @param comparator
     * @param segmentFilter
     * @param logSegmentNamesListener
     * @return future represents the result of log segments
     */
    public Future<Versioned<List<LogSegmentMetadata>>> readLogSegmentsFromStore(
            final Comparator<LogSegmentMetadata> comparator,
            final LogSegmentFilter segmentFilter,
            final LogSegmentNamesListener logSegmentNamesListener) {
        final Promise<Versioned<List<LogSegmentMetadata>>> readResult =
                new Promise<Versioned<List<LogSegmentMetadata>>>();
        metadataStore.getLogSegmentNames(logMetadata.getLogSegmentsPath(), logSegmentNamesListener)
                .addEventListener(new FutureEventListener<Versioned<List<String>>>() {
                    @Override
                    public void onFailure(Throwable cause) {
                        FutureUtils.setException(readResult, cause);
                    }

                    @Override
                    public void onSuccess(Versioned<List<String>> logSegmentNames) {
                        readLogSegmentsFromStore(logSegmentNames, comparator, segmentFilter, readResult);
                    }
                });
        return readResult;
    }

    protected void readLogSegmentsFromStore(final Versioned<List<String>> logSegmentNames,
                                            final Comparator<LogSegmentMetadata> comparator,
                                            final LogSegmentFilter segmentFilter,
                                            final Promise<Versioned<List<LogSegmentMetadata>>> readResult) {
        Set<String> segmentsReceived = new HashSet<String>();
        segmentsReceived.addAll(segmentFilter.filter(logSegmentNames.getValue()));
        Set<String> segmentsAdded;
        final Set<String> removedSegments = Collections.synchronizedSet(new HashSet<String>());
        final Map<String, LogSegmentMetadata> addedSegments =
                Collections.synchronizedMap(new HashMap<String, LogSegmentMetadata>());
        Pair<Set<String>, Set<String>> segmentChanges = logSegmentCache.diff(segmentsReceived);
        segmentsAdded = segmentChanges.getLeft();
        removedSegments.addAll(segmentChanges.getRight());

        if (segmentsAdded.isEmpty()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("No segments added for {}.", getFullyQualifiedName());
            }

            // update the cache before #getCachedLogSegments to return
            updateLogSegmentCache(removedSegments, addedSegments);

            List<LogSegmentMetadata> segmentList;
            try {
                segmentList = getCachedLogSegments(comparator);
            } catch (UnexpectedException e) {
                FutureUtils.setException(readResult, e);
                return;
            }

            FutureUtils.setValue(readResult,
                    new Versioned<List<LogSegmentMetadata>>(segmentList, logSegmentNames.getVersion()));
            return;
        }

        final AtomicInteger numChildren = new AtomicInteger(segmentsAdded.size());
        final AtomicInteger numFailures = new AtomicInteger(0);
        for (final String segment: segmentsAdded) {
            String logSegmentPath = logMetadata.getLogSegmentPath(segment);
            LogSegmentMetadata cachedSegment = metadataCache.get(logSegmentPath);
            if (null != cachedSegment) {
                addedSegments.put(segment, cachedSegment);
                completeReadLogSegmentsFromStore(
                        removedSegments,
                        addedSegments,
                        comparator,
                        readResult,
                        logSegmentNames.getVersion(),
                        numChildren,
                        numFailures);
                continue;
            }
            metadataStore.getLogSegment(logSegmentPath)
                    .addEventListener(new FutureEventListener<LogSegmentMetadata>() {

                        @Override
                        public void onSuccess(LogSegmentMetadata result) {
                            addedSegments.put(segment, result);
                            complete();
                        }

                        @Override
                        public void onFailure(Throwable cause) {
                            // LogSegmentNotFoundException exception is possible in two cases
                            // 1. A log segment was deleted by truncation between the call to getChildren and read
                            // attempt on the znode corresponding to the segment
                            // 2. In progress segment has been completed => inprogress ZNode does not exist
                            if (cause instanceof LogSegmentNotFoundException) {
                                removedSegments.add(segment);
                                complete();
                            } else {
                                // fail fast
                                if (1 == numFailures.incrementAndGet()) {
                                    FutureUtils.setException(readResult, cause);
                                    return;
                                }
                            }
                        }

                        private void complete() {
                            completeReadLogSegmentsFromStore(
                                    removedSegments,
                                    addedSegments,
                                    comparator,
                                    readResult,
                                    logSegmentNames.getVersion(),
                                    numChildren,
                                    numFailures);
                        }
                    });
        }
    }

    private void completeReadLogSegmentsFromStore(final Set<String> removedSegments,
                                                  final Map<String, LogSegmentMetadata> addedSegments,
                                                  final Comparator<LogSegmentMetadata> comparator,
                                                  final Promise<Versioned<List<LogSegmentMetadata>>> readResult,
                                                  final Version logSegmentNamesVersion,
                                                  final AtomicInteger numChildren,
                                                  final AtomicInteger numFailures) {
        if (0 != numChildren.decrementAndGet()) {
            return;
        }
        if (numFailures.get() > 0) {
            return;
        }
        // update the cache only when fetch completed and before #getCachedLogSegments
        updateLogSegmentCache(removedSegments, addedSegments);
        List<LogSegmentMetadata> segmentList;
        try {
            segmentList = getCachedLogSegments(comparator);
        } catch (UnexpectedException e) {
            FutureUtils.setException(readResult, e);
            return;
        }
        FutureUtils.setValue(readResult,
            new Versioned<List<LogSegmentMetadata>>(segmentList, logSegmentNamesVersion));
    }

}
