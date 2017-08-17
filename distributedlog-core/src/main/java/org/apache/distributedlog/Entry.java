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
package org.apache.distributedlog;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.exceptions.WriteException;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A set of {@link LogRecord}s.
 */
public class Entry {

    /**
     * Create a new log record set.
     *
     * @param logName
     *          name of the log
     * @param initialBufferSize
     *          initial buffer size
     * @param envelopeBeforeTransmit
     *          if envelope the buffer before transmit
     * @param codec
     *          compression codec
     * @param statsLogger
     *          stats logger to receive stats
     * @return writer to build a log record set.
     */
    public static Writer newEntry(
            String logName,
            int initialBufferSize,
            boolean envelopeBeforeTransmit,
            CompressionCodec.Type codec,
            StatsLogger statsLogger) {
        return new EnvelopedEntryWriter(
                logName,
                initialBufferSize,
                envelopeBeforeTransmit,
                codec,
                statsLogger);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Build the record set object.
     */
    public static class Builder {

        private long logSegmentSequenceNumber = -1;
        private long entryId = -1;
        private long startSequenceId = Long.MIN_VALUE;
        private boolean envelopeEntry = true;
        private ByteBuf buffer;
        private Optional<Long> txidToSkipTo = Optional.absent();
        private Optional<DLSN> dlsnToSkipTo = Optional.absent();
        private boolean deserializeRecordSet = true;

        private Builder() {}

        /**
         * Set the segment info of the log segment that this record
         * set belongs to.
         *
         * @param lssn
         *          log segment sequence number
         * @param startSequenceId
         *          start sequence id of this log segment
         * @return builder
         */
        public Builder setLogSegmentInfo(long lssn, long startSequenceId) {
            this.logSegmentSequenceNumber = lssn;
            this.startSequenceId = startSequenceId;
            return this;
        }

        /**
         * Set the entry id of this log record set.
         *
         * @param entryId
         *          entry id assigned for this log record set.
         * @return builder
         */
        public Builder setEntryId(long entryId) {
            this.entryId = entryId;
            return this;
        }

        /**
         * Set whether this record set is enveloped or not.
         *
         * @param enabled
         *          flag indicates whether this record set is enveloped or not.
         * @return builder
         */
        public Builder setEnvelopeEntry(boolean enabled) {
            this.envelopeEntry = enabled;
            return this;
        }

        /**
         * Set the entry buffer of the serialized bytes data of this record set.
         *
         * @param buffer
         *          input stream
         * @return builder
         */
        public Builder setEntry(ByteBuf buffer) {
            this.buffer = buffer.retainedSlice();
            return this;
        }

        /**
         * Enable/disable deserialize record set.
         *
         * @param enabled
         *          flag to enable/disable dserialize record set.
         * @return builder
         */
        public Builder deserializeRecordSet(boolean enabled) {
            this.deserializeRecordSet = enabled;
            return this;
        }

        public Entry.Reader buildReader() throws IOException {
            Preconditions.checkNotNull(buffer,
                    "Serialized data or input stream isn't provided");
            return new EnvelopedEntryReader(
                    logSegmentSequenceNumber,
                    entryId,
                    startSequenceId,
                    buffer,
                    envelopeEntry,
                    deserializeRecordSet,
                    NullStatsLogger.INSTANCE);
        }

    }

    /**
     * Writer to append {@link LogRecord}s to {@link Entry}.
     */
    public interface Writer extends EntryBuffer {

        /**
         * Write a {@link LogRecord} to this record set.
         *
         * @param record
         *          record to write
         * @param transmitPromise
         *          callback for transmit result. the promise is only
         *          satisfied when this record set is transmitted.
         * @throws LogRecordTooLongException if the record is too long
         * @throws WriteException when encountered exception writing the record
         */
        void writeRecord(LogRecord record, CompletableFuture<DLSN> transmitPromise)
                throws LogRecordTooLongException, WriteException;

    }

    /**
     * Reader to read {@link LogRecord}s from this record set.
     */
    public interface Reader {

        /**
         * Get the log segment sequence number.
         *
         * @return the log segment sequence number.
         */
        long getLSSN();

        /**
         * Return the entry id.
         *
         * @return the entry id.
         */
        long getEntryId();

        /**
         * Read next log record from this record set.
         *
         * @return next log record from this record set.
         */
        LogRecordWithDLSN nextRecord() throws IOException;

        /**
         * Skip the reader to the record whose transaction id is <code>txId</code>.
         *
         * @param txId
         *          transaction id to skip to.
         * @return true if skip succeeds, otherwise false.
         * @throws IOException
         */
        boolean skipTo(long txId) throws IOException;

        /**
         * Skip the reader to the record whose DLSN is <code>dlsn</code>.
         *
         * @param dlsn
         *          DLSN to skip to.
         * @return true if skip succeeds, otherwise false.
         * @throws IOException
         */
        boolean skipTo(DLSN dlsn) throws IOException;

    }

}
