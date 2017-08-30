/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.distributedlog.tests;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.io.CompressionCodec.Type;
import org.apache.distributedlog.io.CompressionUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmarking serialization and deserilization.
 */
@BenchmarkMode({ Mode.Throughput })
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class CompressionBenchmark {

    @Param({ "10", "100", "1000", "10000" })
    int size;

    byte[] entry;
    byte[] compressedLz4;
    byte[] compressedNone;
    CompressionCodec codecLz4;
    CompressionCodec codecNone;
    OpStatsLogger opStatsLogger;

    @Setup
    public void prepare() {
        Random r = new Random(System.currentTimeMillis());
        this.entry = new byte[this.size];
        r.nextBytes(entry);

        this.codecLz4 = CompressionUtils.getCompressionCodec(Type.LZ4);
        this.codecNone = CompressionUtils.getCompressionCodec(Type.NONE);
        this.opStatsLogger = NullStatsLogger.INSTANCE.getOpStatsLogger("");
        this.compressedLz4 = codecLz4.compress(entry, 0, entry.length, opStatsLogger);
        this.compressedNone = codecNone.compress(entry, 0, entry.length, opStatsLogger);
    }


    @Benchmark
    public void testCompressLZ4() throws Exception {
        testCompress(codecLz4);
    }

    @Benchmark
    public void testCompressNone() throws Exception {
        testCompress(codecNone);
    }

    private void testCompress(CompressionCodec codec) {
        codec.compress(entry, 0, entry.length, opStatsLogger);
    }

    @Benchmark
    public void testDecompressLZ4() throws Exception {
        testDecompress(codecLz4, compressedLz4);
    }

    @Benchmark
    public void testDecompressNone() throws Exception {
        testDecompress(codecNone, compressedNone);
    }

    private void testDecompress(CompressionCodec codec, byte[] compressed) {
        codec.decompress(compressed, 0, compressed.length, opStatsLogger);
    }

}
