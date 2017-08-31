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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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

    ByteBuf entry;
    ByteBuf compressedLz4;
    ByteBuf compressedNone;
    CompressionCodec codecLz4;
    CompressionCodec codecNone;
    OpStatsLogger opStatsLogger;

    @Setup
    public void prepare() {
        Random r = new Random(System.currentTimeMillis());
        byte[] data = new byte[this.size];
        r.nextBytes(data);
        this.entry = Unpooled.buffer(size);
        this.entry.writeBytes(data);

        this.codecLz4 = CompressionUtils.getCompressionCodec(Type.LZ4);
        this.codecNone = CompressionUtils.getCompressionCodec(Type.NONE);
        this.opStatsLogger = NullStatsLogger.INSTANCE.getOpStatsLogger("");
        this.compressedLz4 = codecLz4.compress(entry.slice(), 0);
        this.compressedNone = codecNone.compress(entry.slice(), 0);
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
        ByteBuf compressed = codec.compress(entry.slice(), 0);
        compressed.release();
    }

    @Benchmark
    public void testDecompressLZ4() throws Exception {
        testDecompress(codecLz4, compressedLz4);
    }

    @Benchmark
    public void testDecompressNone() throws Exception {
        testDecompress(codecNone, compressedNone);
    }

    private void testDecompress(CompressionCodec codec, ByteBuf compressed) {
        ByteBuf decompressed = codec.decompress(compressed.slice(), size);
        decompressed.release();
    }

}
