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
package org.apache.distributedlog.io;

import com.github.luben.zstd.Zstd;
import com.google.common.base.Stopwatch;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * ZStandard Compression Codec.
 *
 * {@link http://facebook.github.io/zstd/}
 */
public class ZstdCompressionCodec implements CompressionCodec {

    public ZstdCompressionCodec() {}

    byte[] getOrCopyData(byte[] data, int offset, int length) {
        byte[] copyOfData;
        if (0 == offset && length == data.length) {
            copyOfData = data;
        } else {
            copyOfData = Arrays.copyOfRange(data, offset, offset + length);
        }
        return copyOfData;
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length, OpStatsLogger compressionStat) {
        Stopwatch watch = Stopwatch.createStarted();
        // TODO: make the compression level configurable.
        byte[] compressed = Zstd.compress(getOrCopyData(data, offset, length), 3);

        compressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
        return compressed;
    }

    @Override
    public byte[] decompress(byte[] data, int offset, int length, OpStatsLogger decompressionStat) {
        Stopwatch watch = Stopwatch.createStarted();
        byte[] compressedData = getOrCopyData(data, offset, length);
        int decompressedSize = (int) Zstd.decompressedSize(compressedData);
        byte[] decompressedData = new byte[decompressedSize];
        // TODO: make the compression level configurable.
        int actualDecompressedSize = (int) Zstd.decompress(decompressedData, compressedData);
        if (actualDecompressedSize != decompressedSize) {
            decompressedData = getOrCopyData(decompressedData, 0, actualDecompressedSize);
        }
        decompressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
        return decompressedData;
    }

    @Override
    public byte[] decompress(byte[] data, int offset, int length, int decompressedSize,
                             OpStatsLogger decompressionStat) {
        Stopwatch watch = Stopwatch.createStarted();
        // TODO: make the compression level configurable.
        byte[] decompressed = Zstd.decompress(getOrCopyData(data, offset, length), decompressedSize);
        decompressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
        return decompressed;
    }
}
