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
package org.apache.distributedlog.protocol.util;

import static com.google.common.base.Charsets.UTF_8;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.apache.distributedlog.DLSN;
import java.util.zip.CRC32;
import org.apache.distributedlog.exceptions.DLException;
import org.apache.distributedlog.exceptions.OwnershipAcquireFailedException;
import org.apache.distributedlog.thrift.service.ResponseHeader;

/**
 * With CRC embedded in the application, we have to keep track of per api crc. Ideally this
 * would be done by thrift.
 */
public class ProtocolUtils {

    // For request payload checksum
    private static final ThreadLocal<CRC32> requestCRC = new ThreadLocal<CRC32>() {
        @Override
        protected CRC32 initialValue() {
            return new CRC32();
        }
    };

    /**
     * Generate crc32 for WriteOp.
     */
    public static Long writeOpCRC32(String stream, ByteBuf data) {
        CRC32 crc = requestCRC.get();
        try {
            crc.update(stream.getBytes(UTF_8));
            crc.update(data.nioBuffer());
            return crc.getValue();
        } finally {
            crc.reset();
        }
    }

    /**
     * Generate crc32 for WriteOp.
     */
    public static Long writeOpCRC32(String stream, ByteBuffer data) {
        CRC32 crc = requestCRC.get();
        try {
            crc.update(stream.getBytes(UTF_8));
            crc.update(data);
            return crc.getValue();
        } finally {
            crc.reset();
        }
    }

    /**
     * Generate crc32 for TruncateOp.
     */
    public static Long truncateOpCRC32(String stream, DLSN dlsn) {
        CRC32 crc = requestCRC.get();
        try {
            crc.update(stream.getBytes(UTF_8));
            crc.update(dlsn.serializeBytes());
            return crc.getValue();
        } finally {
            crc.reset();
        }
    }

    /**
     * Generate crc32 for any op which only passes a stream name.
     */
    public static Long streamOpCRC32(String stream) {
        CRC32 crc = requestCRC.get();
        try {
            crc.update(stream.getBytes(UTF_8));
            return crc.getValue();
        } finally {
            crc.reset();
        }
    }

    public static DLException exception(ResponseHeader response) {
        String errMsg;
        switch (response.getCode()) {
            case FOUND:
                if (response.isSetErrMsg()) {
                    errMsg = response.getErrMsg();
                } else {
                    errMsg = "Request is redirected to " + response.getLocation();
                }
                return new OwnershipAcquireFailedException(errMsg, response.getLocation());
            case SUCCESS:
                throw new IllegalArgumentException("Can't instantiate an exception for success response.");
            default:
                if (response.isSetErrMsg()) {
                    errMsg = response.getErrMsg();
                } else {
                    errMsg = response.getCode().name();
                }
                return new DLException(response.getCode().getValue(), errMsg);
        }
    }
}
