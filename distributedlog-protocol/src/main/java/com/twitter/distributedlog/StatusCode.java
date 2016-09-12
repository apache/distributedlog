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

/**
 * General Status Code
 */
public class StatusCode {

    // 2xx: action requested by the client was received, understood, accepted and processed successfully.

    // standard response for successful requests.
    public static final int SUCCESS = 200;

    // 3xx: client must take additional action to complete the request.
    // client closed.
    public static final int CLIENT_CLOSED = 301;
    // found the stream in a different server, a redirection is required by client.
    public static final int FOUND = 302;

    // 4xx: client seems to have erred.

    // request is denied for some reason
    public static final int REQUEST_DENIED = 403;
    // request record too large
    public static final int TOO_LARGE_RECORD = 413;

    // 5xx: server failed to fulfill an apparently valid request.

    // Generic error message, given when no more specific message is suitable.
    public static final int INTERNAL_SERVER_ERROR = 500;
    // Not implemented
    public static final int NOT_IMPLEMENTED = 501;
    // Already Closed Exception
    public static final int ALREADY_CLOSED = 502;
    // Service is currently unavailable (because it is overloaded or down for maintenance).
    public static final int SERVICE_UNAVAILABLE = 503;
    // Locking exception
    public static final int LOCKING_EXCEPTION = 504;
    // ZooKeeper Errors
    public static final int ZOOKEEPER_ERROR = 505;
    // Metadata exception
    public static final int METADATA_EXCEPTION = 506;
    // BK Transmit Error
    public static final int BK_TRANSMIT_ERROR = 507;
    // Flush timeout
    public static final int FLUSH_TIMEOUT = 508;
    // Log empty
    public static final int LOG_EMPTY = 509;
    // Log not found
    public static final int LOG_NOT_FOUND = 510;
    // Truncated Transactions
    public static final int TRUNCATED_TRANSACTION = 511;
    // End of Stream
    public static final int END_OF_STREAM = 512;
    // Transaction Id Out of Order
    public static final int TRANSACTION_OUT_OF_ORDER = 513;
    // Write exception
    public static final int WRITE_EXCEPTION = 514;
    // Stream Unavailable
    public static final int STREAM_UNAVAILABLE = 515;
    // Write cancelled exception
    public static final int WRITE_CANCELLED_EXCEPTION = 516;
    // over-capacity/backpressure
    public static final int OVER_CAPACITY = 517;
    // stream exists but is not ready (recovering etc.).
    // the difference between NOT_READY and UNAVAILABLE is that UNAVAILABLE
    // indicates the stream is no longer owned by the proxy and we should
    // redirect. NOT_READY indicates the stream exist at the proxy but isn't
    // ready for writes.
    public static final int STREAM_NOT_READY = 518;
    // Region Unavailable
    public static final int REGION_UNAVAILABLE = 519;
    // Invalid Enveloped Entry
    public static final int INVALID_ENVELOPED_ENTRY = 520;
    // Unsupported metadata version
    public static final int UNSUPPORTED_METADATA_VERSION = 521;
    // Log Already Exists
    public static final int LOG_EXISTS = 522;
    // Checksum failed on the request
    public static final int CHECKSUM_FAILED = 523;
    // Overcapacity: too many streams
    public static final int TOO_MANY_STREAMS = 524;

    // 6xx: unexpected
    public static final int UNEXPECTED = 600;
    public static final int INTERRUPTED = 601;
    public static final int INVALID_STREAM_NAME = 602;
    public static final int ILLEGAL_STATE = 603;

    // 10xx: reader exceptions
    public static final int RETRYABLE_READ = 1000;
    public static final int LOG_READ_ERROR = 1001;
    // Read cancelled exception
    public static final int READ_CANCELLED_EXCEPTION = 1002;

    public static String getStatusName(int code) {
        switch (code) {
            case SUCCESS:
                return "SUCCESS";
            case CLIENT_CLOSED:
                return "CLIENT_CLOSED";
            case FOUND:
                return "FOUND";
            case REQUEST_DENIED:
                return "REQUEST_DENIED";
            case TOO_LARGE_RECORD:
                return "TOO_LARGE_RECORD";
            case INTERNAL_SERVER_ERROR:
                return "INTERNAL_SERVER_ERROR";
            case NOT_IMPLEMENTED:
                return "NOT_IMPLEMENTED";
            case ALREADY_CLOSED:
                return "ALREADY_CLOSED";
            case SERVICE_UNAVAILABLE:
                return "SERVICE_UNAVAILABLE";
            case LOCKING_EXCEPTION:
                return "LOCKING_EXCEPTION";
            case ZOOKEEPER_ERROR:
                return "ZOOKEEPER_ERROR";
            case METADATA_EXCEPTION:
                return "METADATA_EXCEPTION";
            case BK_TRANSMIT_ERROR:
                return "BK_TRANSMIT_ERROR";
            case FLUSH_TIMEOUT:
                return "FLUSH_TIMEOUT";
            case LOG_EMPTY:
                return "LOG_EMPTY";
            case LOG_NOT_FOUND:
                return "LOG_NOT_FOUND";
            case TRUNCATED_TRANSACTION:
                return "TRUNCATED_TRANSACTION";
            case END_OF_STREAM:
                return "END_OF_STREAM";
            case TRANSACTION_OUT_OF_ORDER:
                return "TRANSACTION_OUT_OF_ORDER";
            case WRITE_EXCEPTION:
                return "WRITE_EXCEPTION";
            case STREAM_UNAVAILABLE:
                return "STREAM_UNAVAILABLE";
            case WRITE_CANCELLED_EXCEPTION:
                return "WRITE_CANCELLED_EXCEPTION";
            case OVER_CAPACITY:
                return "OVER_CAPACITY";
            case STREAM_NOT_READY:
                return "STREAM_NOT_READY";
            case REGION_UNAVAILABLE:
                return "REGION_UNAVAILABLE";
            case INVALID_ENVELOPED_ENTRY:
                return "INVALID_ENVELOPED_ENTRY";
            case UNSUPPORTED_METADATA_VERSION:
                return "UNSUPPORTED_METADATA_VERSION";
            case LOG_EXISTS:
                return "LOG_EXISTS";
            case CHECKSUM_FAILED:
                return "CHECKSUM_FAILED";
            case TOO_MANY_STREAMS:
                return "TOO_MANY_STREAMS";
            case UNEXPECTED:
                return "UNEXPECTED";
            case INTERRUPTED:
                return "INTERRUPTED";
            case INVALID_STREAM_NAME:
                return "INVALID_STREAM_NAME";
            case ILLEGAL_STATE:
                return "ILLEGAL_STATE";
            case RETRYABLE_READ:
                return "RETRYABLE_READ";
            case LOG_READ_ERROR:
                return "LOG_READ_ERROR";
            case READ_CANCELLED_EXCEPTION:
                return "READ_CANCELLED_EXCEPTION";
            default:
                return "UNKNOWN";
        }
    }

}
