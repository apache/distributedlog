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
package com.twitter.distributedlog.client.proxy;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.service.protocol.ClientInfo;
import com.twitter.distributedlog.service.protocol.HeartbeatOptions;
import com.twitter.distributedlog.service.protocol.ServerInfo;
import com.twitter.distributedlog.service.protocol.WriteContext;
import com.twitter.distributedlog.service.protocol.WriteResponse;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * Client talks to a single proxy.
 */
public interface ProxyClient {

    interface Builder {
        /**
         * Build a proxy client to <code>address</code>.
         *
         * @param address
         *          proxy address
         * @return proxy client
         */
        ProxyClient build(SocketAddress address);
    }

    /**
     * Get the socket address of this proxy client connects to.
     *
     * @return socket address
     */
    SocketAddress getAddress();

    /**
     * Handshake with the write proxy.
     *
     * @param clientInfo
     *          client info to pass to server during handshaking
     * @return server info
     */
    Future<ServerInfo> handshake(ClientInfo clientInfo);

    /**
     * Send heartbeats to a given stream.
     *
     * @param stream stream to heartbeat
     * @param context write context for the request
     * @param heartbeatOptions heartbeat options
     * @return write response
     */
    Future<WriteResponse> heartbeat(String stream, WriteContext context, HeartbeatOptions heartbeatOptions);

    /**
     * Write <code>data</code> to a given <i>stream</i>.
     *
     * @param stream stream to write
     * @param data data to write
     * @param context context of the request
     * @return write response
     */
    Future<WriteResponse> write(String stream, ByteBuffer data, WriteContext context);

    /**
     * Truncate the <code>stream</code>.
     *
     * @param stream stream to truncate
     * @param dlsn dlsn to truncate
     * @param context context of the request
     * @return write response
     */
    Future<WriteResponse> truncate(String stream, DLSN dlsn, WriteContext context);

    /**
     * Release the ownership of the <code>stream</code>.
     *
     * @param stream stream to release ownership
     * @param context context of the request
     * @return write response
     */
    Future<WriteResponse> release(String stream, WriteContext context);

    /**
     * Create the <i>stream</i>.
     *
     * @param stream stream to create
     * @param context context of the request
     * @return write response
     */
    Future<WriteResponse> create(String stream, WriteContext context);

    /**
     * Delete the <i>stream</i>.
     *
     * @param stream stream to delete
     * @param context context of the request
     * @return write response
     */
    Future<WriteResponse> delete(String stream, WriteContext context);

    /**
     * Set a proxy server to enable/disable accepting new streams.
     *
     * @param enabled flag to enable/disable accepting new streams.
     * @return void
     */
    Future<Void> setAcceptNewStream(boolean enabled);

    /**
     * Close the proxy client.
     *
     * @return
     */
    Future<BoxedUnit> close();
}
