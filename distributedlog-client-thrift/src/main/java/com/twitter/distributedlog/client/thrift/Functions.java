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
package com.twitter.distributedlog.client.thrift;

import com.twitter.distributedlog.service.protocol.ServerInfo;
import com.twitter.distributedlog.service.protocol.WriteResponse;
import scala.Function1;
import scala.runtime.AbstractFunction1;

import static com.twitter.distributedlog.client.thrift.ThriftUtils.*;

/**
 * Functions to convert protocol objects
 */
public class Functions {

    public static final Function1<com.twitter.distributedlog.thrift.service.ServerInfo, ServerInfo> TO_SERVER_INFO_FUNC =
            new AbstractFunction1<com.twitter.distributedlog.thrift.service.ServerInfo, ServerInfo>() {
                @Override
                public ServerInfo apply(com.twitter.distributedlog.thrift.service.ServerInfo serverInfo) {
                    return fromThriftServerInfo(serverInfo);
                }
            };

    public static final Function1<com.twitter.distributedlog.thrift.service.WriteResponse, WriteResponse> TO_WRITE_RESPONSE_FUNC =
            new AbstractFunction1<com.twitter.distributedlog.thrift.service.WriteResponse, WriteResponse>() {
                @Override
                public WriteResponse apply(com.twitter.distributedlog.thrift.service.WriteResponse writeResponse) {
                    return fromThriftWriteResponse(writeResponse);
                }
            };

}
