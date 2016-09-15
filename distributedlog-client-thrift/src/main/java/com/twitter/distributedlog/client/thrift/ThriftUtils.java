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

import com.google.common.base.Optional;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.service.protocol.ClientInfo;
import com.twitter.distributedlog.service.protocol.HeartbeatOptions;
import com.twitter.distributedlog.service.protocol.ResponseHeader;
import com.twitter.distributedlog.service.protocol.ServerInfo;
import com.twitter.distributedlog.service.protocol.WriteContext;
import com.twitter.distributedlog.service.protocol.WriteResponse;

/**
 * Utils for thrift protocol
 */
public class ThriftUtils {

    public static com.twitter.distributedlog.thrift.service.ClientInfo toThriftClientInfo(ClientInfo clientInfo) {
        com.twitter.distributedlog.thrift.service.ClientInfo thriftClientInfo =
                new com.twitter.distributedlog.thrift.service.ClientInfo();
        if (clientInfo.getStreamNameRegex().isPresent()) {
            thriftClientInfo.setStreamNameRegex(clientInfo.getStreamNameRegex().get());
        }
        if (clientInfo.shouldGetOwnerships().isPresent()) {
            thriftClientInfo.setGetOwnerships(clientInfo.shouldGetOwnerships().get());
        }
        return thriftClientInfo;
    }

    public static com.twitter.distributedlog.thrift.service.WriteContext toThriftWriteContext(WriteContext context) {
        com.twitter.distributedlog.thrift.service.WriteContext thriftWriteContext =
                new com.twitter.distributedlog.thrift.service.WriteContext();
        if (context.isRecordSet().isPresent()) {
            thriftWriteContext.setIsRecordSet(context.isRecordSet().get());
        }
        if (!context.getTriedHosts().isEmpty()) {
            thriftWriteContext.setTriedHosts(context.getTriedHosts());
        }
        if (null != context.getCrc32()) {
            thriftWriteContext.setCrc32(context.getCrc32());
        }
        return thriftWriteContext;
    }

    public static com.twitter.distributedlog.thrift.service.HeartbeatOptions toThriftHeartbeatOptions(HeartbeatOptions options) {
        com.twitter.distributedlog.thrift.service.HeartbeatOptions thriftHeartbeatOptions =
                new com.twitter.distributedlog.thrift.service.HeartbeatOptions();
        if (options.shouldSendHeartBeatToReader().isPresent()) {
            thriftHeartbeatOptions.setSendHeartBeatToReader(options.shouldSendHeartBeatToReader().get());
        }
        return thriftHeartbeatOptions;
    }

    public static ResponseHeader fromThriftResponseHeader(com.twitter.distributedlog.thrift.service.ResponseHeader thriftResponseHeader) {
        return new ResponseHeader(
                thriftResponseHeader.getCode().getValue(),
                Optional.fromNullable(thriftResponseHeader.getErrMsg()),
                Optional.fromNullable(thriftResponseHeader.getLocation()));
    }

    public static ServerInfo fromThriftServerInfo(com.twitter.distributedlog.thrift.service.ServerInfo thriftServerInfo) {
        Integer serverStatusCode = null;
        if (thriftServerInfo.isSetServerStatus()) {
            serverStatusCode = thriftServerInfo.getServerStatus().getValue();
        }
        return new ServerInfo(
                Optional.fromNullable(thriftServerInfo.getOwnerships()),
                Optional.fromNullable(serverStatusCode));
    }

    public static WriteResponse fromThriftWriteResponse(com.twitter.distributedlog.thrift.service.WriteResponse thriftWriteResponse) {
        DLSN dlsn = null;
        if (thriftWriteResponse.isSetDlsn()) {
            dlsn = DLSN.deserialize(thriftWriteResponse.getDlsn());
        }
        return new WriteResponse(
                fromThriftResponseHeader(thriftWriteResponse.getHeader()),
                Optional.fromNullable(dlsn));
    }

}
