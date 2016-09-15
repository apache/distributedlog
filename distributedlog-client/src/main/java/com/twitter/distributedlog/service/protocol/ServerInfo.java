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
package com.twitter.distributedlog.service.protocol;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Map;

/**
 * Server Info
 */
public class ServerInfo {

    private final Optional<Map<String, String>> ownerships;
    private final Optional<Integer> serverStatus;

    public ServerInfo(Optional<Map<String, String>> ownerships,
                      Optional<Integer> serverStatus) {
        this.ownerships = ownerships;
        this.serverStatus = serverStatus;
    }

    public Optional<Map<String, String>> getOwnerships() {
        return this.ownerships;
    }

    public int getOwnershipsSize() {
        if (ownerships.isPresent()) {
            return ownerships.get().size();
        }
        return 0;
    }

    public Optional<Integer> getServerStatus() {
        return this.serverStatus;
    }

    public boolean isServerDown() {
        return this.serverStatus.isPresent() && ServerStatus.DOWN == serverStatus.get();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(ownerships);
        builder.append(serverStatus);
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ServerInfo)) {
            return false;
        }

        ServerInfo another = (ServerInfo) obj;
        return Objects.equal(ownerships, another.ownerships) &&
                Objects.equal(serverStatus, another.serverStatus);
    }
}
