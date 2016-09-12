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

/**
 * Client Info
 */
public class ClientInfo {

    private final Optional<String> streamNameRegex;
    private final Optional<Boolean> getOwnerships;

    public ClientInfo(Optional<String> streamNameRegex,
                      Optional<Boolean> getOwnerships) {
        this.streamNameRegex = streamNameRegex;
        this.getOwnerships = getOwnerships;
    }

    public Optional<String> getStreamNameRegex() {
        return this.streamNameRegex;
    }

    public Optional<Boolean> shouldGetOwnerships() {
        return this.getOwnerships;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(streamNameRegex);
        builder.append(getOwnerships);
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ClientInfo)) {
            return false;
        }
        ClientInfo another = (ClientInfo) obj;
        return Objects.equal(streamNameRegex, another.streamNameRegex) &&
                Objects.equal(getOwnerships, another.getOwnerships);
    }
}
