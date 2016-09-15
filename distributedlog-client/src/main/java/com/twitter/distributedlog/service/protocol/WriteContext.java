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
import com.google.common.collect.Sets;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Set;

/**
 * Context for a rpc request to write proxy
 */
public class WriteContext {

    private Set<String> triedHosts;
    private Long crc32;
    private Optional<Boolean> isRecordSet;

    public WriteContext() {
        this.triedHosts = Sets.newHashSet();
        this.crc32 = null;
        this.isRecordSet = Optional.absent();
    }

    public WriteContext(Set<String> triedHosts,
                        Long crc32,
                        Optional<Boolean> isRecordSet) {
        this.triedHosts = triedHosts;
        this.crc32 = crc32;
        this.isRecordSet = isRecordSet;
    }

    public Set<String> getTriedHosts() {
        return this.triedHosts;
    }

    public void addTriedHost(String host) {
        this.triedHosts.add(host);
    }

    public boolean isHostTried(String host) {
        return this.triedHosts.contains(host);
    }

    public Long getCrc32() {
        return this.crc32;
    }

    public void setCrc32(Long crc32) {
        this.crc32 = crc32;
    }

    public Optional<Boolean> isRecordSet() {
        return this.isRecordSet;
    }

    public void setRecordSet(boolean isRecordSet) {
        this.isRecordSet = Optional.fromNullable(isRecordSet);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WriteContext)) {
            return false;
        }
        WriteContext another = (WriteContext) obj;
        return Objects.equal(triedHosts, another.triedHosts) &&
                Objects.equal(crc32, another.crc32) &&
                Objects.equal(isRecordSet, another.isRecordSet);
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(triedHosts);
        builder.append(crc32);
        builder.append(isRecordSet);
        return builder.toHashCode();
    }
}
