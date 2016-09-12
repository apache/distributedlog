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
import com.twitter.distributedlog.DLSN;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Write response
 */
public class WriteResponse {

    private final ResponseHeader header;
    private final Optional<DLSN> dlsn;

    public WriteResponse(ResponseHeader header,
                         Optional<DLSN> dlsn) {
        this.header = header;
        this.dlsn = dlsn;
    }

    public ResponseHeader getHeader() {
        return this.header;
    }

    public Optional<DLSN> getDlsn() {
        return this.dlsn;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WriteResponse)) {
            return false;
        }
        WriteResponse another = (WriteResponse) obj;
        return Objects.equal(header, another.header) &&
                Objects.equal(dlsn, another.dlsn);
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(header);
        builder.append(dlsn);
        return builder.toHashCode();
    }
}
