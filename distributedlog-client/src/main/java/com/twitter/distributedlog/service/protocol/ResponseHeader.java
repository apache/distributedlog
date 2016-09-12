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
 * Header of the response
 */
public class ResponseHeader {

    private final int code;
    private final Optional<String> errMsg;
    private final Optional<String> location;

    public ResponseHeader(int code,
                          Optional<String> errMsg,
                          Optional<String> location) {
        this.code = code;
        this.errMsg = errMsg;
        this.location = location;
    }

    public int getCode() {
        return code;
    }

    public Optional<String> getErrMsg() {
        return errMsg;
    }

    public Optional<String> getLocation() {
        return location;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(code);
        builder.append(errMsg);
        builder.append(location);
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ResponseHeader)) {
            return false;
        }
        ResponseHeader another = (ResponseHeader) obj;
        return code == another.code &&
                Objects.equal(errMsg, another.errMsg) &&
                Objects.equal(location, another.location);
    }
}
