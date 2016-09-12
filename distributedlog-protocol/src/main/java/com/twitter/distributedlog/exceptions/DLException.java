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
package com.twitter.distributedlog.exceptions;

import com.google.common.base.Optional;
import com.twitter.distributedlog.StatusCode;

import java.io.IOException;

import static com.twitter.distributedlog.StatusCode.*;

public class DLException extends IOException {
    private static final long serialVersionUID = -4485775468586114393L;
    protected final int code;

    protected DLException(int code) {
        super();
        this.code = code;
    }

    protected DLException(int code, String msg) {
        super(msg);
        this.code = code;
    }

    protected DLException(int code, Throwable t) {
        super(t);
        this.code = code;
    }

    protected DLException(int code, String msg, Throwable t) {
        super(msg, t);
        this.code = code;
    }

    /**
     * Return the status code representing the exception.
     *
     * @return status code representing the exception.
     */
    public int getCode() {
        return code;
    }

    public static DLException of(int code,
                                 Optional<String> errMsgOptional,
                                 Optional<String> locationOptional) {
        String errMsg;
        switch (code) {
            case FOUND:
                if (errMsgOptional.isPresent()) {
                    errMsg = errMsgOptional.get();
                } else {
                    errMsg = "Request is redirected to " + locationOptional;
                }
                return new OwnershipAcquireFailedException(errMsg, locationOptional.get());
            case SUCCESS:
                throw new IllegalArgumentException("Can't instantiate an exception for success response.");
            default:
                if (errMsgOptional.isPresent()) {
                    errMsg = errMsgOptional.get();
                } else {
                    errMsg = StatusCode.getStatusName(code);
                }
                return new DLException(code, errMsg);
        }
    }

}
