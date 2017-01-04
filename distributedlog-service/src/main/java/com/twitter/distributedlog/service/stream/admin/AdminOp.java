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
package com.twitter.distributedlog.service.stream.admin;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.util.Future;

/**
 * Admin operation interface.
 */
public interface AdminOp<RespT> {

    /**
     * Invoked before the stream op is executed.
     */
    void preExecute() throws DLException;

    /**
     * Execute the operation.
     *
     * @return the future represents the response of the operation
     */
    Future<RespT> execute();

}
