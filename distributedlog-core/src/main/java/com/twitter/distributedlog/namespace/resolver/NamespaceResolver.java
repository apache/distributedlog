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
package com.twitter.distributedlog.namespace.resolver;

import com.twitter.distributedlog.exceptions.InvalidStreamNameException;

/**
 * DistributedLog uses {@link NamespaceResolver} to identify the locations of streams
 * under a given namespace. The resolver will resolve the stream names into
 * hierarchical paths to locate the metadata of streams.
 */
public interface NamespaceResolver {

    /**
     * Validate the stream name.
     *
     * @param streamName name of the stream
     * @throws InvalidStreamNameException if the stream name is invalid.
     */
    void validateStreamName(String streamName) throws InvalidStreamNameException;

    /**
     * Resolve the <i>streamName</i> into the the stream path. The stream path is used
     * for locating the metadata in the metadata store.
     *
     * @param streamName name of the stream
     * @return path (location) of the metadata of the stream.
     */
    String resolveStreamPath(String streamName);

}
