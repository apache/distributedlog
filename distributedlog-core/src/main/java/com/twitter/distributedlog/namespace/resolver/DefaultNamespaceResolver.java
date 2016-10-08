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
 * Default namespace resolver implementation.
 * <p>It is a flat namespace resolver. It resolves all the stream
 * under a same flat hierarchy.
 */
public class DefaultNamespaceResolver implements NamespaceResolver {

    public static final DefaultNamespaceResolver INSTANCE = new DefaultNamespaceResolver();

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateStreamName(String nameOfStream)
            throws InvalidStreamNameException {
        String reason = null;
        char chars[] = nameOfStream.toCharArray();
        char c;
        // validate the stream to see if meet zookeeper path's requirement
        for (int i = 0; i < chars.length; i++) {
            c = chars[i];

            if (c == 0) {
                reason = "null character not allowed @" + i;
                break;
            } else if (c == '/') {
                reason = "'/' not allowed @" + i;
                break;
            } else if (c > '\u0000' && c < '\u001f'
                    || c > '\u007f' && c < '\u009F'
                    || c > '\ud800' && c < '\uf8ff'
                    || c > '\ufff0' && c < '\uffff') {
                reason = "invalid charater @" + i;
                break;
            }
        }
        if (null != reason) {
            throw new InvalidStreamNameException(nameOfStream, reason);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String resolveStreamPath(String streamName) {
        return streamName;
    }
}
