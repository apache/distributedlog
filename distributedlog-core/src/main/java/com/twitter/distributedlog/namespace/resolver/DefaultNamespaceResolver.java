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
     * Use zookeeper styple filesystem name.
     * @param streamName name of the stream
     * @throws InvalidStreamNameException
     */
    private void validateFileSystemLikeStreamName(String streamName) throws InvalidStreamNameException {
        if (streamName == null) {
            throw new InvalidStreamNameException("Stream name cannot be null");
        } else if (streamName.length() == 0) {
            throw new InvalidStreamNameException("Stream name length must be > 0");
        } else if (streamName.charAt(0) != 47) {
            throw new InvalidStreamNameException("Stream name must start with / character");
        } else if (streamName.length() != 1) {
            if (streamName.charAt(streamName.length() - 1) == 47) {
                throw new InvalidStreamNameException("Stream name must not end with / character");
            } else {
                String reason = null;
                char lastc = 47;
                char[] chars = streamName.toCharArray();

                for (int i = 1; i < chars.length; ++i) {
                    char c = chars[i];
                    if (c == 0) {
                        reason = "null character not allowed @" + i;
                        break;
                    }

                    if (c == '<' || c == '>') {
                        reason = "< or > specified @" + i;
                        break;
                    }

                    if (c == ' ') {
                        reason = "empty space specified @" + i;
                        break;
                    }

                    if (c == '/' && lastc == '/') {
                        reason = "empty node name specified @" + i;
                        break;
                    }

                    if (c == '.' && lastc == '.') {
                        if (chars[i - 2] == '/' && (i + 1 == chars.length || chars[i + 1] == '/')) {
                            reason = "relative paths not allowed @" + i;
                            break;
                        }
                    } else if (c == '.') {
                        if (chars[i - 1] == '/' && (i + 1 == chars.length || chars[i + 1] == '/')) {
                            reason = "relative paths not allowed @" + i;
                            break;
                        }
                    } else if (c > '\u0000' && c < '\u001f'
                            || c > '\u007f' && c < '\u009F'
                            || c > '\ud800' && c < '\uf8ff'
                            || c > '\ufff0' && c < '\uffff') {
                        reason = "invalid character @" + i;
                        break;
                    }
                    lastc = chars[i];
                }

                if (reason != null) {
                    throw new InvalidStreamNameException("Invalid stream name \"" + streamName + "\" caused by " + reason);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateStreamName(String nameOfStream)
            throws InvalidStreamNameException {
        String path = resolveStreamPath(nameOfStream);
        validateFileSystemLikeStreamName("/" + path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String resolveStreamPath(String streamName) {
        if (streamName.startsWith("/")) {
            return streamName.substring(1);
        } else {
            return streamName;
        }
    }
}
