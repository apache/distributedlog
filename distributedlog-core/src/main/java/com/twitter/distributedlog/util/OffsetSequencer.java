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
package com.twitter.distributedlog.util;

import com.twitter.distributedlog.LogRecord;

/**
 * Offset based sequencer. It generated non-decreasing transaction id using offsets.
 * It isn't thread-safe. The caller takes the responsibility on synchronization.
 */
public class OffsetSequencer implements Sequencer {

    private long lastOffset = -1L;

    @Override
    public void setLastId(long lastId) {
        // NOTE: it is a bit tricky here. when a log stream is recovered, we can only get the last transaction id,
        //       but not the length of last record. so we don't know how many `bytes` to advance. so just to advance
        //       one here.
        this.lastOffset = lastId + 1;
    }

    @Override
    public long nextId() {
        return lastOffset;
    }

    @Override
    public void advance(LogRecord record) {
        // skip the control records (they are invisible to the users)
        if (record.isControl()) {
            return;
        }
        this.lastOffset += record.getPayload().length;
    }
}
