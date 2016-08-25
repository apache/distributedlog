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

import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.LogRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestOffsetSequencer {

    @Test(timeout = 60000)
    public void testAdvance() {
        OffsetSequencer sequencer = new OffsetSequencer();
        sequencer.setLastId(999L);
        assertEquals(1000L, sequencer.nextId());
        // call next id again will return same offset
        assertEquals(1000L, sequencer.nextId());
        // advance with control record will not advance offset
        LogRecord record = DLMTestUtil.getLogRecordInstance(1000L, 100);
        record.setControl();
        sequencer.advance(record);
        assertEquals(1000L, sequencer.nextId());
        // advance with user record will advance the offset
        record = DLMTestUtil.getLogRecordInstance(1000L, 100);
        sequencer.advance(record);
        assertEquals(1000L + 100, sequencer.nextId());
    }
}
