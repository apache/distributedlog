/*
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

package org.apache.distributedlog.namespace;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Optional;
import java.util.Iterator;
import org.apache.distributedlog.acl.AccessControlManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.callback.NamespaceListener;
import org.junit.Test;

/**
 * Unit test of {@link DistributedLogNamespaceImpl}.
 */
public class TestDistributedLogNamespaceImpl {

    private final Namespace impl = mock(Namespace.class);
    private final DistributedLogNamespaceImpl namespace = new DistributedLogNamespaceImpl(impl);

    @Test
    public void testGetNamespaceDriver() {
        NamespaceDriver driver = mock(NamespaceDriver.class);
        when(impl.getNamespaceDriver()).thenReturn(driver);
        assertEquals(driver, namespace.getNamespaceDriver());
        verify(impl, times(1)).getNamespaceDriver();
    }

    @Test
    public void testCreateLog() throws Exception {
        String logName = "test-log-name";
        namespace.createLog(logName);
        verify(impl, times(1)).createLog(eq(logName));
    }

    @Test
    public void testDeleteLog() throws Exception {
        String logName = "test-log-name";
        namespace.deleteLog(logName);
        verify(impl, times(1)).deleteLog(eq(logName));
    }

    @Test
    public void testOpenLog() throws Exception {
        String logName = "test-open-log";
        namespace.openLog(logName);
        verify(impl, times(1)).openLog(eq(logName));
    }

    @Test
    public void testOpenLog2() throws Exception {
        String logName = "test-open-log";
        namespace.openLog(logName, Optional.absent(), Optional.absent(), Optional.absent());
        verify(impl, times(1))
            .openLog(
                eq(logName),
                eq(java.util.Optional.empty()),
                eq(java.util.Optional.empty()),
                eq(java.util.Optional.empty()));
    }

    @Test
    public void testLogExists() throws Exception {
        String logName = "test-log-exists";
        when(impl.logExists(anyString())).thenReturn(true);
        assertTrue(namespace.logExists(logName));
        verify(impl, times(1)).logExists(eq(logName));
    }

    @Test
    public void testGetLogs() throws Exception {
        Iterator<String> logs = mock(Iterator.class);
        when(impl.getLogs()).thenReturn(logs);
        assertEquals(logs, namespace.getLogs());
        verify(impl, times(1)).getLogs();
    }

    @Test
    public void testRegisterNamespaceListener() throws Exception {
        NamespaceListener listener = mock(NamespaceListener.class);
        namespace.registerNamespaceListener(listener);
        verify(impl, times(1)).registerNamespaceListener(eq(listener));
    }

    @Test
    public void testCreateAccessControlManager() throws Exception {
        AccessControlManager manager = mock(AccessControlManager.class);
        when(impl.createAccessControlManager()).thenReturn(manager);
        assertEquals(manager, namespace.createAccessControlManager());
        verify(impl, times(1)).createAccessControlManager();
    }

    @Test
    public void testClose() {
        namespace.close();
        verify(impl, times(1)).close();
    }

}
