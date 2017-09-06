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

import com.google.common.base.Optional;
import java.io.IOException;
import java.util.Iterator;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogManager;
import org.apache.distributedlog.DistributedLogManagerImpl;
import org.apache.distributedlog.acl.AccessControlManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.callback.NamespaceListener;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.exceptions.LogNotFoundException;

/**
 * A wapper over {@link org.apache.distributedlog.api.namespace.Namespace}.
 */
class DistributedLogNamespaceImpl implements DistributedLogNamespace {

    private static <T> java.util.Optional<T> gOptional2JOptional(Optional<T> gOptional) {
        if (gOptional.isPresent()) {
            return java.util.Optional.of(gOptional.get());
        } else {
            return java.util.Optional.empty();
        }
    }

    private final Namespace impl;

    DistributedLogNamespaceImpl(Namespace impl) {
        this.impl = impl;
    }

    @Override
    public NamespaceDriver getNamespaceDriver() {
        return impl.getNamespaceDriver();
    }

    @Override
    public void createLog(String logName) throws InvalidStreamNameException, IOException {
        impl.createLog(logName);
    }

    @Override
    public void deleteLog(String logName) throws InvalidStreamNameException, LogNotFoundException, IOException {
        impl.deleteLog(logName);
    }

    @Override
    public DistributedLogManager openLog(String logName) throws InvalidStreamNameException, IOException {
        return new DistributedLogManagerImpl(impl.openLog(logName));
    }

    @Override
    public DistributedLogManager openLog(String logName,
                                         Optional<DistributedLogConfiguration> logConf,
                                         Optional<DynamicDistributedLogConfiguration> dynamicLogConf,
                                         Optional<StatsLogger> perStreamStatsLogger)
            throws InvalidStreamNameException, IOException {
        return new DistributedLogManagerImpl(
            impl.openLog(
                logName,
                gOptional2JOptional(logConf),
                gOptional2JOptional(dynamicLogConf),
                gOptional2JOptional(perStreamStatsLogger)
            ));
    }

    @Override
    public boolean logExists(String logName) throws IOException {
        return impl.logExists(logName);
    }

    @Override
    public Iterator<String> getLogs() throws IOException {
        return impl.getLogs();
    }

    @Override
    public void registerNamespaceListener(NamespaceListener listener) {
        impl.registerNamespaceListener(listener);
    }

    @Override
    public AccessControlManager createAccessControlManager() throws IOException {
        return impl.createAccessControlManager();
    }

    @Override
    public void close() {
        impl.close();
    }
}
