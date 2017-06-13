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
package org.apache.distributedlog.service.config;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.common.config.ConcurrentConstConfiguration;
import org.apache.distributedlog.common.config.ConfigurationSubscription;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.common.config.FileConfigurationBuilder;
import org.apache.distributedlog.common.config.PropertiesConfigurationBuilder;
import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For all streams return the same dynamic config based on configFile.
 */
public class DefaultStreamConfigProvider implements StreamConfigProvider {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamConfigProvider.class);

    private final Optional<DynamicDistributedLogConfiguration> dynConf;
    private final ConfigurationSubscription confSub;

    public DefaultStreamConfigProvider(String configFilePath,
                                       ScheduledExecutorService executorService,
                                       int reloadPeriod,
                                       TimeUnit reloadUnit)
        throws ConfigurationException {
        try {
            File configFile = new File(configFilePath);
            FileConfigurationBuilder properties =
                new PropertiesConfigurationBuilder(configFile.toURI().toURL());
            ConcurrentConstConfiguration defaultConf =
                new ConcurrentConstConfiguration(new DistributedLogConfiguration());
            DynamicDistributedLogConfiguration conf =
                new DynamicDistributedLogConfiguration(defaultConf);
            List<FileConfigurationBuilder> fileConfigBuilders = Lists.newArrayList(properties);
            confSub = new ConfigurationSubscription(
                conf, fileConfigBuilders, executorService, reloadPeriod, reloadUnit);
            this.dynConf = Optional.of(conf);
        } catch (MalformedURLException ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Optional<DynamicDistributedLogConfiguration> getDynamicStreamConfig(String streamName) {
        return dynConf;
    }
}
