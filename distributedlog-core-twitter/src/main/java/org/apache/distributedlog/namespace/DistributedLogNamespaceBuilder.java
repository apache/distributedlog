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
package org.apache.distributedlog.namespace;

import com.google.common.base.Preconditions;
import org.apache.distributedlog.BKDistributedLogNamespace;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.feature.CoreFeatureKeys;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.injector.AsyncRandomFailureInjector;
import org.apache.distributedlog.util.ConfUtils;
import org.apache.distributedlog.util.DLUtils;
import org.apache.distributedlog.util.OrderedScheduler;
import org.apache.distributedlog.util.SimplePermitLimiter;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Builder to construct a <code>DistributedLogNamespace</code>.
 * The builder takes the responsibility of loading backend according to the uri.
 *
 * @see DistributedLogNamespace
 * @since 0.3.32
 */
public class DistributedLogNamespaceBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLogNamespaceBuilder.class);

    public static DistributedLogNamespaceBuilder newBuilder() {
        return new DistributedLogNamespaceBuilder();
    }

    private final NamespaceBuilder builder = NamespaceBuilder.newBuilder();

    // private constructor
    private DistributedLogNamespaceBuilder() {}

    /**
     * DistributedLog Configuration used for the namespace.
     *
     * @param conf
     *          distributedlog configuration
     * @return namespace builder.
     */
    public DistributedLogNamespaceBuilder conf(DistributedLogConfiguration conf) {
        this.builder.conf(conf);
        return this;
    }

    /**
     * Dynamic DistributedLog Configuration used for the namespace
     *
     * @param dynConf dynamic distributedlog configuration
     * @return namespace builder
     */
    public DistributedLogNamespaceBuilder dynConf(DynamicDistributedLogConfiguration dynConf) {
        this.builder.dynConf(dynConf);
        return this;
    }

    /**
     * Namespace Location.
     *
     * @param uri
     *          namespace location uri.
     * @see DistributedLogNamespace
     * @return namespace builder.
     */
    public DistributedLogNamespaceBuilder uri(URI uri) {
        this.builder.uri(uri);
        return this;
    }

    /**
     * Stats Logger used for stats collection
     *
     * @param statsLogger
     *          stats logger
     * @return namespace builder.
     */
    public DistributedLogNamespaceBuilder statsLogger(StatsLogger statsLogger) {
        this.builder.statsLogger(statsLogger);
        return this;
    }

    /**
     * Stats Logger used for collecting per log stats.
     *
     * @param statsLogger
     *          stats logger for collecting per log stats
     * @return namespace builder.
     */
    public DistributedLogNamespaceBuilder perLogStatsLogger(StatsLogger statsLogger) {
        this.builder.perLogStatsLogger(statsLogger);
        return this;
    }

    /**
     * Feature provider used to control the availabilities of features in the namespace.
     *
     * @param featureProvider
     *          feature provider to control availabilities of features.
     * @return namespace builder.
     */
    public DistributedLogNamespaceBuilder featureProvider(FeatureProvider featureProvider) {
        this.builder.featureProvider(featureProvider);
        return this;
    }

    /**
     * Client Id used for accessing the namespace
     *
     * @param clientId
     *          client id used for accessing the namespace
     * @return namespace builder.
     */
    public DistributedLogNamespaceBuilder clientId(String clientId) {
        this.builder.clientId(clientId);
        return this;
    }

    /**
     * Region Id used for encoding logs in the namespace. The region id
     * is useful when the namespace is globally spanning over regions.
     *
     * @param regionId
     *          region id.
     * @return namespace builder.
     */
    public DistributedLogNamespaceBuilder regionId(int regionId) {
        this.builder.regionId(regionId);
        return this;
    }

    @SuppressWarnings("deprecation")
    private static StatsLogger normalizePerLogStatsLogger(StatsLogger statsLogger,
                                                          StatsLogger perLogStatsLogger,
                                                          DistributedLogConfiguration conf) {
        StatsLogger normalizedPerLogStatsLogger = perLogStatsLogger;
        if (perLogStatsLogger == NullStatsLogger.INSTANCE &&
                conf.getEnablePerStreamStat()) {
            normalizedPerLogStatsLogger = statsLogger.scope("stream");
        }
        return normalizedPerLogStatsLogger;
    }

    /**
     * Build the namespace.
     *
     * @return the namespace instance.
     * @throws IllegalArgumentException when there is illegal argument provided in the builder
     * @throws NullPointerException when there is null argument provided in the builder
     * @throws IOException when fail to build the backend
     */
    public DistributedLogNamespace build()
            throws IllegalArgumentException, NullPointerException, IOException {

    }
}
