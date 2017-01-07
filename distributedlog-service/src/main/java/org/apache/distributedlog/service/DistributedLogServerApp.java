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
package org.apache.distributedlog.service;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.distributedlog.util.CommandLineUtils.getOptionalBooleanArg;
import static org.apache.distributedlog.util.CommandLineUtils.getOptionalIntegerArg;
import static org.apache.distributedlog.util.CommandLineUtils.getOptionalStringArg;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.client.routing.RoutingService;
import org.apache.distributedlog.client.routing.RoutingUtils;
import org.apache.distributedlog.client.serverset.DLZkServerSet;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The launcher of the distributedlog proxy server.
 */
public class DistributedLogServerApp {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLogServerApp.class);

    private static final String USAGE = "DistributedLogServerApp [-u <uri>] [-c <conf>]";
    private final String[] args;
    private final Options options = new Options();

    private DistributedLogServerApp(String[] args) {
        this.args = args;

        // prepare options
        options.addOption("u", "uri", true, "DistributedLog URI");
        options.addOption("c", "conf", true, "DistributedLog Configuration File");
        options.addOption("sc", "stream-conf", true, "Per Stream Configuration Directory");
        options.addOption("p", "port", true, "DistributedLog Server Port");
        options.addOption("sp", "stats-port", true, "DistributedLog Stats Port");
        options.addOption("pd", "stats-provider", true, "DistributedLog Stats Provider");
        options.addOption("si", "shard-id", true, "DistributedLog Shard ID");
        options.addOption("a", "announce", false, "ServerSet Path to Announce");
        options.addOption("la", "load-appraiser", true, "LoadAppraiser Implementation to Use");
        options.addOption("mx", "thriftmux", false, "Is thriftmux enabled");
    }

    private void printUsage() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(USAGE, options);
    }

    private void run() {
        try {
            logger.info("Running distributedlog server : args = {}", Arrays.toString(args));
            BasicParser parser = new BasicParser();
            CommandLine cmdline = parser.parse(options, args);
            runCmd(cmdline);
        } catch (ParseException pe) {
            logger.error("Argument error : {}", pe.getMessage());
            printUsage();
            Runtime.getRuntime().exit(-1);
        } catch (IllegalArgumentException iae) {
            logger.error("Argument error : {}", iae.getMessage());
            printUsage();
            Runtime.getRuntime().exit(-1);
        } catch (ConfigurationException ce) {
            logger.error("Configuration error : {}", ce.getMessage());
            printUsage();
            Runtime.getRuntime().exit(-1);
        } catch (IOException ie) {
            logger.error("Failed to start distributedlog server : ", ie);
            Runtime.getRuntime().exit(-1);
        } catch (ClassNotFoundException cnf) {
          logger.error("Failed to start distributedlog server : ", cnf);
          Runtime.getRuntime().exit(-1);
        }
    }

    private void runCmd(CommandLine cmdline)
        throws IllegalArgumentException, IOException, ConfigurationException, ClassNotFoundException {
        final StatsReceiver statsReceiver = NullStatsReceiver.get();
        Optional<String> confOptional = getOptionalStringArg(cmdline, "c");
        DistributedLogConfiguration dlConf = new DistributedLogConfiguration();
        if (confOptional.isPresent()) {
            String configFile = confOptional.get();
            try {
                dlConf.loadConf(new File(configFile).toURI().toURL());
            } catch (ConfigurationException e) {
                throw new IllegalArgumentException("Failed to load distributedlog configuration from "
                    + configFile + ".");
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Failed to load distributedlog configuration from malformed "
                        + configFile + ".");
            }
        }
        // load the stats provider
        final StatsProvider statsProvider = getOptionalStringArg(cmdline, "pd")
                .transform(new Function<String, StatsProvider>() {
                    @Nullable
                    @Override
                    public StatsProvider apply(@Nullable String name) {
                        return ReflectionUtils.newInstance(name, StatsProvider.class);
                    }
                }).or(new NullStatsProvider());

        final Optional<String> uriOption = getOptionalStringArg(cmdline, "u");
        checkArgument(uriOption.isPresent(), "No distributedlog uri provided.");
        URI dlUri = URI.create(uriOption.get());

        DLZkServerSet serverSet = DLZkServerSet.of(dlUri, (int) TimeUnit.SECONDS.toMillis(60));
        RoutingService routingService = RoutingUtils.buildRoutingService(serverSet.getServerSet())
                .statsReceiver(statsReceiver.scope("routing"))
                .build();

        final DistributedLogServer server = DistributedLogServer.runServer(
                uriOption,
                confOptional,
                getOptionalStringArg(cmdline, "sc"),
                getOptionalIntegerArg(cmdline, "p"),
                getOptionalIntegerArg(cmdline, "sp"),
                getOptionalIntegerArg(cmdline, "si"),
                getOptionalBooleanArg(cmdline, "a"),
                getOptionalStringArg(cmdline, "la"),
                getOptionalBooleanArg(cmdline, "mx"),
                routingService,
                statsReceiver,
                statsProvider);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Closing DistributedLog Server.");
                server.close();
                logger.info("Closed DistributedLog Server.");
                statsProvider.stop();
            }
        });

        try {
            server.join();
        } catch (InterruptedException e) {
            logger.warn("Interrupted when waiting distributedlog server to be finished : ", e);
        }

        logger.info("DistributedLog Service Interrupted.");
        server.close();
        logger.info("Closed DistributedLog Server.");
        statsProvider.stop();
    }

    public static void main(String[] args) {
        final DistributedLogServerApp launcher = new DistributedLogServerApp(args);
        launcher.run();
    }
}
