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
package com.twitter.distributedlog.fs;

import com.google.common.base.Optional;
import com.twitter.distributedlog.AppendOnlyStreamReader;
import com.twitter.distributedlog.AppendOnlyStreamWriter;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;

/**
 * A FileSystem Implementation powered by replicated logs
 */
public class DistributedLogFileSystem extends FileSystem {

    private final Logger logger = LoggerFactory.getLogger(DistributedLogFileSystem.class);

    //
    // Settings
    //

    public static final String DLFS_CONF_FILE = "dlfs.configuration.file";


    private URI rootUri;
    private DistributedLogNamespace namespace;
    private final DistributedLogConfiguration dlConf;
    private Path workingDir;

    public DistributedLogFileSystem() {
        this.dlConf = new DistributedLogConfiguration();
    }

    @Override
    public URI getUri() {
        return rootUri;
    }

    //
    // Initialization
    //

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        // initialize

        this.rootUri = name;
        // load the configuration
        String dlConfLocation = conf.get(DLFS_CONF_FILE);
        if (null != dlConfLocation) {
            try {
                this.dlConf.loadConf(new File(dlConfLocation).toURI().toURL());
                logger.info("Loaded the distributedlog configuration from {}", dlConfLocation);
            } catch (ConfigurationException e) {
                logger.error("Failed to load the distributedlog configuration from " + dlConfLocation, e);
                throw new IOException("Failed to load distributedlog configuration from " + dlConfLocation);
            }
        }
        // initialize the namespace
        this.namespace = DistributedLogNamespaceBuilder.newBuilder()
                .clientId("dlfs-client-" + InetAddress.getLocalHost().getHostName())
                .conf(dlConf)
                .regionId(DistributedLogConstants.LOCAL_REGION_ID)
                .uri(name)
                .build();
        logger.info("Initialized the filesystem at {}", name);
    }

    @Override
    public void close() throws IOException {
        // clean up the resource
        namespace.close();
        super.close();
    }

    //
    // Util Functions
    //

    private Path makeAbsolute(Path f) {
        if (f.isAbsolute()) {
            return f;
        } else {
            return new Path(workingDir, f);
        }
    }

    private String getStreamName(Path absolutePath) {
        return absolutePath.toUri().getPath().substring(1);
    }

    //
    // Home & Working Directory
    //

    @Override
    public Path getHomeDirectory() {
        return this.makeQualified(new Path(System.getProperty("user.home")));
    }

    protected Path getInitialWorkingDirectory() {
        return this.makeQualified(new Path(System.getProperty("user.dir")));
    }

    @Override
    public void setWorkingDirectory(Path path) {
        workingDir = makeAbsolute(path);
        checkPath(workingDir);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }


    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException {
        Path absolutePath = makeAbsolute(path);
        DistributedLogManager dlm = namespace.openLog(getStreamName(absolutePath));
        AppendOnlyStreamReader reader;
        try {
            reader = dlm.getAppendOnlyStreamReader();
        } catch (LogNotFoundException lnfe) {
            throw new FileNotFoundException(path.toString());
        } catch (LogEmptyException lee) {
            throw new FileNotFoundException(path.toString());
        }
        return new FSDataInputStream(
                new BufferedFSInputStream(
                        new DistributedLogInputStream(dlm, reader),
                        bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path path,
                                     FsPermission fsPermission,
                                     boolean overwrite,
                                     int bufferSize,
                                     short replication,
                                     long blockSize,
                                     Progressable progressable) throws IOException {

        // TODO: support overwrite, support permission
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(dlConf);
        confLocal.setEnsembleSize(replication);
        confLocal.setWriteQuorumSize(replication);
        confLocal.setAckQuorumSize(replication);
        confLocal.setMaxLogSegmentBytes(blockSize);
        return append(path, bufferSize, Optional.of(confLocal));
    }

    @Override
    public FSDataOutputStream append(Path path,
                                     int bufferSize,
                                     Progressable progressable) throws IOException {
        return append(path, bufferSize, Optional.<DistributedLogConfiguration>absent());
    }

    private FSDataOutputStream append(Path path,
                                      int bufferSize,
                                      Optional<DistributedLogConfiguration> confLocal)
            throws IOException {
        Path absolutePath = makeAbsolute(path);
        DistributedLogManager dlm = namespace.openLog(
                getStreamName(absolutePath),
                confLocal,
                Optional.<DynamicDistributedLogConfiguration>absent());
        AppendOnlyStreamWriter streamWriter = dlm.getAppendOnlyStreamWriter();
        return new FSDataOutputStream(new BufferedOutputStream(
                new DistributedLogOutputStream(dlm, streamWriter), bufferSize), statistics);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        Path absolutePath = makeAbsolute(path);
        namespace.deleteLog(getStreamName(absolutePath));
        return true;
    }

    //
    // Not Supported
    //

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Rename is not supported yet");
    }

    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        throw new UnsupportedOperationException("Truncate is not supported yet");
    }
}
