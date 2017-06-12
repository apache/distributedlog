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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.acl.DefaultAccessControlManager;
import org.apache.distributedlog.client.routing.LocalRoutingService;
import org.apache.distributedlog.exceptions.OwnershipAcquireFailedException;
import org.apache.distributedlog.exceptions.StreamUnavailableException;
import org.apache.distributedlog.protocol.util.ProtocolUtils;
import org.apache.distributedlog.service.config.NullStreamConfigProvider;
import org.apache.distributedlog.service.config.ServerConfiguration;
import org.apache.distributedlog.service.placement.EqualLoadAppraiser;
import org.apache.distributedlog.service.stream.Stream;
import org.apache.distributedlog.service.stream.StreamImpl;
import org.apache.distributedlog.service.stream.StreamImpl.StreamStatus;
import org.apache.distributedlog.service.stream.StreamManagerImpl;
import org.apache.distributedlog.service.stream.WriteOp;
import org.apache.distributedlog.service.streamset.DelimiterStreamPartitionConverter;
import org.apache.distributedlog.service.streamset.IdentityStreamPartitionConverter;
import org.apache.distributedlog.service.streamset.StreamPartitionConverter;
import org.apache.distributedlog.thrift.service.HeartbeatOptions;
import org.apache.distributedlog.thrift.service.StatusCode;
import org.apache.distributedlog.thrift.service.WriteContext;
import org.apache.distributedlog.thrift.service.WriteResponse;
import org.apache.distributedlog.util.ConfUtils;
import com.twitter.util.Await;
import com.twitter.util.Future;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Case for DistributedLog Service.
 */
public class TestDistributedLogService extends TestDistributedLogBase {

    private static final Logger logger = LoggerFactory.getLogger(TestDistributedLogService.class);

    @Rule
    public TestName testName = new TestName();

    private ServerConfiguration serverConf;
    private DistributedLogConfiguration dlConf;
    private URI uri;
    private final CountDownLatch latch = new CountDownLatch(1);
    private DistributedLogServiceImpl service;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        dlConf = new DistributedLogConfiguration();
        dlConf.addConfiguration(conf);
        dlConf.setLockTimeout(0)
                .setOutputBufferSize(0)
                .setPeriodicFlushFrequencyMilliSeconds(10)
                .setSchedulerShutdownTimeoutMs(100);
        serverConf = newLocalServerConf();
        uri = createDLMURI("/" + testName.getMethodName());
        ensureURICreated(uri);
        service = createService(serverConf, dlConf, latch);
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (null != service) {
            service.shutdown();
        }
        super.teardown();
    }

    private DistributedLogConfiguration newLocalConf() {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(dlConf);
        return confLocal;
    }

    private ServerConfiguration newLocalServerConf() {
        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(dlConf);
        serverConf.setServerThreads(1);
        return serverConf;
    }

    private DistributedLogServiceImpl createService(
            ServerConfiguration serverConf,
            DistributedLogConfiguration dlConf) throws Exception {
        return createService(serverConf, dlConf, new CountDownLatch(1));
    }

    private DistributedLogServiceImpl createService(
            ServerConfiguration serverConf,
            DistributedLogConfiguration dlConf,
            CountDownLatch latch) throws Exception {
        // Build the stream partition converter
        StreamPartitionConverter converter;
        try {
            converter = ReflectionUtils.newInstance(serverConf.getStreamPartitionConverterClass());
        } catch (ConfigurationException e) {
            logger.warn("Failed to load configured stream-to-partition converter. Fallback to use {}",
                    IdentityStreamPartitionConverter.class.getName());
            converter = new IdentityStreamPartitionConverter();
        }
        return new DistributedLogServiceImpl(
            serverConf,
            dlConf,
            ConfUtils.getConstDynConf(dlConf),
            new NullStreamConfigProvider(),
            uri,
            converter,
            new LocalRoutingService(),
            NullStatsLogger.INSTANCE,
            NullStatsLogger.INSTANCE,
            latch,
            new EqualLoadAppraiser());
    }

    private StreamImpl createUnstartedStream(DistributedLogServiceImpl service,
                                             String name) throws Exception {
        StreamImpl stream = (StreamImpl) service.newStream(name);
        stream.initialize();
        return stream;
    }

    private ByteBuffer createRecord(long txid) {
        return ByteBuffer.wrap(("record-" + txid).getBytes(UTF_8));
    }

    private WriteOp createWriteOp(DistributedLogServiceImpl service,
                                  String streamName,
                                  long txid) {
        ByteBuffer data = createRecord(txid);
        return service.newWriteOp(streamName, data, null);
    }

    @Test(timeout = 60000)
    public void testAcquireStreams() throws Exception {
        String streamName = testName.getMethodName();
        StreamImpl s0 = createUnstartedStream(service, streamName);
        ServerConfiguration serverConf1 = new ServerConfiguration();
        serverConf1.addConfiguration(serverConf);
        serverConf1.setServerPort(9999);
        DistributedLogServiceImpl service1 = createService(serverConf1, dlConf);
        StreamImpl s1 = createUnstartedStream(service1, streamName);

        // create write ops
        WriteOp op0 = createWriteOp(service, streamName, 0L);
        s0.submit(op0);

        WriteOp op1 = createWriteOp(service1, streamName, 1L);
        s1.submit(op1);

        // check pending size
        assertEquals("Write Op 0 should be pending in service 0",
                1, s0.numPendingOps());
        assertEquals("Write Op 1 should be pending in service 1",
                1, s1.numPendingOps());

        // start acquiring s0
        s0.start();
        WriteResponse wr0 = Await.result(op0.result());
        assertEquals("Op 0 should succeed",
                StatusCode.SUCCESS, wr0.getHeader().getCode());
        assertEquals("Service 0 should acquire stream",
                StreamStatus.INITIALIZED, s0.getStatus());
        assertNotNull(s0.getManager());
        assertNotNull(s0.getWriter());
        assertNull(s0.getLastException());

        // start acquiring s1
        s1.start();
        WriteResponse wr1 = Await.result(op1.result());
        assertEquals("Op 1 should fail",
                StatusCode.FOUND, wr1.getHeader().getCode());
        // the stream will be set to ERROR and then be closed.
        assertTrue("Service 1 should be in unavailable state",
                StreamStatus.isUnavailable(s1.getStatus()));
        assertNotNull(s1.getManager());
        assertNull(s1.getWriter());
        assertNotNull(s1.getLastException());
        assertTrue(s1.getLastException() instanceof OwnershipAcquireFailedException);

        service1.shutdown();
    }

    @Test(timeout = 60000)
    public void testAcquireStreamsWhenExceedMaxCachedPartitions() throws Exception {
        String streamName = testName.getMethodName() + "_0000";

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(dlConf);
        confLocal.setMaxCachedPartitionsPerProxy(1);

        ServerConfiguration serverConfLocal = new ServerConfiguration();
        serverConfLocal.addConfiguration(serverConf);
        serverConfLocal.setStreamPartitionConverterClass(DelimiterStreamPartitionConverter.class);

        DistributedLogServiceImpl serviceLocal = createService(serverConfLocal, confLocal);
        Stream stream = serviceLocal.getLogWriter(streamName);

        // stream is cached
        assertNotNull(stream);
        assertEquals(1, serviceLocal.getStreamManager().numCached());

        // create write ops
        WriteOp op0 = createWriteOp(service, streamName, 0L);
        stream.submit(op0);
        WriteResponse wr0 = Await.result(op0.result());
        assertEquals("Op 0 should succeed",
                StatusCode.SUCCESS, wr0.getHeader().getCode());
        assertEquals(1, serviceLocal.getStreamManager().numAcquired());

        // should fail to acquire another partition
        try {
            serviceLocal.getLogWriter(testName.getMethodName() + "_0001");
            fail("Should fail to acquire new streams");
        } catch (StreamUnavailableException sue) {
            // expected
        }
        assertEquals(1, serviceLocal.getStreamManager().numCached());
        assertEquals(1, serviceLocal.getStreamManager().numAcquired());

        // should be able to acquire partitions from other streams
        String anotherStreamName = testName.getMethodName() + "-another_0001";
        Stream anotherStream = serviceLocal.getLogWriter(anotherStreamName);
        assertNotNull(anotherStream);
        assertEquals(2, serviceLocal.getStreamManager().numCached());

        // create write ops
        WriteOp op1 = createWriteOp(service, anotherStreamName, 0L);
        anotherStream.submit(op1);
        WriteResponse wr1 = Await.result(op1.result());
        assertEquals("Op 1 should succeed",
                StatusCode.SUCCESS, wr1.getHeader().getCode());
        assertEquals(2, serviceLocal.getStreamManager().numAcquired());
    }

    @Test(timeout = 60000)
    public void testAcquireStreamsWhenExceedMaxAcquiredPartitions() throws Exception {
        String streamName = testName.getMethodName() + "_0000";

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(dlConf);
        confLocal.setMaxCachedPartitionsPerProxy(-1);
        confLocal.setMaxAcquiredPartitionsPerProxy(1);

        ServerConfiguration serverConfLocal = new ServerConfiguration();
        serverConfLocal.addConfiguration(serverConf);
        serverConfLocal.setStreamPartitionConverterClass(DelimiterStreamPartitionConverter.class);

        DistributedLogServiceImpl serviceLocal = createService(serverConfLocal, confLocal);
        Stream stream = serviceLocal.getLogWriter(streamName);

        // stream is cached
        assertNotNull(stream);
        assertEquals(1, serviceLocal.getStreamManager().numCached());

        // create write ops
        WriteOp op0 = createWriteOp(service, streamName, 0L);
        stream.submit(op0);
        WriteResponse wr0 = Await.result(op0.result());
        assertEquals("Op 0 should succeed",
                StatusCode.SUCCESS, wr0.getHeader().getCode());
        assertEquals(1, serviceLocal.getStreamManager().numAcquired());

        // should be able to cache partitions from same stream
        String anotherStreamName = testName.getMethodName() + "_0001";
        Stream anotherStream = serviceLocal.getLogWriter(anotherStreamName);
        assertNotNull(anotherStream);
        assertEquals(2, serviceLocal.getStreamManager().numCached());

        // create write ops
        WriteOp op1 = createWriteOp(service, anotherStreamName, 0L);
        anotherStream.submit(op1);
        WriteResponse wr1 = Await.result(op1.result());
        assertEquals("Op 1 should fail",
                StatusCode.STREAM_UNAVAILABLE, wr1.getHeader().getCode());
        assertEquals(1, serviceLocal.getStreamManager().numAcquired());
    }

    @Test(timeout = 60000)
    public void testCloseShouldErrorOutPendingOps() throws Exception {
        String streamName = testName.getMethodName();
        StreamImpl s = createUnstartedStream(service, streamName);

        int numWrites = 10;
        List<Future<WriteResponse>> futureList = new ArrayList<Future<WriteResponse>>(numWrites);
        for (int i = 0; i < numWrites; i++) {
            WriteOp op = createWriteOp(service, streamName, i);
            s.submit(op);
            futureList.add(op.result());
        }
        assertEquals(numWrites, s.numPendingOps());
        Await.result(s.requestClose("close stream"));
        assertEquals("Stream " + streamName + " is set to " + StreamStatus.CLOSED,
                StreamStatus.CLOSED, s.getStatus());
        for (int i = 0; i < numWrites; i++) {
            Future<WriteResponse> future = futureList.get(i);
            WriteResponse wr = Await.result(future);
            assertEquals("Pending op should fail with " + StatusCode.STREAM_UNAVAILABLE,
                    StatusCode.STREAM_UNAVAILABLE, wr.getHeader().getCode());
        }
    }

    @Test(timeout = 60000)
    public void testCloseTwice() throws Exception {
        String streamName = testName.getMethodName();
        StreamImpl s = createUnstartedStream(service, streamName);

        int numWrites = 10;
        List<Future<WriteResponse>> futureList = new ArrayList<Future<WriteResponse>>(numWrites);
        for (int i = 0; i < numWrites; i++) {
            WriteOp op = createWriteOp(service, streamName, i);
            s.submit(op);
            futureList.add(op.result());
        }
        assertEquals(numWrites, s.numPendingOps());

        Future<Void> closeFuture0 = s.requestClose("close 0");
        assertTrue("Stream " + streamName + " should be set to " + StreamStatus.CLOSING,
                StreamStatus.CLOSING == s.getStatus()
                    || StreamStatus.CLOSED == s.getStatus());
        Future<Void> closeFuture1 = s.requestClose("close 1");
        assertTrue("Stream " + streamName + " should be set to " + StreamStatus.CLOSING,
                StreamStatus.CLOSING == s.getStatus()
                    || StreamStatus.CLOSED == s.getStatus());

        Await.result(closeFuture0);
        assertEquals("Stream " + streamName + " should be set to " + StreamStatus.CLOSED,
                StreamStatus.CLOSED, s.getStatus());
        Await.result(closeFuture1);
        assertEquals("Stream " + streamName + " should be set to " + StreamStatus.CLOSED,
                StreamStatus.CLOSED, s.getStatus());

        for (int i = 0; i < numWrites; i++) {
            Future<WriteResponse> future = futureList.get(i);
            WriteResponse wr = Await.result(future);
            assertEquals("Pending op should fail with " + StatusCode.STREAM_UNAVAILABLE,
                    StatusCode.STREAM_UNAVAILABLE, wr.getHeader().getCode());
        }
    }

    @Test(timeout = 60000)
    public void testFailRequestsDuringClosing() throws Exception {
        String streamName = testName.getMethodName();
        StreamImpl s = createUnstartedStream(service, streamName);

        Future<Void> closeFuture = s.requestClose("close");
        assertTrue("Stream " + streamName + " should be set to " + StreamStatus.CLOSING,
                StreamStatus.CLOSING == s.getStatus()
                    || StreamStatus.CLOSED == s.getStatus());
        WriteOp op1 = createWriteOp(service, streamName, 0L);
        s.submit(op1);
        WriteResponse response1 = Await.result(op1.result());
        assertEquals("Op should fail with " + StatusCode.STREAM_UNAVAILABLE + " if it is closing",
                StatusCode.STREAM_UNAVAILABLE, response1.getHeader().getCode());

        Await.result(closeFuture);
        assertEquals("Stream " + streamName + " should be set to " + StreamStatus.CLOSED,
                StreamStatus.CLOSED, s.getStatus());
        WriteOp op2 = createWriteOp(service, streamName, 1L);
        s.submit(op2);
        WriteResponse response2 = Await.result(op2.result());
        assertEquals("Op should fail with " + StatusCode.STREAM_UNAVAILABLE + " if it is closed",
                StatusCode.STREAM_UNAVAILABLE, response2.getHeader().getCode());
    }

    @Test(timeout = 60000)
    public void testServiceTimeout() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setOutputBufferSize(Integer.MAX_VALUE)
                .setImmediateFlushEnabled(false)
                .setPeriodicFlushFrequencyMilliSeconds(0);
        ServerConfiguration serverConfLocal = newLocalServerConf();
        serverConfLocal.addConfiguration(serverConf);
        serverConfLocal.setServiceTimeoutMs(200)
                .setStreamProbationTimeoutMs(100);
        String streamName = testName.getMethodName();
        // create a new service with 200ms timeout
        DistributedLogServiceImpl localService = createService(serverConfLocal, confLocal);
        StreamManagerImpl streamManager = (StreamManagerImpl) localService.getStreamManager();

        int numWrites = 10;
        List<Future<WriteResponse>> futureList = new ArrayList<Future<WriteResponse>>(numWrites);
        for (int i = 0; i < numWrites; i++) {
            futureList.add(localService.write(streamName, createRecord(i)));
        }

        assertTrue("Stream " + streamName + " should be cached",
                streamManager.getCachedStreams().containsKey(streamName));

        StreamImpl s = (StreamImpl) streamManager.getCachedStreams().get(streamName);
        // the stream should be set CLOSING
        while (StreamStatus.CLOSING != s.getStatus()
            && StreamStatus.CLOSED != s.getStatus()) {
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertNotNull("Writer should be initialized", s.getWriter());
        assertNull("No exception should be thrown", s.getLastException());
        Future<Void> closeFuture = s.getCloseFuture();
        Await.result(closeFuture);
        for (int i = 0; i < numWrites; i++) {
            assertTrue("Write should not fail before closing",
                    futureList.get(i).isDefined());
            WriteResponse response = Await.result(futureList.get(i));
            assertTrue("Op should fail with " + StatusCode.WRITE_CANCELLED_EXCEPTION
                    + " but " + response.getHeader().getCode() + " is received.",
                    StatusCode.BK_TRANSMIT_ERROR == response.getHeader().getCode()
                        || StatusCode.WRITE_EXCEPTION == response.getHeader().getCode()
                        || StatusCode.WRITE_CANCELLED_EXCEPTION == response.getHeader().getCode());
        }

        while (streamManager.getCachedStreams().containsKey(streamName)) {
            TimeUnit.MILLISECONDS.sleep(20);
        }

        assertFalse("Stream should be removed from cache",
                streamManager.getCachedStreams().containsKey(streamName));
        assertFalse("Stream should be removed from acquired cache",
                streamManager.getAcquiredStreams().containsKey(streamName));

        localService.shutdown();
    }

    private DistributedLogServiceImpl createConfiguredLocalService() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setOutputBufferSize(0)
                .setImmediateFlushEnabled(true)
                .setPeriodicFlushFrequencyMilliSeconds(0);
        return createService(serverConf, confLocal);
    }

    private ByteBuffer getTestDataBuffer() {
        return ByteBuffer.wrap("test-data".getBytes());
    }

    @Test(timeout = 60000)
    public void testNonDurableWrite() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setOutputBufferSize(Integer.MAX_VALUE)
                .setImmediateFlushEnabled(false)
                .setPeriodicFlushFrequencyMilliSeconds(0)
                .setDurableWriteEnabled(false);
        ServerConfiguration serverConfLocal = new ServerConfiguration();
        serverConfLocal.addConfiguration(serverConf);
        serverConfLocal.enableDurableWrite(false);
        serverConfLocal.setServiceTimeoutMs(Integer.MAX_VALUE)
                .setStreamProbationTimeoutMs(Integer.MAX_VALUE);
        String streamName = testName.getMethodName();
        DistributedLogServiceImpl localService =
                createService(serverConfLocal, confLocal);
        StreamManagerImpl streamManager = (StreamManagerImpl) localService.getStreamManager();

        int numWrites = 10;
        List<Future<WriteResponse>> futureList = new ArrayList<Future<WriteResponse>>();
        for (int i = 0; i < numWrites; i++) {
            futureList.add(localService.write(streamName, createRecord(i)));
        }
        assertTrue("Stream " + streamName + " should be cached",
                streamManager.getCachedStreams().containsKey(streamName));
        List<WriteResponse> resultList = Await.result(Future.collect(futureList));
        for (WriteResponse wr : resultList) {
            assertEquals(DLSN.InvalidDLSN, DLSN.deserialize(wr.getDlsn()));
        }

        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testWriteOpNoChecksum() throws Exception {
        DistributedLogServiceImpl localService = createConfiguredLocalService();
        WriteContext ctx = new WriteContext();
        Future<WriteResponse> result = localService.writeWithContext("test", getTestDataBuffer(), ctx);
        WriteResponse resp = Await.result(result);
        assertEquals(StatusCode.SUCCESS, resp.getHeader().getCode());
        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testTruncateOpNoChecksum() throws Exception {
        DistributedLogServiceImpl localService = createConfiguredLocalService();
        WriteContext ctx = new WriteContext();
        Future<WriteResponse> result = localService.truncate("test", new DLSN(1, 2, 3).serialize(), ctx);
        WriteResponse resp = Await.result(result);
        assertEquals(StatusCode.SUCCESS, resp.getHeader().getCode());
        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testStreamOpNoChecksum() throws Exception {
        DistributedLogServiceImpl localService = createConfiguredLocalService();
        WriteContext ctx = new WriteContext();
        HeartbeatOptions option = new HeartbeatOptions();
        option.setSendHeartBeatToReader(true);

        // hearbeat to acquire the stream and then release the stream
        Future<WriteResponse> result = localService.heartbeatWithOptions("test", ctx, option);
        WriteResponse resp = Await.result(result);
        assertEquals(StatusCode.SUCCESS, resp.getHeader().getCode());
        result = localService.release("test", ctx);
        resp = Await.result(result);
        assertEquals(StatusCode.SUCCESS, resp.getHeader().getCode());

        // heartbeat to acquire the stream and then delete the stream
        result = localService.heartbeatWithOptions("test", ctx, option);
        resp = Await.result(result);
        assertEquals(StatusCode.SUCCESS, resp.getHeader().getCode());
        result = localService.delete("test", ctx);
        resp = Await.result(result);
        assertEquals(StatusCode.SUCCESS, resp.getHeader().getCode());

        // shutdown the local service
        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testWriteOpChecksumBadChecksum() throws Exception {
        DistributedLogServiceImpl localService = createConfiguredLocalService();
        WriteContext ctx = new WriteContext().setCrc32(999);
        Future<WriteResponse> result = localService.writeWithContext("test", getTestDataBuffer(), ctx);
        WriteResponse resp = Await.result(result);
        assertEquals(StatusCode.CHECKSUM_FAILED, resp.getHeader().getCode());
        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testWriteOpChecksumBadStream() throws Exception {
        DistributedLogServiceImpl localService = createConfiguredLocalService();
        WriteContext ctx = new WriteContext().setCrc32(
            ProtocolUtils.writeOpCRC32("test", getTestDataBuffer().array()));
        Future<WriteResponse> result = localService.writeWithContext("test1", getTestDataBuffer(), ctx);
        WriteResponse resp = Await.result(result);
        assertEquals(StatusCode.CHECKSUM_FAILED, resp.getHeader().getCode());
        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testWriteOpChecksumBadData() throws Exception {
        DistributedLogServiceImpl localService = createConfiguredLocalService();
        ByteBuffer buffer = getTestDataBuffer();
        WriteContext ctx = new WriteContext().setCrc32(
            ProtocolUtils.writeOpCRC32("test", buffer.array()));

        // Overwrite 1 byte to corrupt data.
        buffer.put(1, (byte) 0xab);
        Future<WriteResponse> result = localService.writeWithContext("test", buffer, ctx);
        WriteResponse resp = Await.result(result);
        assertEquals(StatusCode.CHECKSUM_FAILED, resp.getHeader().getCode());
        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testStreamOpChecksumBadChecksum() throws Exception {
        DistributedLogServiceImpl localService = createConfiguredLocalService();
        WriteContext ctx = new WriteContext().setCrc32(999);
        Future<WriteResponse> result = localService.heartbeat("test", ctx);
        WriteResponse resp = Await.result(result);
        assertEquals(StatusCode.CHECKSUM_FAILED, resp.getHeader().getCode());
        result = localService.release("test", ctx);
        resp = Await.result(result);
        assertEquals(StatusCode.CHECKSUM_FAILED, resp.getHeader().getCode());
        result = localService.delete("test", ctx);
        resp = Await.result(result);
        assertEquals(StatusCode.CHECKSUM_FAILED, resp.getHeader().getCode());
        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testTruncateOpChecksumBadChecksum() throws Exception {
        DistributedLogServiceImpl localService = createConfiguredLocalService();
        WriteContext ctx = new WriteContext().setCrc32(999);
        Future<WriteResponse> result = localService.truncate("test", new DLSN(1, 2, 3).serialize(), ctx);
        WriteResponse resp = Await.result(result);
        assertEquals(StatusCode.CHECKSUM_FAILED, resp.getHeader().getCode());
        localService.shutdown();
    }

    private WriteOp getWriteOp(String name, SettableFeature disabledFeature, Long checksum) {
        return new WriteOp(name,
            ByteBuffer.wrap("test".getBytes()),
            new NullStatsLogger(),
            new NullStatsLogger(),
            new IdentityStreamPartitionConverter(),
            new ServerConfiguration(),
            (byte) 0,
            checksum,
            false,
            disabledFeature,
            DefaultAccessControlManager.INSTANCE);
    }

    @Test(timeout = 60000)
    public void testStreamOpBadChecksumWithChecksumDisabled() throws Exception {
        String streamName = testName.getMethodName();

        SettableFeature disabledFeature = new SettableFeature("", 0);

        WriteOp writeOp0 = getWriteOp(streamName, disabledFeature, 919191L);
        WriteOp writeOp1 = getWriteOp(streamName, disabledFeature, 919191L);

        try {
            writeOp0.preExecute();
            fail("should have thrown");
        } catch (Exception ex) {
        }

        disabledFeature.set(1);
        writeOp1.preExecute();
    }

    @Test(timeout = 60000)
    public void testStreamOpGoodChecksumWithChecksumDisabled() throws Exception {
        String streamName = testName.getMethodName();

        SettableFeature disabledFeature = new SettableFeature("", 1);
        WriteOp writeOp0 = getWriteOp(
            streamName,
            disabledFeature,
            ProtocolUtils.writeOpCRC32(streamName, "test".getBytes()));
        WriteOp writeOp1 = getWriteOp(
            streamName,
            disabledFeature,
            ProtocolUtils.writeOpCRC32(streamName, "test".getBytes()));

        writeOp0.preExecute();
        disabledFeature.set(0);
        writeOp1.preExecute();
    }

    @Test(timeout = 60000)
    public void testCloseStreamsShouldFlush() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setOutputBufferSize(Integer.MAX_VALUE)
                .setImmediateFlushEnabled(false)
                .setPeriodicFlushFrequencyMilliSeconds(0);

        String streamNamePrefix = testName.getMethodName();
        DistributedLogServiceImpl localService = createService(serverConf, confLocal);
        StreamManagerImpl streamManager = (StreamManagerImpl) localService.getStreamManager();

        int numStreams = 10;
        int numWrites = 10;
        List<Future<WriteResponse>> futureList =
                Lists.newArrayListWithExpectedSize(numStreams * numWrites);
        for (int i = 0; i < numStreams; i++) {
            String streamName = streamNamePrefix + "-" + i;
            HeartbeatOptions hbOptions = new HeartbeatOptions();
            hbOptions.setSendHeartBeatToReader(true);
            // make sure the first log segment of each stream created
            Await.result(localService.heartbeatWithOptions(streamName, new WriteContext(), hbOptions));
            for (int j = 0; j < numWrites; j++) {
                futureList.add(localService.write(streamName, createRecord(i * numWrites + j)));
            }
        }

        assertEquals("There should be " + numStreams + " streams in cache",
                numStreams, streamManager.getCachedStreams().size());
        while (streamManager.getAcquiredStreams().size() < numStreams) {
            TimeUnit.MILLISECONDS.sleep(20);
        }

        Future<List<Void>> closeResult = localService.closeStreams();
        List<Void> closedStreams = Await.result(closeResult);
        assertEquals("There should be " + numStreams + " streams closed",
                numStreams, closedStreams.size());
        // all writes should be flushed
        for (Future<WriteResponse> future : futureList) {
            WriteResponse response = Await.result(future);
            assertTrue("Op should succeed or be rejected : " + response.getHeader().getCode(),
                    StatusCode.SUCCESS == response.getHeader().getCode()
                        || StatusCode.WRITE_EXCEPTION == response.getHeader().getCode()
                        || StatusCode.STREAM_UNAVAILABLE == response.getHeader().getCode());
        }
        assertTrue("There should be no streams in the cache",
                streamManager.getCachedStreams().isEmpty());
        assertTrue("There should be no streams in the acquired cache",
                streamManager.getAcquiredStreams().isEmpty());

        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testCloseStreamsShouldAbort() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setOutputBufferSize(Integer.MAX_VALUE)
                .setImmediateFlushEnabled(false)
                .setPeriodicFlushFrequencyMilliSeconds(0);

        String streamNamePrefix = testName.getMethodName();
        DistributedLogServiceImpl localService = createService(serverConf, confLocal);
        StreamManagerImpl streamManager = (StreamManagerImpl) localService.getStreamManager();

        int numStreams = 10;
        int numWrites = 10;
        List<Future<WriteResponse>> futureList =
                Lists.newArrayListWithExpectedSize(numStreams * numWrites);
        for (int i = 0; i < numStreams; i++) {
            String streamName = streamNamePrefix + "-" + i;
            HeartbeatOptions hbOptions = new HeartbeatOptions();
            hbOptions.setSendHeartBeatToReader(true);
            // make sure the first log segment of each stream created
            Await.result(localService.heartbeatWithOptions(streamName, new WriteContext(), hbOptions));
            for (int j = 0; j < numWrites; j++) {
                futureList.add(localService.write(streamName, createRecord(i * numWrites + j)));
            }
        }

        assertEquals("There should be " + numStreams + " streams in cache",
                numStreams, streamManager.getCachedStreams().size());
        while (streamManager.getAcquiredStreams().size() < numStreams) {
            TimeUnit.MILLISECONDS.sleep(20);
        }

        for (Stream s : streamManager.getAcquiredStreams().values()) {
            StreamImpl stream = (StreamImpl) s;
            stream.setStatus(StreamStatus.ERROR);
        }

        Future<List<Void>> closeResult = localService.closeStreams();
        List<Void> closedStreams = Await.result(closeResult);
        assertEquals("There should be " + numStreams + " streams closed",
                numStreams, closedStreams.size());
        // all writes should be flushed
        for (Future<WriteResponse> future : futureList) {
            WriteResponse response = Await.result(future);
            assertTrue("Op should fail with " + StatusCode.BK_TRANSMIT_ERROR + " or be rejected : "
                    + response.getHeader().getCode(),
                    StatusCode.BK_TRANSMIT_ERROR == response.getHeader().getCode()
                        || StatusCode.WRITE_EXCEPTION == response.getHeader().getCode()
                        || StatusCode.WRITE_CANCELLED_EXCEPTION == response.getHeader().getCode());
        }
        // acquired streams should all been removed after we close them
        assertTrue("There should be no streams in the acquired cache",
            streamManager.getAcquiredStreams().isEmpty());
        localService.shutdown();
        // cached streams wouldn't be removed immediately after streams are closed
        // but they should be removed after we shutdown the service
        assertTrue("There should be no streams in the cache after shutting down the service",
            streamManager.getCachedStreams().isEmpty());
    }

    @Test(timeout = 60000)
    public void testShutdown() throws Exception {
        service.shutdown();
        StreamManagerImpl streamManager = (StreamManagerImpl) service.getStreamManager();
        WriteResponse response =
                Await.result(service.write(testName.getMethodName(), createRecord(0L)));
        assertEquals("Write should fail with " + StatusCode.SERVICE_UNAVAILABLE,
                StatusCode.SERVICE_UNAVAILABLE, response.getHeader().getCode());
        assertTrue("There should be no streams created after shutdown",
                streamManager.getCachedStreams().isEmpty());
        assertTrue("There should be no streams acquired after shutdown",
                streamManager.getAcquiredStreams().isEmpty());
    }

    @Test(timeout = 60000)
    public void testGetOwner() throws Exception {
        ((LocalRoutingService) service.getRoutingService())
                .addHost("stream-0", service.getServiceAddress().getSocketAddress())
                .setAllowRetrySameHost(false);

        service.startPlacementPolicy();

        WriteResponse response = Await.result(service.getOwner("stream-1", new WriteContext()));
        assertEquals(StatusCode.FOUND, response.getHeader().getCode());
        assertEquals(service.getServiceAddress().toString(),
                response.getHeader().getLocation());

        // service cache "stream-2"
        StreamImpl stream = (StreamImpl) service.getStreamManager().getOrCreateStream("stream-2", false);
        // create write ops to stream-2 to make service acquire the stream
        WriteOp op = createWriteOp(service, "stream-2", 0L);
        stream.submit(op);
        stream.start();
        WriteResponse wr = Await.result(op.result());
        assertEquals("Op should succeed",
                StatusCode.SUCCESS, wr.getHeader().getCode());
        assertEquals("Service should acquire stream",
                StreamStatus.INITIALIZED, stream.getStatus());
        assertNotNull(stream.getManager());
        assertNotNull(stream.getWriter());
        assertNull(stream.getLastException());

        // the stream is acquired
        response = Await.result(service.getOwner("stream-2", new WriteContext()));
        assertEquals(StatusCode.FOUND, response.getHeader().getCode());
        assertEquals(service.getServiceAddress().toString(),
                response.getHeader().getLocation());
    }

}
