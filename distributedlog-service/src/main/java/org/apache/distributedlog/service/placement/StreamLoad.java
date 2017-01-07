/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.service.placement;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;

/**
 * An object represent the load of a stream.
 *
 * <p>A comparable data object containing the identifier of the stream and the appraised load produced
 * by the stream.
 */
public class StreamLoad implements Comparable {
    private static final int BUFFER_SIZE = 4096;
    public final String stream;
    private final int load;

    public StreamLoad(String stream, int load) {
        this.stream = stream;
        this.load = load;
    }

    public int getLoad() {
        return load;
    }

    public String getStream() {
        return stream;
    }

    protected org.apache.distributedlog.service.placement.thrift.StreamLoad toThrift() {
        org.apache.distributedlog.service.placement.thrift.StreamLoad tStreamLoad =
            new org.apache.distributedlog.service.placement.thrift.StreamLoad();
        return tStreamLoad.setStream(stream).setLoad(load);
    }

    public byte[] serialize() throws IOException {
        TMemoryBuffer transport = new TMemoryBuffer(BUFFER_SIZE);
        TJSONProtocol protocol = new TJSONProtocol(transport);
        try {
            toThrift().write(protocol);
            transport.flush();
            return transport.toString(UTF_8.name()).getBytes(UTF_8);
        } catch (TException e) {
            throw new IOException("Failed to serialize stream load : ", e);
        } catch (UnsupportedEncodingException uee) {
            throw new IOException("Failed to serialize stream load : ", uee);
        }
    }

    public static StreamLoad deserialize(byte[] data) throws IOException {
        org.apache.distributedlog.service.placement.thrift.StreamLoad tStreamLoad =
            new org.apache.distributedlog.service.placement.thrift.StreamLoad();
        TMemoryInputTransport transport = new TMemoryInputTransport(data);
        TJSONProtocol protocol = new TJSONProtocol(transport);
        try {
            tStreamLoad.read(protocol);
            return new StreamLoad(tStreamLoad.getStream(), tStreamLoad.getLoad());
        } catch (TException e) {
            throw new IOException("Failed to deserialize stream load : ", e);
        }
    }

    @Override
    public int compareTo(Object o) {
        StreamLoad other = (StreamLoad) o;
        if (load == other.getLoad()) {
            return stream.compareTo(other.getStream());
        } else {
            return Long.compare(load, other.getLoad());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StreamLoad)) {
            return false;
        }
        StreamLoad other = (StreamLoad) o;
        return stream.equals(other.getStream()) && load == other.getLoad();
    }

    @Override
    public String toString() {
        return String.format("StreamLoad<Stream: %s, Load: %d>", stream, load);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(stream).append(load).build();
    }
}
