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
package com.twitter.distributedlog.service.placement;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import com.twitter.distributedlog.service.placement.thrift.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;

import static com.google.common.base.Charsets.UTF_8;

/**
 * A comparable data object containing the identifier of the server, total appraised load on the
 * server, and all streams assigned to the server by the resource placement mapping. This is
 * comparable first by load and then by server so that a sorted data structure of these will be
 * consistent across multiple calculations.
 */
public class ServerLoad implements Comparable {
  private static final int BUFFER_SIZE = 4096000;
  private final String server;
  private final HashSet<StreamLoad> streamLoads = new HashSet<StreamLoad>();
  private long load = 0l;

  public ServerLoad(String server) {
    this.server = server;
  }

  synchronized public long addStream(StreamLoad stream) {
    this.load += stream.getLoad();
    streamLoads.add(stream);
    return this.load;
  }

  synchronized public long removeStream(String stream) {
    for (StreamLoad streamLoad : streamLoads) {
      if (streamLoad.stream.equals(stream)) {
        this.load -= streamLoad.getLoad();
        streamLoads.remove(streamLoad);
        return this.load;
      }
    }
    return this.load; //Throwing an exception wouldn't help us as our logic should never reach here
  }

  public synchronized long getLoad() {
    return load;
  }

  public synchronized Set<StreamLoad> getStreamLoads() {
    return streamLoads;
  }

  public synchronized String getServer() {
    return server;
  }

  protected synchronized com.twitter.distributedlog.service.placement.thrift.ServerLoad toThrift() {
    com.twitter.distributedlog.service.placement.thrift.ServerLoad tServerLoad
        = new com.twitter.distributedlog.service.placement.thrift.ServerLoad();
    tServerLoad.setServer(server);
    tServerLoad.setLoad(load);
    ArrayList<com.twitter.distributedlog.service.placement.thrift.StreamLoad> tStreamLoads
        = new ArrayList<com.twitter.distributedlog.service.placement.thrift.StreamLoad>();
    for (StreamLoad streamLoad: streamLoads) {
      tStreamLoads.add(streamLoad.toThrift());
    }
    tServerLoad.setStreams(tStreamLoads);
    return tServerLoad;
  }

  public byte[] serialize() throws IOException {
    TMemoryBuffer transport = new TMemoryBuffer(BUFFER_SIZE);
    TJSONProtocol protocol = new TJSONProtocol(transport);
    try {
      toThrift().write(protocol);
      transport.flush();
      return transport.toString(UTF_8.name()).getBytes(UTF_8);
    } catch (TException e) {
      throw new IOException("Failed to serialize server load : ", e);
    } catch (UnsupportedEncodingException uee) {
      throw new IOException("Failed to serialize server load : ", uee);
    }
  }

  public static ServerLoad deserialize(byte[] data) throws IOException {
    com.twitter.distributedlog.service.placement.thrift.ServerLoad tServerLoad
        = new com.twitter.distributedlog.service.placement.thrift.ServerLoad();
    TMemoryInputTransport transport = new TMemoryInputTransport(data);
    TJSONProtocol protocol = new TJSONProtocol(transport);
    try {
      tServerLoad.read(protocol);
      ServerLoad serverLoad = new ServerLoad(tServerLoad.getServer());
      if (tServerLoad.isSetStreams()) {
        for (com.twitter.distributedlog.service.placement.thrift.StreamLoad tStreamLoad : tServerLoad.getStreams()) {
          serverLoad.addStream(new StreamLoad(tStreamLoad.getStream(), tStreamLoad.getLoad()));
        }
      }
      return serverLoad;
    } catch (TException e) {
      throw new IOException("Failed to deserialize server load : ", e);
    }
  }

  @Override
  public synchronized int compareTo(Object o) {
    ServerLoad other = (ServerLoad) o;
    if (load == other.getLoad()) {
      return server.compareTo(other.getServer());
    } else {
      return Long.compare(load, other.getLoad());
    }
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (!(o instanceof ServerLoad)) {
      return false;
    }
    ServerLoad other = (ServerLoad) o;
    return server.equals(other.getServer()) && load == other.getLoad() && streamLoads.equals(other.getStreamLoads());
  }

  @Override
  public synchronized String toString() {
    return String.format("ServerLoad<Server: %s, Load: %d, Streams: %s>", server, load, streamLoads);
  }

  @Override
  public synchronized int hashCode() {
    return new HashCodeBuilder().append(server).append(load).append(streamLoads).build();
  }
}
