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
package com.twitter.distributedlog.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.client.DistributedLogMultiStreamWriter;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.util.FutureEventListener;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * It is a kafka producer that uses dl streams
 */
public class KafkaDistributedLogProducer<K, V> implements Producer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDistributedLogProducer.class);
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private final String clientId;
    private final Partitioner partitioner;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final Time time;
    private final DistributedLogClient client;
    // TODO: update when there are partition changes
    private final List<String> dlStreamPartitions;
    private final DistributedLogMultiStreamWriter unpartitionedWriter;
    private final ConcurrentMap<Integer, DistributedLogMultiStreamWriter> partitionedWriters;

    // Assume all streams have same partitions
    private KafkaDistributedLogProducer(ProducerConfig config,
                                        Serializer<K> keySerializer,
                                        Serializer<V> valueSerializer,
                                        DistributedLogClient client,
                                        List<String> dlStreamPartitions) {
        this.producerConfig = config;
        this.time = new SystemTime();
        // get the client id
        String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        if (StringUtils.isBlank(clientId)) {
            this.clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
        } else {
            this.clientId = clientId;
        }
        // get the partitioner
        this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
        // get the dl client
        this.client = client;
        // get the dl partitions
        this.dlStreamPartitions = dlStreamPartitions;
        // get the serializer
        if (keySerializer == null) {
            this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    Serializer.class);
            this.keySerializer.configure(config.originals(), true);
        } else {
            config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            this.keySerializer = keySerializer;
        }
        if (valueSerializer == null) {
            this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    Serializer.class);
            this.valueSerializer.configure(config.originals(), false);
        } else {
            config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
            this.valueSerializer = valueSerializer;
        }
        // get writers
        this.unpartitionedWriter = createWriter(-1);
        this.partitionedWriters = new ConcurrentHashMap<Integer, DistributedLogMultiStreamWriter>();
        for (int i = 0; i < dlStreamPartitions.size(); i++) {
            partitionedWriters.put(i, createWriter(i));
        }
        config.logUnused();
    }

    private DistributedLogMultiStreamWriter createWriter(int partition) {
        if (partition < 0) {
            return DistributedLogMultiStreamWriter.newBuilder()
                    .client(client)
                    .streams(dlStreamPartitions)
                    .build();
        }
        return DistributedLogMultiStreamWriter.newBuilder()
                .client(client)
                .streams(Lists.newArrayList(dlStreamPartitions.get(partition)))
                .build();
    }

    private DistributedLogMultiStreamWriter getWriter(int partition) {
        if (partition < 0) {
            return unpartitionedWriter;
        }
        return partitionedWriters.get(partition);
    }

    private static <E> Future<E> exception(Throwable t) {
        SettableFuture<E> future = SettableFuture.create();
        future.setException(t);
        return future;
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * calls configured partitioner class to compute the partition.
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey , byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        if (partition != null) {
            int numPartitions = dlStreamPartitions.size();
            // they have given us a partition, use it
            if (partition < 0 || partition >= numPartitions)
                throw new IllegalArgumentException("Invalid partition given with record: " + partition
                                                   + " is not in the range [0..."
                                                   + numPartitions
                                                   + "].");
            return partition;
        }
        return this.partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), serializedValue,
            cluster);
    }

    private ByteBuffer serializeKeyValue(byte[] key, byte[] value) {
        int recordSize = 0;
        int keySize;
        recordSize += (Integer.SIZE / 8);
        if (null != key) {
            keySize = key.length;
            recordSize += key.length;
        } else {
            keySize = 0;
        }
        int valueSize;
        recordSize += (Integer.SIZE / 8);
        if (null != value) {
            valueSize = value.length;
            recordSize += value.length;
        } else {
            valueSize = 0;
        }
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);
        buffer.putInt(keySize);
        if (null != key) {
            buffer.put(key);
        }
        buffer.putInt(valueSize);
        if (null != value) {
            buffer.put(value);
        }
        buffer.rewind();
        return buffer;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        return send(producerRecord, null);
    }

    @Override
    public Future<RecordMetadata> send(final ProducerRecord<K, V> record, Callback callback) {
        // serialize key and value
        byte[] serializedKey;
        try {
            serializedKey = keySerializer.serialize(record.topic(), record.key());
        } catch (ClassCastException cce) {
            return exception(new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                    " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                    " specified in key.serializer"));
        }
        byte[] serializedValue;
        try {
            serializedValue = valueSerializer.serialize(record.topic(), record.value());
        } catch (ClassCastException cce) {
            return exception(new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                    " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                    " specified in value.serializer"));
        }
        // get the partition
        // TODO: Cluster information
        final int partition = partition(record, serializedKey, serializedValue, null);

        // write the key/value pair
        ByteBuffer buffer = serializeKeyValue(serializedKey, serializedValue);
        final SettableFuture<RecordMetadata> result = SettableFuture.create();
        getWriter(partition).write(buffer).addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onSuccess(DLSN dlsn) {
                // TODO: ask write proxy to return transaction id
                result.set(new RecordMetadata(new TopicPartition(record.topic(), 0), -1L, -1L));
            }

            @Override
            public void onFailure(Throwable throwable) {
                result.setException(throwable);
            }
        });
        return result;
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        // no-op
        return Maps.newHashMap();
    }

    @Override
    public void close() {
        unpartitionedWriter.close();
        for (DistributedLogMultiStreamWriter writer : partitionedWriters.values()) {
            writer.close();
        }
        partitionedWriters.clear();
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        close();
    }
}
