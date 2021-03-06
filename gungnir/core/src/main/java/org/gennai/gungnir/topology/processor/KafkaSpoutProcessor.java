/**
 * Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
 * (see CONTRIBUTORS.md)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  A copy of the
 * License is distributed with this work in the LICENSE.md file.  You may
 * also obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gennai.gungnir.topology.processor;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.processor.spout.MessageId;
import org.gennai.gungnir.topology.processor.spout.TupleAndMessageId;
import org.gennai.gungnir.utils.ConcurrentKryo;
import org.gennai.gungnir.utils.kafka.InvalidConfigException;
import org.gennai.gungnir.utils.kafka.KafkaClientBuilder;
import org.gennai.gungnir.utils.kafka.KafkaClientBuilder.OffsetRequest;
import org.gennai.gungnir.utils.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

@Deprecated
public class KafkaSpoutProcessor implements SpoutProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutProcessor.class);

  private GungnirConfig config;
  private String topicName;
  private List<String> zkServers;
  private ConsumerConnector consumerConnector;
  private ConsumerIterator<byte[], byte[]> consumerIterator;

  private List<TopicMetadata> getTopicMetaData() throws Exception {
    CuratorFramework curator = CuratorFrameworkFactory.builder()
        .connectString(StringUtils.join(zkServers, ","))
        .sessionTimeoutMs(config.getInteger(KAFKA_ZOOKEEPER_SESSION_TIMEOUT))
        .connectionTimeoutMs(config.getInteger(KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT))
        .retryPolicy(new RetryNTimes(config.getInteger(KAFKA_ZOOKEEPER_RETRY_TIMES),
            config.getInteger(KAFKA_ZOOKEEPER_RETRY_INTERVAL))).build();

    curator.start();

    List<TopicMetadata> topicMetadata = KafkaUtils.getTopicMetaData(curator, topicName);

    curator.close();

    return topicMetadata;
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context, String schemaName)
      throws ProcessorException {
    this.config = config;

    topicName = TRACKDATA_TOPIC + context.getAccountId() + "." + schemaName;
    zkServers = config.getList(KAFKA_ZOOKEEPER_SERVERS);
    try {
      List<TopicMetadata> topicMetadata = getTopicMetaData();
      for (TopicMetadata metaData : topicMetadata) {
        for (PartitionMetadata part : metaData.partitionsMetadata()) {
          LOG.debug("topic: {}, partition: {}, leader: {}, replicas: {}, isr: {}",
              metaData.topic(),
              part.partitionId(), part.leader(), part.replicas(), part.isr());
        }
      }
    } catch (Exception e) {
      throw new ProcessorException("Failed to get topic meta data", e);
    }

    try {
      consumerConnector =
          KafkaClientBuilder.createConsumer()
              .groupId(context.getTopologyId())
              .offset(OffsetRequest.LARGEST)
              .zkServers(zkServers)
              .autoCommitInterval(config.getInteger(KAFKA_AUTO_COMMIT_INTERVAL))
              .zkSessionTimeout(config.getInteger(KAFKA_ZOOKEEPER_SESSION_TIMEOUT))
              .zkConnectionTimeout(config.getInteger(KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT))
              .zkSyncTimeout(config.getInteger(KAFKA_ZOOKEEPER_SYNC_TIMEOUT)).build();
    } catch (InvalidConfigException e) {
      throw new ProcessorException(e);
    }

    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
        .createMessageStreams(ImmutableMap.of(topicName, 1));

    KafkaStream<byte[], byte[]> stream = consumerMap.get(topicName).get(0);

    consumerIterator = stream.iterator();

    LOG.info("KafkaSpoutProcessor opened({})", this);
  }

  @Override
  public List<TupleAndMessageId> read() throws ProcessorException {
    if (consumerIterator == null) {
      throw new ProcessorException("Processor isn't open");
    }

    MessageAndMetadata<byte[], byte[]> msgAndMetadata = consumerIterator.next();
    byte[] bytes = msgAndMetadata.message();

    @SuppressWarnings("unchecked")
    List<Object> values = ConcurrentKryo.deserialize(bytes, ArrayList.class);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Read tuple values {}", values);
    }

    List<TupleAndMessageId> tuples = Lists.newArrayList();
    tuples.add(new TupleAndMessageId(values, null));

    return tuples;
  }

  @Override
  public void ack(MessageId messageId) {
  }

  @Override
  public void fail(MessageId messageId) {
  }

  @Override
  public void close() {
    if (consumerConnector != null) {
      consumerConnector.commitOffsets();
      consumerConnector.shutdown();
      LOG.info("KafkaSpoutProcessor closed({})", topicName);
    }
  }

  @Override
  public KafkaSpoutProcessor clone() {
    return new KafkaSpoutProcessor();
  }

  @Override
  public String toString() {
    return "kafka_spout()";
  }
}
