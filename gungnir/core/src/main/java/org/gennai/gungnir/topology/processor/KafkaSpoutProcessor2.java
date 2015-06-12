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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.processor.spout.MessageId;
import org.gennai.gungnir.topology.processor.spout.TupleAndMessageId;
import org.gennai.gungnir.topology.processor.spout.kafka.BrokersReader2;
import org.gennai.gungnir.topology.processor.spout.kafka.KafkaMessageId;
import org.gennai.gungnir.topology.processor.spout.kafka.PartitionManager;
import org.gennai.gungnir.topology.processor.spout.kafka.ZkState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.DynamicPartitionConnections;
import storm.kafka.FailedFetchException;
import storm.kafka.KafkaUtils;
import storm.kafka.Partition;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.task.TopologyContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class KafkaSpoutProcessor2 implements SpoutProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutProcessor2.class);

  public static final String STATE_NODE_PATH = GUNGNIR_NODE_PATH + "/kafka_spout/state";
  public static final String FETCH_SIZE = "kafka.spout.fetch.size";
  public static final String FETCH_INTERVAL = "kafka.spout.fetch.interval";
  public static final String OFFSET_BEHIND_MAX = "kafka.spout.offset.behind.max";
  public static final String STATE_UPDATE_INTERVAL = "kafka.spout.state.update.interval";
  public static final String REPLICATION_FACTOR = "kafka.spout.topic.replication.factor";
  public static final String READ_BROKERS_RETRY_TIMES = "kafka.spout.read.brokers.retry.times";
  public static final String READ_BROKERS_RETRY_INTERVAL =
      "kafka.spout.read.brokers.retry.interval";
  public static final String PARTITION_OPERATION_RETRY_TIMES =
      "kafka.spout.partition.operation.retry.times";
  public static final String PARTITION_OPERATION_RETRY_INTERVAL =
      "kafka.spout.partition.operation.retry.interval";

  private long startOffsetTime;
  private boolean forceFromStart;
  private GungnirConfig config;
  private String topicName;
  private String topologyId;
  private int taskIndex;
  private int totalTasks;
  private ZkHosts hosts;
  private SpoutConfig spoutConfig;
  private Map<Partition, PartitionManager> managersMap;
  private int fetchInterval;
  private long refreshFreqMs;
  private BrokersReader2 brokerReader;
  private DynamicPartitionConnections connections;
  private ZkState state;
  private List<PartitionManager> cachedManagers;
  private long lastRefreshTime;
  private int curPartitionIndex;
  private long lastStateUpdateTime;

  public KafkaSpoutProcessor2() {
    this.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
  }

  public KafkaSpoutProcessor2(long startOffsetTime) {
    this.startOffsetTime = startOffsetTime;
  }

  public KafkaSpoutProcessor2(long startOffsetTime, boolean forceFromStart) {
    this.startOffsetTime = startOffsetTime;
    this.forceFromStart = forceFromStart;
  }

  public KafkaSpoutProcessor2(KafkaSpoutProcessor2 c) {
    this.startOffsetTime = c.startOffsetTime;
    this.forceFromStart = c.forceFromStart;
  }

  private void refreshManagers() {
    try {
      GlobalPartitionInformation brokersInfo = brokerReader.getCurrentBrokers();
      List<Partition> partitions =
          KafkaUtils.calculatePartitionsForTask(brokersInfo, totalTasks, taskIndex);

      Set<Partition> curPartitions = managersMap.keySet();
      Set<Partition> newPartitions = Sets.newHashSet(partitions);
      newPartitions.removeAll(curPartitions);

      Set<Partition> deletedPartitions = Sets.newHashSet(curPartitions);
      deletedPartitions.removeAll(partitions);

      for (Partition partition : deletedPartitions) {
        PartitionManager manager = managersMap.remove(partition);
        manager.commit();
        manager.close();
      }

      for (Partition partition : newPartitions) {
        PartitionManager manager =
            new PartitionManager(config, spoutConfig, state, connections, topologyId, partition);
        managersMap.put(partition, manager);
      }

      cachedManagers = Lists.newArrayList(managersMap.values());

      LOG.info("Partition managers refreshed {}", cachedManagers);
    } catch (Exception e) {
      LOG.error("Failed to refresh partition managers", e);
    }
  }

  private List<PartitionManager> getPartitionManagers() {
    if (cachedManagers == null || (System.currentTimeMillis() - lastRefreshTime) > refreshFreqMs) {
      refreshManagers();
      lastRefreshTime = System.currentTimeMillis();
    }
    return cachedManagers;
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context, String schemaName)
      throws ProcessorException {
    this.config = config;
    topicName = TRACKDATA_TOPIC + context.getAccountId() + "." + schemaName;
    topologyId = context.getTopologyId();
    TopologyContext topologyContext = context.getComponent().getTopologyContext();
    taskIndex = topologyContext.getThisTaskIndex();
    totalTasks = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();

    List<String> zkServers = config.getList(KAFKA_ZOOKEEPER_SERVERS);
    hosts = new ZkHosts(StringUtils.join(zkServers, ","));
    spoutConfig = new SpoutConfig(hosts, topicName, STATE_NODE_PATH, topologyId);
    spoutConfig.forceFromStart = forceFromStart;
    spoutConfig.startOffsetTime = startOffsetTime;
    spoutConfig.fetchSizeBytes = config.getInteger(FETCH_SIZE);
    spoutConfig.bufferSizeBytes = spoutConfig.fetchSizeBytes;
    spoutConfig.maxOffsetBehind = config.getLong(OFFSET_BEHIND_MAX);
    spoutConfig.stateUpdateIntervalMs = config.getInteger(STATE_UPDATE_INTERVAL);

    managersMap = Maps.newHashMap();
    fetchInterval = config.getInteger(FETCH_INTERVAL);
    refreshFreqMs = TimeUnit.SECONDS.toMillis(hosts.refreshFreqSecs);

    brokerReader = new BrokersReader2(config, hosts, topicName);
    try {
      brokerReader.createTopic(totalTasks);
    } catch (Exception e) {
      throw new ProcessorException("Failed to create topic", e);
    }

    connections = new DynamicPartitionConnections(spoutConfig, brokerReader);
    state = new ZkState(config);

    LOG.info("KafkaSpoutProcessor opened({})", topicName);
  }

  private void commit() {
    for (PartitionManager manager : getPartitionManagers()) {
      manager.commit();
    }
    lastStateUpdateTime = System.currentTimeMillis();
  }

  @Override
  public List<TupleAndMessageId> read() throws ProcessorException, InterruptedException {
    List<TupleAndMessageId> tupleAndMessageIds = Lists.newArrayList();

    boolean empty = true;
    while (!Thread.interrupted()) {
      List<PartitionManager> managers = getPartitionManagers();
      for (int i = 0; i < managers.size(); i++) {
        try {
          curPartitionIndex = curPartitionIndex % managers.size();

          List<TupleAndMessageId> tuples = managers.get(curPartitionIndex).next();

          if (tuples != null) {
            for (TupleAndMessageId tuple : tuples) {
              tupleAndMessageIds.add(tuple);
            }

            empty = false;
            break;
          }

          curPartitionIndex = (curPartitionIndex + 1) % managers.size();
        } catch (FailedFetchException e) {
          LOG.warn("Failed to fetch messages", e);
          refreshManagers();
        }
      }

      long now = System.currentTimeMillis();
      if ((now - lastStateUpdateTime) > spoutConfig.stateUpdateIntervalMs) {
        commit();
      }

      if (empty) {
        TimeUnit.MILLISECONDS.sleep(fetchInterval);
      } else {
        break;
      }
    }

    if (empty) {
      Thread.currentThread().interrupt();
    }

    return tupleAndMessageIds;
  }

  @Override
  public void ack(MessageId messageId) {
    KafkaMessageId kafkaMessageId = (KafkaMessageId) messageId;
    PartitionManager manager = managersMap.get(kafkaMessageId.getPartition());
    if (manager != null) {
      manager.ack(kafkaMessageId.getOffset());
    }
  }

  @Override
  public void fail(MessageId messageId) {
    KafkaMessageId kafkaMessageId = (KafkaMessageId) messageId;
    PartitionManager manager = managersMap.get(kafkaMessageId.getPartition());
    if (manager != null) {
      manager.fail(kafkaMessageId.getOffset());
    }
  }

  @Override
  public void close() {
    List<PartitionManager> managers = getPartitionManagers();
    if (managers != null) {
      for (PartitionManager manager : managers) {
        manager.commit();
        manager.close();
      }
    }
    if (brokerReader != null) {
      brokerReader.close();
    }
    if (state != null) {
      state.close();
    }

    LOG.info("KafkaSpoutProcessor closed({})", topicName);
  }

  @Override
  public KafkaSpoutProcessor2 clone() {
    return new KafkaSpoutProcessor2(this);
  }

  @Override
  public String toString() {
    return "kafka_spout()";
  }
}
