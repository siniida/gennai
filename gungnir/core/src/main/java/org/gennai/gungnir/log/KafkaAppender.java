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

package org.gennai.gungnir.log;

import static org.gennai.gungnir.GungnirConfig.*;

import java.util.List;

import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.utils.kafka.KafkaClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class KafkaAppender extends AppenderBase<ILoggingEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAppender.class);

  private String topicName;
  private Producer<Integer, byte[]> producer;

  public KafkaAppender() {
    GungnirConfig config = GungnirManager.getManager().getConfig();
    List<String> brokers = config.getList(KAFKA_MONITOR_BROKERS);
    int requiredAcks = config.getInteger(KAFKA_MONITOR_REQUIRED_ACKS);
    producer =
        KafkaClientBuilder.createProducer().brokers(brokers).requiredAcks(requiredAcks).build();
    LOG.info("Kafka appender created");
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    String topic = null;
    if (topicName != null) {
      topic = topicName;
    } else {
      topic = eventObject.getLoggerName();
    }

    try {
      producer.send(new KeyedMessage<Integer, byte[]>(topic, eventObject.getMessage().toString()
          .getBytes()));
    } catch (FailedToSendMessageException e) {
      LOG.error("Failed to append log '{}'", eventObject.getMessage().toString(), e);
    }
  }
}
