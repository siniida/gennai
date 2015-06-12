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

package org.gennai.gungnir.console;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.console.history.FileHistory;
import kafka.javaapi.consumer.ConsumerConnector;

import org.codehaus.jackson.map.ObjectMapper;
import org.gennai.gungnir.client.GungnirClientException;
import org.gennai.gungnir.console.Console.CommandHandler;
import org.gennai.gungnir.thrift.GungnirServerException;
import org.gennai.gungnir.utils.kafka.ConsumerExecutor;
import org.gennai.gungnir.utils.kafka.InvalidConfigException;
import org.gennai.gungnir.utils.kafka.KafkaClientBuilder;
import org.gennai.gungnir.utils.kafka.KafkaClientBuilder.OffsetRequest;

@Deprecated
public class MonitorCommandHandler implements CommandHandler {

  private static final Pattern MONITOR_COMMAND_PATTERN = Pattern
      .compile("^MONITOR\\s+([0-9,a-f,A-F]{24})$", Pattern.CASE_INSENSITIVE);

  private ConsoleContext context;
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public void prepare(ConsoleContext context) {
    this.context = context;
  }

  @Override
  public boolean isMatch(String command) {
    return command.toUpperCase().startsWith("MONITOR");
  }

  private void connectMonitor(String topologyId, String statementId)
      throws IOException {
    List<String> zkServers = context.getConfig().getList(KAFKA_MONITOR_ZOOKEEPER_SERVERS);

    ConsumerExecutor executor = null;
    try {
      ConsumerConnector consumerConnector =
          KafkaClientBuilder
              .createConsumer()
              .groupId(CONSOLE_GROUP + statementId)
              .offset(OffsetRequest.SMALLEST)
              .zkServers(zkServers)
              .autoCommitInterval(
                  context.getConfig().getInteger(KAFKA_MONITOR_AUTO_COMMIT_INTERVAL))
              .zkSessionTimeout(
                  context.getConfig().getInteger(KAFKA_MONITOR_ZOOKEEPER_SESSION_TIMEOUT))
              .zkConnectionTimeout(
                  context.getConfig().getInteger(KAFKA_MONITOR_ZOOKEEPER_CONNECTION_TIMEOUT))
              .zkSyncTimeout(context.getConfig().getInteger(KAFKA_MONITOR_ZOOKEEPER_SYNC_TIMEOUT))
              .build();

      executor = new ConsumerExecutor(consumerConnector);

      executor.open(topologyId, new ConsumerExecutor.ConsumerListener() {
        @Override
        public void onMessage(byte[] bytes) {
          try {
            context.getReader().println(new String(bytes));
            context.getReader().flush();
          } catch (IOException e) {
            e.printStackTrace(System.err);
          }
        }
      });

      context.getReader().setPrompt(null);
      String line = null;
      while ((line = context.getReader().readLine()) != null) {
        context.getReader().getHistory().removeLast();
        if (":q".equals(line)) {
          break;
        }
      }

      context.getReader().println("monitor closed... please wait");
      context.getReader().flush();
    } catch (InvalidConfigException e) {
      context.getReader().println("FAILED: " + e.getMessage());
    } finally {
      if (executor != null) {
        executor.close();
      }
    }
  }

  @Override
  public void execute(String command) {
    try {
      Matcher matcher = MONITOR_COMMAND_PATTERN.matcher(command);
      if (matcher.find()) {
        String topologyName = matcher.group(1);
        try {
          String res = context.getStatement().execute("DESC TOPOLOGY " + topologyName);
          @SuppressWarnings("unchecked")
          Map<String, String> desc = (HashMap<String, String>) mapper.readValue(res, HashMap.class);
          String topologyId = desc.get("id");

          connectMonitor(topologyId, context.getStatement().statementId);
        } catch (GungnirServerException e) {
          context.getReader().println("FAILED: " + e.getMessage());
        } catch (GungnirClientException e) {
          context.getReader().println("FAILED: " + e.getMessage());
        }
      } else {
        context.getReader().println("FAILED: MONITOR command usage: MONITOR TOPOLOGY_NAME");
      }

      context.getReader().getHistory().add(command.replace('\n', ' ') + ';');
      ((FileHistory) context.getReader().getHistory()).flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
