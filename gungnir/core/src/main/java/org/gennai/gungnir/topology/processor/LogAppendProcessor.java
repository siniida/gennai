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

import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.log.DebugLogger;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.json.StructSerializer;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import com.google.common.collect.Maps;

public class LogAppendProcessor implements EmitProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private transient Map<String, List<String>> outputFieldNames;
  private transient DebugLogger logger;
  private transient Marker marker;
  private transient ObjectMapper mapper;

  @Override
  public void open(GungnirConfig config, GungnirContext context, OperatorContext operatorContext,
      Map<String, List<String>> outputFieldNames) throws ProcessorException {
    this.outputFieldNames = outputFieldNames;

    logger = context.getDebugLogger(config);
    marker = MarkerFactory.getMarker(context.getTopologyName() + " " + operatorContext.getName()
        + "_" + operatorContext.getId());

    SimpleModule module = new SimpleModule("GungnirModule",
        new Version(GUNGNIR_VERSION[0], GUNGNIR_VERSION[1], GUNGNIR_VERSION[2], null));
    module.addSerializer(Struct.class, new StructSerializer());

    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
    mapper.registerModule(module);
  }

  @Override
  public void write(List<TupleValues> tuples) throws ProcessorException {
    for (TupleValues tupleValues : tuples) {
      List<String> fieldNames = outputFieldNames.get(tupleValues.getTupleName());
      if (fieldNames.size() > 0) {
        Map<String, Object> record = Maps.newLinkedHashMap();
        for (int i = 0; i < fieldNames.size(); i++) {
          record.put(fieldNames.get(i), tupleValues.getValues().get(i));
        }

        try {
          logger.logging(marker, mapper.writeValueAsString(record));
        } catch (JsonGenerationException e) {
          throw new ProcessorException("Failed to convert json format", e);
        } catch (JsonMappingException e) {
          throw new ProcessorException("Failed to convert json format", e);
        } catch (IOException e) {
          throw new ProcessorException("Failed to convert json format", e);
        }
      }
    }
  }

  @Override
  public void close() {
  }
}
