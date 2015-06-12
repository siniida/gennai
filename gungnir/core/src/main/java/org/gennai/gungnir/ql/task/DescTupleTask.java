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

package org.gennai.gungnir.ql.task;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.tuple.schema.FieldType;
import org.gennai.gungnir.tuple.schema.ListType;
import org.gennai.gungnir.tuple.schema.MapType;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.StructType;
import org.gennai.gungnir.tuple.schema.TimestampType;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DescTupleTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DescTupleTask.class);

  private String schemaName;
  private UserEntity owner;
  private ObjectMapper mapper;

  public DescTupleTask(String schemaName, UserEntity owner) {
    this.schemaName = schemaName;
    this.owner = owner;
    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
  }

  private ObjectNode fieldTypeToNode(ObjectMapper mapper, FieldType fieldType) {
    ObjectNode typeNode = mapper.createObjectNode();
    if (fieldType != null) {
      if (fieldType instanceof TimestampType) {
        typeNode.put("type", fieldType.getName());
        String dateFormat = ((TimestampType) fieldType).getDateFormat();
        if (dateFormat != null) {
          typeNode.put("dateFormat", dateFormat);
        }
      } else if (fieldType instanceof ListType) {
        typeNode.put("type", fieldType.getName());
        typeNode.put("element", fieldTypeToNode(mapper, ((ListType) fieldType).getElementType()));
      } else if (fieldType instanceof MapType) {
        typeNode.put("type", fieldType.getName());
        typeNode.put("key", fieldTypeToNode(mapper, ((MapType) fieldType).getKeyType()));
        typeNode.put("value", fieldTypeToNode(mapper, ((MapType) fieldType).getValueType()));
      } else if (fieldType instanceof StructType) {
        typeNode.put("type", fieldType.getName());
        StructType structType = (StructType) fieldType;
        ObjectNode fieldsNode = mapper.createObjectNode();
        for (int i = 0; i < structType.getFieldCount(); i++) {
          fieldsNode.put(structType.getFieldName(i),
              fieldTypeToNode(mapper, structType.getFieldType(i)));
        }
        typeNode.put("fields", fieldsNode);
      } else {
        typeNode.put("type", fieldType.getName());
      }
    } else {
      typeNode.put("type", "auto detect");
    }
    return typeNode;
  }

  @Override
  public String execute() throws TaskExecuteException {
    Schema schema = null;
    try {
      schema = GungnirManager.getManager().getMetaStore().findSchema(schemaName, owner);
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    }

    if (!(schema instanceof TupleSchema)) {
      throw new TaskExecuteException(schemaName + " isn't tuple");
    }

    ObjectNode schemaNode = mapper.createObjectNode();
    schemaNode.put("name", schema.getSchemaName());
    ObjectNode fieldsNode = mapper.createObjectNode();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      ObjectNode typeNode = fieldTypeToNode(mapper, schema.getFieldType(i));
      if (typeNode != null) {
        fieldsNode.put(schema.getFieldName(i), typeNode);
      }
    }
    schemaNode.put("fields", fieldsNode);
    if (schema.getPartitionFields() != null) {
      ArrayNode partitionsNode = mapper.createArrayNode();
      for (String partitionField : schema.getPartitionFields()) {
        partitionsNode.add(partitionField);
      }
      schemaNode.put("partitioned", partitionsNode);
    }
    ArrayNode topologiesNode = mapper.createArrayNode();
    for (String topologyId : schema.getTopologies()) {
      topologiesNode.add(topologyId);
    }
    schemaNode.put("topologies", topologiesNode);
    schemaNode.put("owner", schema.getOwner().getName());
    schemaNode.putPOJO("createTime", schema.getCreateTime());
    if (schema.getComment() != null) {
      schemaNode.put("comment", schema.getComment());
    }

    try {
      return mapper.writeValueAsString(schemaNode);
    } catch (Exception e) {
      LOG.error("Failed to convert json format", e);
      throw new TaskExecuteException("Failed to convert json format", e);
    }
  }
}
