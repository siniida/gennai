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

package org.gennai.gungnir.ql;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.tuple.schema.Schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class SchemaRegistry implements Cloneable {

  private List<Schema> schemas = Lists.newArrayList();
  private Map<String, Integer> schemasIndex = Maps.newHashMap();

  public SchemaRegistry() {
  }

  public SchemaRegistry(SchemaRegistry c) {
    this.schemas = Lists.newArrayList(c.schemas);
    this.schemasIndex = Maps.newHashMap(c.schemasIndex);
  }

  public void register(Schema schema) {
    schemas.add(schema);
    schemasIndex.put(schema.getSchemaName(), schemas.size() - 1);
  }

  public void registerAll(List<Schema> schemas) {
    for (Schema schema : schemas) {
      register(schema);
    }
  }

  public void register(String aliasName, Schema schema) {
    Integer index = schemasIndex.get(schema.getSchemaName());
    if (index != null) {
      schemasIndex.put(aliasName, index);
    }
  }

  public boolean exists(String schemaName) {
    return schemasIndex.containsKey(schemaName);
  }

  public Schema get(String schemaName) {
    Integer index = schemasIndex.get(schemaName);
    if (index != null) {
      return schemas.get(index);
    }
    return null;
  }

  public List<Schema> getSchemas() {
    return schemas;
  }

  public void unregister(String schemaName) {
    Integer index = schemasIndex.get(schemaName);
    if (index != null) {
      for (Iterator<Map.Entry<String, Integer>> it = schemasIndex.entrySet().iterator(); it
          .hasNext();) {
        Map.Entry<String, Integer> entry = it.next();
        if (entry.getValue().equals(index)) {
          it.remove();
        }
      }
      schemas.remove(index.intValue());
      for (Map.Entry<String, Integer> entry : schemasIndex.entrySet()) {
        if (entry.getValue() > index) {
          int i = entry.getValue() - 1;
          entry.setValue(i);
        }
      }
    }
  }

  @Override
  public SchemaRegistry clone() {
    return new SchemaRegistry(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Schema schema : schemas) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(schema.getSchemaName());
    }
    return sb.toString();
  }
}
