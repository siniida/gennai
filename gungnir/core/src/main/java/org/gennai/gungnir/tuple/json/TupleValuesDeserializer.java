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

package org.gennai.gungnir.tuple.json;

import static org.gennai.gungnir.tuple.schema.PrimitiveType.*;

import java.io.IOException;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer.BooleanDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer.ByteDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer.DoubleDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer.FloatDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer.IntegerDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer.LongDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer.ShortDeserializer;
import org.codehaus.jackson.map.deser.std.StringDeserializer;
import org.codehaus.jackson.map.type.TypeFactory;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.schema.FieldType;
import org.gennai.gungnir.tuple.schema.ListType;
import org.gennai.gungnir.tuple.schema.MapType;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.StructType;
import org.gennai.gungnir.tuple.schema.TimestampType;

import com.google.common.collect.Lists;

public class TupleValuesDeserializer extends JsonDeserializer<TupleValues> {

  private Schema schema;
  private volatile JsonDeserializer<?>[] deserializers;

  public TupleValuesDeserializer(Schema schema) {
    this.schema = schema;
  }

  public JsonDeserializer<?> findTypedValueDeserializer(FieldType fieldType) {
    if (fieldType == null) {
      return null;
    }

    if (fieldType.equals(STRING)) {
      return new StringDeserializer();
    } else if (fieldType.equals(TINYINT)) {
      return new ByteDeserializer(Byte.class, null);
    } else if (fieldType.equals(SMALLINT)) {
      return new ShortDeserializer(Short.class, null);
    } else if (fieldType.equals(INT)) {
      return new IntegerDeserializer(Integer.class, null);
    } else if (fieldType.equals(BIGINT)) {
      return new LongDeserializer(Long.class, null);
    } else if (fieldType.equals(FLOAT)) {
      return new FloatDeserializer(Float.class, null);
    } else if (fieldType.equals(DOUBLE)) {
      return new DoubleDeserializer(Double.class, null);
    } else if (fieldType.equals(BOOLEAN)) {
      return new BooleanDeserializer(Boolean.class, null);
    } else if (fieldType instanceof TimestampType) {
      return new TimestampDeserializer((TimestampType) fieldType);
    } else if (fieldType instanceof ListType) {
      return new ListDeserializer((ListType) fieldType, this);
    } else if (fieldType instanceof MapType) {
      return new MapDeserializer((MapType) fieldType, this);
    } else if (fieldType instanceof StructType) {
      return new StructDeserializer((StructType) fieldType, this);
    }
    return null;
  }

  @Override
  public TupleValues deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    if (deserializers == null) {
      synchronized (this) {
        if (deserializers == null) {
          JsonDeserializer<?>[] deser = new JsonDeserializer<?>[schema.getFieldCount()];
          for (int i = 0; i < schema.getFieldCount(); i++) {
            deser[i] = findTypedValueDeserializer(schema.getFieldType(i));
          }
          this.deserializers = deser;
        }
      }
    }

    JsonToken t = jp.getCurrentToken();
    if (t == JsonToken.START_OBJECT) {
      t = jp.nextToken();
    }

    Object[] values = new Object[schema.getFieldCount()];
    for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
      jp.nextToken();

      Integer index = schema.getFieldIndex(jp.getCurrentName());
      if (index == null) {
        throw ctxt.mappingException("Undefined field '{}'" + jp.getCurrentName());
      }

      JsonDeserializer<?> deserializer = null;
      if (deserializers[index] != null) {
        deserializer = deserializers[index];
      } else {
        deserializer =
            ctxt.getDeserializerProvider().findTypedValueDeserializer(ctxt.getConfig(),
                TypeFactory.unknownType(), null);
      }

      values[index] = deserializer.deserialize(jp, ctxt);
    }

    return new TupleValues(schema.getSchemaName(), Lists.newArrayList(values));
  }
}
