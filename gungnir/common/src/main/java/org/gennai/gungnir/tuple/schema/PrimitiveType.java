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

package org.gennai.gungnir.tuple.schema;

import static org.gennai.gungnir.GungnirConst.*;

import java.lang.reflect.Type;

public final class PrimitiveType implements FieldType {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private enum EnumType {
    STRING(String.class), TINYINT(Byte.class), SMALLINT(Short.class), INT(Integer.class), BIGINT(
        Long.class), FLOAT(Float.class), DOUBLE(Double.class), BOOLEAN(Boolean.class);

    private Type javaClass;

    EnumType(Type javaClass) {
      this.javaClass = javaClass;
    }
  }

  public static final PrimitiveType STRING = new PrimitiveType(EnumType.STRING);
  public static final PrimitiveType TINYINT = new PrimitiveType(EnumType.TINYINT);
  public static final PrimitiveType SMALLINT = new PrimitiveType(EnumType.SMALLINT);
  public static final PrimitiveType INT = new PrimitiveType(EnumType.INT);
  public static final PrimitiveType BIGINT = new PrimitiveType(EnumType.BIGINT);
  public static final PrimitiveType FLOAT = new PrimitiveType(EnumType.FLOAT);
  public static final PrimitiveType DOUBLE = new PrimitiveType(EnumType.DOUBLE);
  public static final PrimitiveType BOOLEAN = new PrimitiveType(EnumType.BOOLEAN);

  private EnumType type;

  private PrimitiveType(EnumType type) {
    this.type = type;
  }

  public Type getJavaType() {
    return type.javaClass;
  }

  @Override
  public String getName() {
    return type.toString();
  }

  @Override
  public boolean isInstance(Object obj) {
    if (obj.getClass() != type.javaClass) {
      return false;
    }
    return true;
  }

  public static PrimitiveType valueOf(String typeName) {
    return new PrimitiveType(EnumType.valueOf(typeName));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PrimitiveType other = (PrimitiveType) obj;
    if (type != other.type) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return type.toString();
  }
}
