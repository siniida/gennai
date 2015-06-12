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

package org.gennai.gungnir.tuple;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Arrays;

public class SimpleCondition extends BaseCondition {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private Field field;
  private Object value;

  public SimpleCondition(ConditionType type, Field field, Object value) {
    setType(type);
    this.field = field;
    this.value = value;
  }

  private SimpleCondition(SimpleCondition c) {
    super(c);
    this.field = c.field;
    this.value = c.value;
  }

  public Field getField() {
    return field;
  }

  public void setField(Field field) {
    this.field = field;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SimpleCondition other = (SimpleCondition) obj;
    if (field == null) {
      if (other.field != null) {
        return false;
      }
    } else if (!field.equals(other.field)) {
      return false;
    }
    if (value == null) {
      if (other.value != null) {
        return false;
      }
    } else if (!value.equals(other.value)) {
      return false;
    }
    return true;
  }

  public SimpleCondition clone() {
    return new SimpleCondition(this);
  }

  @Override
  public String toString() {
    if (value == null) {
      return field + " " + getType().getDisplayString();
    } else {
      if (value.getClass().isArray()) {
        return field + " " + getType().getDisplayString() + " " + Arrays.toString((Object[]) value);
      } else {
        return field + " " + getType().getDisplayString() + " " + value;
      }
    }
  }
}
