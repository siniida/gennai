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

import java.util.List;

import com.google.common.collect.Lists;

public abstract class BaseCondition implements Condition {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private ConditionType type;

  protected BaseCondition() {
  }

  protected BaseCondition(BaseCondition c) {
    this.type = c.type;
  }

  protected void setType(ConditionType type) {
    this.type = type;
  }

  @Override
  public ConditionType getType() {
    return type;
  }

  @Override
  public boolean isComplex() {
    if (this instanceof ComplexCondition) {
      return true;
    }
    return false;
  }

  @Override
  public ComplexCondition toComplex() {
    if (this instanceof ComplexCondition) {
      return (ComplexCondition) this;
    }
    return null;
  }

  @Override
  public SimpleCondition toSimple() {
    if (this instanceof SimpleCondition) {
      return (SimpleCondition) this;
    }
    return null;
  }

  @Override
  public List<String> getFieldNames() {
    List<String> fieldNames = Lists.newArrayList();
    if (this instanceof ComplexCondition) {
      for (Condition condition : this.toComplex().getConditions()) {
        fieldNames.addAll(condition.getFieldNames());
      }
    } else {
      fieldNames.add(this.toSimple().getField().getFieldName());
    }
    return fieldNames;
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
    Condition other = (Condition) obj;
    if (type != other.getType()) {
      return false;
    }
    return true;
  }

  @Override
  public abstract Condition clone();
}
