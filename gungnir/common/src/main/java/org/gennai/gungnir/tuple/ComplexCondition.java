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

public class ComplexCondition extends BaseCondition {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private List<Condition> conditions;

  public ComplexCondition(ConditionType type, Condition... condition) {
    setType(type);
    conditions = Lists.newArrayList(condition);
  }

  private ComplexCondition(ComplexCondition c) {
    super(c);
    this.conditions = Lists.newArrayList();
    for (Condition cond : c.conditions) {
      conditions.add(cond.clone());
    }
  }

  public List<Condition> getConditions() {
    return conditions;
  }

  void addCondition(Condition condition) {
    conditions.add(condition);
  }

  public ComplexCondition and(Condition... conditions) {
    return new ComplexCondition(ConditionType.AND, this,
        new ComplexCondition(ConditionType.AND, conditions));
  }

  @Override
  public ComplexCondition clone() {
    return new ComplexCondition(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((conditions == null) ? 0 : conditions.hashCode());
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
    ComplexCondition other = (ComplexCondition) obj;
    if (conditions == null) {
      if (other.conditions != null) {
        return false;
      }
    } else if (!conditions.equals(other.conditions)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Condition condition : conditions) {
      if (sb.length() > 1) {
        sb.append(" " + getType().getDisplayString() + " ");
      }
      if (getType() == ConditionType.NOT) {
        sb.append(getType().getDisplayString() + " ");
      }
      if (condition.isComplex()) {
        sb.append('(');
      }
      sb.append(condition);
      if (condition.isComplex()) {
        sb.append(')');
      }
    }
    return sb.toString();
  }
}
