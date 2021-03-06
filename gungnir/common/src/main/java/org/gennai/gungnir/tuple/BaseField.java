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

import org.gennai.gungnir.tuple.Condition.ConditionType;

public abstract class BaseField implements Field {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public SimpleCondition eq(Object value) {
    return new SimpleCondition(ConditionType.EQ, this, value);
  }

  public SimpleCondition ne(Object value) {
    return new SimpleCondition(ConditionType.NE, this, value);
  }

  public SimpleCondition gt(Object value) {
    return new SimpleCondition(ConditionType.GT, this, value);
  }

  public SimpleCondition ge(Object value) {
    return new SimpleCondition(ConditionType.GE, this, value);
  }

  public SimpleCondition lt(Object value) {
    return new SimpleCondition(ConditionType.LT, this, value);
  }

  public SimpleCondition le(Object value) {
    return new SimpleCondition(ConditionType.LE, this, value);
  }

  public SimpleCondition like(String value) {
    return new SimpleCondition(ConditionType.LIKE, this, value);
  }

  public SimpleCondition regexp(String value) {
    return new SimpleCondition(ConditionType.REGEXP, this, value);
  }

  public SimpleCondition in(Object... value) {
    return new SimpleCondition(ConditionType.IN, this, value);
  }

  public SimpleCondition all(Object... value) {
    return new SimpleCondition(ConditionType.ALL, this, value);
  }

  public SimpleCondition between(Number from, Number to) {
    return new SimpleCondition(ConditionType.BETWEEN, this, new Object[] {from, to});
  }

  public SimpleCondition isNull() {
    return new SimpleCondition(ConditionType.IS_NULL, this, null);
  }

  public SimpleCondition isNotNull() {
    return new SimpleCondition(ConditionType.IS_NOT_NULL, this, null);
  }
}
