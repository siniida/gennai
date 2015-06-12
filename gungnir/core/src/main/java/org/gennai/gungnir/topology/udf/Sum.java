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

package org.gennai.gungnir.topology.udf;

import static org.gennai.gungnir.GungnirConst.*;

import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@BaseFunction.Description(name = "sum")
public class Sum extends BaseAggregateFunction<Number> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(Sum.class);

  private Number total;

  public Sum() {
  }

  protected Sum(Sum c) {
    super(c);
  }

  @Override
  public Sum create(Object... parameters) throws ArgumentException {
    if (parameters.length == 1) {
      setParameters(parameters);
    } else {
      throw new ArgumentException("Incorrect number of arguments");
    }
    return this;
  }

  @Override
  protected void prepare() {
    total = 0L;
  }

  static Number addition(Number t, Object value) {
    if (value != null) {
      if (value instanceof Byte) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d += (Byte) value;
          t = d;
        } else {
          Long l = ((Long) t);
          l += (Byte) value;
          t = l;
        }
      } else if (value instanceof Short) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d += (Short) value;
          t = d;
        } else {
          Long l = ((Long) t);
          l += (Short) value;
          t = l;
        }
      } else if (value instanceof Integer) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d += (Integer) value;
          t = d;
        } else {
          Long l = ((Long) t);
          l += (Integer) value;
          t = l;
        }
      } else if (value instanceof Long) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d += (Long) value;
          t = d;
        } else {
          Long l = ((Long) t);
          l += (Long) value;
          t = l;
        }
      } else if (value instanceof Float) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d += (Float) value;
          t = d;
        } else {
          Double d = ((Long) t).doubleValue();
          d += (Float) value;
          t = d;
        }
      } else if (value instanceof Double) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d += (Double) value;
          t = d;
        } else {
          Double d = ((Long) t).doubleValue();
          d += (Double) value;
          t = d;
        }
      } else {
        LOG.warn("Values ​​that can be added is only numeric types");
      }
    }
    return t;
  }

  static Number subtraction(Number t, Object value) {
    if (value != null) {
      if (value instanceof Byte) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d -= (Byte) value;
          t = d;
        } else {
          Long l = ((Long) t);
          l -= (Byte) value;
          t = l;
        }
      } else if (value instanceof Short) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d -= (Short) value;
          t = d;
        } else {
          Long l = ((Long) t);
          l -= (Short) value;
          t = l;
        }
      } else if (value instanceof Integer) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d -= (Integer) value;
          t = d;
        } else {
          Long l = ((Long) t);
          l -= (Integer) value;
          t = l;
        }
      } else if (value instanceof Long) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d -= (Long) value;
          t = d;
        } else {
          Long l = ((Long) t);
          l -= (Long) value;
          t = l;
        }
      } else if (value instanceof Float) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d -= (Float) value;
          t = d;
        } else {
          Double d = ((Long) t).doubleValue();
          d -= (Float) value;
          t = d;
        }
      } else if (value instanceof Double) {
        if (t instanceof Double) {
          Double d = ((Double) t);
          d -= (Double) value;
          t = d;
        } else {
          Double d = ((Long) t).doubleValue();
          d -= (Double) value;
          t = d;
        }
      } else {
        LOG.warn("Values ​​that can be added is only numeric types");
      }
    }
    return t;
  }

  @Override
  public Number evaluate(GungnirTuple tuple) {
    if (getParameter(0) instanceof Field) {
      total = addition(total, ((Field) getParameter(0)).getValue(tuple));
    } else {
      total = addition(total, getParameter(0));
    }
    return total;
  }

  @Override
  public Number exclude(GungnirTuple tuple) {
    if (getParameter(0) instanceof AggregateFunction<?>) {
      total = subtraction(total, ((AggregateFunction<?>) getParameter(0)).exclude(tuple));
    } else if (getParameter(0) instanceof Field) {
      total = subtraction(total, ((Field) getParameter(0)).getValue(tuple));
    } else {
      total = subtraction(total, getParameter(0));
    }
    return total;
  }

  @Override
  public void clear() {
    if (getParameter(0) instanceof AggregateFunction<?>) {
      ((AggregateFunction<?>) getParameter(0)).clear();
    }
    total = 0L;
  }

  @Override
  public Sum clone() {
    return new Sum(this);
  }
}
