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

@BaseFunction.Description(name = "ifnull")
public class Ifnull extends BaseFunction<Object> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public Ifnull() {
  }

  private Ifnull(Ifnull c) {
    super(c);
  }

  @Override
  public Ifnull create(Object... parameters) throws ArgumentException {
    if (parameters.length == 2) {
      setParameters(parameters);
    } else {
      throw new ArgumentException("Incorrect number of arguments");
    }
    return this;
  }

  @Override
  protected void prepare() {
  }

  @Override
  public Object evaluate(GungnirTuple tuple) {
    for (Object parameter : getParameters()) {
      if (parameter instanceof Field) {
        Object value = ((Field) parameter).getValue(tuple);
        if (value != null) {
          return value;
        }
      } else {
        return parameter;
      }
    }
    return null;
  }

  @Override
  public Ifnull clone() {
    return new Ifnull(this);
  }
}
