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

package org.gennai.gungnir.ql.analysis;

import java.util.Map;

import org.gennai.gungnir.topology.udf.ArgumentException;
import org.gennai.gungnir.topology.udf.Average;
import org.gennai.gungnir.topology.udf.Cast;
import org.gennai.gungnir.topology.udf.CollectList;
import org.gennai.gungnir.topology.udf.CollectSet;
import org.gennai.gungnir.topology.udf.Concat;
import org.gennai.gungnir.topology.udf.Cosine;
import org.gennai.gungnir.topology.udf.Count;
import org.gennai.gungnir.topology.udf.DateFormat;
import org.gennai.gungnir.topology.udf.Distance;
import org.gennai.gungnir.topology.udf.Function;
import org.gennai.gungnir.topology.udf.Ifnull;
import org.gennai.gungnir.topology.udf.ParseUrl;
import org.gennai.gungnir.topology.udf.RegexpExtract;
import org.gennai.gungnir.topology.udf.Sine;
import org.gennai.gungnir.topology.udf.Size;
import org.gennai.gungnir.topology.udf.Slice;
import org.gennai.gungnir.topology.udf.Split;
import org.gennai.gungnir.topology.udf.Sqrt;
import org.gennai.gungnir.topology.udf.Sum;
import org.gennai.gungnir.topology.udf.Tangent;

import com.google.common.collect.Maps;

public class FunctionRegistry {

  private Map<String, Class<? extends Function<?>>> registerMap = Maps.newHashMap();

  public FunctionRegistry() {
    registerFunction("count", Count.class);
    registerFunction("avg", Average.class);
    registerFunction("sum", Sum.class);
    registerFunction("concat", Concat.class);
    registerFunction("ifnull", Ifnull.class);
    registerFunction("split", Split.class);
    registerFunction("regexp_extract", RegexpExtract.class);
    registerFunction("parse_url", ParseUrl.class);
    registerFunction("cast", Cast.class);
    registerFunction("date_format", DateFormat.class);
    registerFunction("distance", Distance.class);
    registerFunction("sqrt", Sqrt.class);
    registerFunction("sin", Sine.class);
    registerFunction("cos", Cosine.class);
    registerFunction("tan", Tangent.class);
    registerFunction("size", Size.class);
    registerFunction("slice", Slice.class);
    registerFunction("collect_list", CollectList.class);
    registerFunction("collect_set", CollectSet.class);
  }

  public void registerFunction(String name, Class<? extends Function<?>> registerClass) {
    registerMap.put(name, registerClass);
  }

  public Function<?> create(String name, Object... parameters)
      throws RegisterException, ArgumentException {
    Class<? extends Function<?>> registerClass = registerMap.get(name);
    if (registerClass != null) {
      try {
        if (parameters != null) {
          return registerClass.newInstance().create(parameters);
        } else {
          return registerClass.newInstance().create();
        }
      } catch (InstantiationException e) {
        throw new RegisterException("Failed to create instance '" + name + "' function", e);
      } catch (IllegalAccessException e) {
        throw new RegisterException("Failed to create instance '" + name + "' function", e);
      }
    } else {
      throw new RegisterException("Function isn't registered '" + name + "'");
    }
  }
}
