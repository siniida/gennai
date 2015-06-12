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

package org.gennai.gungnir.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public final class ConcurrentKryo {

  private static class KryoThreadLocal extends ThreadLocal<Kryo> {

    @Override
    protected Kryo initialValue() {
      return new Kryo();
    }
  }

  private static class OutputThreadLocal extends ThreadLocal<Output> {

    @Override
    protected Output initialValue() {
      return new Output(2048, 10240 * 1024);
    }
  }

  private static class InputThreadLocal extends ThreadLocal<Input> {

    @Override
    protected Input initialValue() {
      return new Input();
    }
  }

  private static final KryoThreadLocal KRYO_THREAD_LOCAL = new KryoThreadLocal();
  private static final OutputThreadLocal OUTPUT_THREAD_LOCAL = new OutputThreadLocal();
  private static final InputThreadLocal INPUT_THREAD_LOCAL = new InputThreadLocal();

  private ConcurrentKryo() {
  }

  public static byte[] serialize(Object object) {
    Output output = OUTPUT_THREAD_LOCAL.get();
    output.clear();
    KRYO_THREAD_LOCAL.get().writeObject(output, object);
    return output.toBytes();
  }

  public static <T> T deserialize(byte[] bytes, Class<T> type) {
    Input input = INPUT_THREAD_LOCAL.get();
    input.setBuffer(bytes);
    return KRYO_THREAD_LOCAL.get().readObject(input, type);
  }
}
