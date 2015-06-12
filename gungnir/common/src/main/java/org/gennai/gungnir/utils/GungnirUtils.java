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

import static org.gennai.gungnir.tuple.schema.TupleSchema.FieldTypes.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public final class GungnirUtils {

  private static final Logger LOG = LoggerFactory.getLogger(GungnirUtils.class);

  private static ThreadLocal<Map<String, SimpleDateFormat>> DATE_FORMAT_MAP_THREAD_LOCAL =
      new ThreadLocal<Map<String, SimpleDateFormat>>() {
        @Override
        protected Map<String, SimpleDateFormat> initialValue() {
          return Maps.newHashMap();
        }
      };

  private GungnirUtils() {
  }

  public static Byte toTinyint(Object value) throws TypeCastException {
    try {
      if (value instanceof String) {
        return Byte.parseByte((String) value);
      } else if (value instanceof Number) {
        return ((Number) value).byteValue();
      } else if (value instanceof Boolean) {
        return ((Boolean) value) ? (byte) 1 : (byte) 0;
      } else if (value instanceof Date) {
        return (byte) TimeUnit.MILLISECONDS.toSeconds(((Date) value).getTime());
      }
    } catch (NumberFormatException e) {
      throw new TypeCastException(value, TINYINT.toString());
    }
    throw new TypeCastException(value, TINYINT.toString());
  }

  public static Short toSmallint(Object value) throws TypeCastException {
    try {
      if (value instanceof String) {
        return Short.parseShort((String) value);
      } else if (value instanceof Number) {
        return ((Number) value).shortValue();
      } else if (value instanceof Boolean) {
        return ((Boolean) value) ? (short) 1 : (short) 0;
      } else if (value instanceof Date) {
        return (short) TimeUnit.MILLISECONDS.toSeconds(((Date) value).getTime());
      }
    } catch (NumberFormatException e) {
      throw new TypeCastException(value, SMALLINT.toString());
    }
    throw new TypeCastException(value, SMALLINT.toString());
  }

  public static Integer toInt(Object value) throws TypeCastException {
    try {
      if (value instanceof String) {
        return Integer.parseInt((String) value);
      } else if (value instanceof Number) {
        return ((Number) value).intValue();
      } else if (value instanceof Boolean) {
        return ((Boolean) value) ? (int) 1 : (int) 0;
      } else if (value instanceof Date) {
        return (int) TimeUnit.MILLISECONDS.toSeconds(((Date) value).getTime());
      }
    } catch (NumberFormatException e) {
      throw new TypeCastException(value, INT.toString());
    }
    throw new TypeCastException(value, INT.toString());
  }

  public static Long toBigint(Object value) throws TypeCastException {
    try {
      if (value instanceof String) {
        return Long.parseLong((String) value);
      } else if (value instanceof Number) {
        return ((Number) value).longValue();
      } else if (value instanceof Boolean) {
        return ((Boolean) value) ? (long) 1 : (long) 0;
      } else if (value instanceof Date) {
        return (long) TimeUnit.MILLISECONDS.toSeconds(((Date) value).getTime());
      }
    } catch (NumberFormatException e) {
      throw new TypeCastException(value, BIGINT.toString());
    }
    throw new TypeCastException(value, BIGINT.toString());
  }

  public static Float toFloat(Object value) throws TypeCastException {
    try {
      if (value instanceof String) {
        return Float.parseFloat((String) value);
      } else if (value instanceof Number) {
        return ((Number) value).floatValue();
      } else if (value instanceof Boolean) {
        return ((Boolean) value) ? (float) 1 : (float) 0;
      } else if (value instanceof Date) {
        return (float) TimeUnit.MILLISECONDS.toSeconds(((Date) value).getTime());
      }
    } catch (NumberFormatException e) {
      throw new TypeCastException(value, FLOAT.toString());
    }
    throw new TypeCastException(value, FLOAT.toString());
  }

  public static Double toDouble(Object value) throws TypeCastException {
    try {
      if (value instanceof String) {
        return Double.parseDouble((String) value);
      } else if (value instanceof Number) {
        return ((Number) value).doubleValue();
      } else if (value instanceof Boolean) {
        return ((Boolean) value) ? (double) 1 : (double) 0;
      } else if (value instanceof Date) {
        return (double) TimeUnit.MILLISECONDS.toSeconds(((Date) value).getTime());
      }
    } catch (NumberFormatException e) {
      throw new TypeCastException(value, DOUBLE.toString());
    }
    throw new TypeCastException(value, DOUBLE.toString());
  }

  public static Boolean toBoolean(Object value) {
    if (value instanceof String) {
      return ((String) value).length() != 0;
    } else if (value instanceof Byte) {
      return ((Number) value).byteValue() != 0;
    } else if (value instanceof Short) {
      return ((Number) value).shortValue() != 0;
    } else if (value instanceof Integer) {
      return ((Number) value).intValue() != 0;
    } else if (value instanceof Long) {
      return ((Number) value).longValue() != 0;
    } else if (value instanceof Float) {
      return ((Number) value).floatValue() != 0;
    } else if (value instanceof Double) {
      return ((Number) value).doubleValue() != 0;
    } else if (value instanceof Boolean) {
      return ((Boolean) value);
    } else if (value instanceof Date) {
      return TimeUnit.MILLISECONDS.toSeconds(((Date) value).getTime()) != 0;
    }
    return false;
  }

  public static Date toTimestamp(Object value, String dateFormat) throws TypeCastException {
    if (dateFormat != null) {
      Map<String, SimpleDateFormat> dateFormatMap = DATE_FORMAT_MAP_THREAD_LOCAL.get();
      SimpleDateFormat sdf = dateFormatMap.get(dateFormat);
      if (sdf == null) {
        sdf = new SimpleDateFormat(dateFormat);
        dateFormatMap.put(dateFormat, sdf);
      }

      if (value instanceof String) {
        try {
          return sdf.parse((String) value);
        } catch (ParseException e) {
          throw new TypeCastException(value, TIMESTAMP(dateFormat).toString());
        }
      }

      throw new TypeCastException(value, TIMESTAMP(dateFormat).toString());
    } else {
      try {
        if (value instanceof String) {
          return new Date(TimeUnit.SECONDS.toMillis(Long.parseLong((String) value)));
        } else if (value instanceof Number) {
          return new Date(TimeUnit.SECONDS.toMillis(((Number) value).longValue()));
        } else if (value instanceof Date) {
          return (Date) value;
        }
      } catch (NumberFormatException e) {
        throw new TypeCastException(value, TIMESTAMP.toString());
      }
    }

    throw new TypeCastException(value, TIMESTAMP.toString());
  }

  public static Date toTimestamp(Object value) throws TypeCastException {
    return toTimestamp(value, null);
  }

  public static ThreadFactory createThreadFactory(String name) {
    return new ThreadFactoryBuilder().setNameFormat(name + "-%d")
        .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

          @Override
          public void uncaughtException(Thread t, Throwable e) {
            LOG.error("Uncaugh exception has occurred", e);
          }
        }).build();
  }

  public static long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  public static int currentTimeSec() {
    return (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
  }

  public static String getLocalAddress() {
    String hostAddress = null;
    try {
      hostAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      hostAddress = "127.0.0.1";
    }

    if ("127.0.0.1".equals(hostAddress)) {
      Enumeration<NetworkInterface> netInterfaces = null;
      try {
        netInterfaces = NetworkInterface.getNetworkInterfaces();
        while (netInterfaces.hasMoreElements()) {
          NetworkInterface ni = netInterfaces.nextElement();
          Enumeration<InetAddress> ips = ni.getInetAddresses();
          while (ips.hasMoreElements()) {
            InetAddress ip = ips.nextElement();
            if (ip.isSiteLocalAddress()) {
              hostAddress = ip.getHostAddress();
            }
          }
        }
      } catch (SocketException e) {
        return hostAddress;
      }
    }

    return hostAddress;
  }

  public static void addJar(Path srcJar, Path addJar, Path outputJar) throws IOException {
    JarInputStream jis = null;
    JarOutputStream jos = null;
    try {
      Set<String> entries = Sets.newHashSet();
      jis = new JarInputStream(new BufferedInputStream(Files.newInputStream(srcJar,
          StandardOpenOption.READ)));

      jos = new JarOutputStream(new BufferedOutputStream(Files.newOutputStream(outputJar,
          StandardOpenOption.CREATE)));

      byte[] bytes = new byte[8192];
      ZipEntry entry;
      while ((entry = jis.getNextEntry()) != null) {
        entries.add(entry.getName());

        jos.putNextEntry(entry);
        int sz;
        while ((sz = jis.read(bytes, 0, 8192)) != -1) {
          jos.write(bytes, 0, sz);
        }
        jos.closeEntry();
      }
      jis.close();
      jis = null;

      jis = new JarInputStream(new BufferedInputStream(Files.newInputStream(addJar,
          StandardOpenOption.READ)));

      while ((entry = jis.getNextEntry()) != null) {
        if (!entry.getName().startsWith("META-INF") && !entry.getName().endsWith("LICENSE")
            && !entry.getName().endsWith("NOTICE") && !entry.getName().endsWith(".md")) {
          if (entries.contains(entry.getName())) {
            if (!entry.getName().endsWith("/")) {
              jos.close();
              jos = null;
              Files.delete(outputJar);
              throw new IOException(entry.getName() + " already exists");
            }
          } else {
            jos.putNextEntry(entry);
            int sz;
            while ((sz = jis.read(bytes, 0, 8192)) != -1) {
              jos.write(bytes, 0, sz);
            }
            jos.closeEntry();
          }
        }
      }
    } finally {
      if (jis != null) {
        jis.close();
      }
      if (jos != null) {
        jos.close();
      }
    }
  }

  public static ClassLoader addToClassPath(ClassLoader cloader, List<Path> addPaths) {
    URLClassLoader loader = (URLClassLoader) cloader;
    List<URL> newPaths = Lists.newArrayList(loader.getURLs());

    for (Path addPath : addPaths) {
      try {
        URL url = addPath.toUri().toURL();
        newPaths.add(url);
      } catch (MalformedURLException e) {
        LOG.error("Failed to add to classpath {}", addPath, e);
      }
    }

    return new URLClassLoader(newPaths.toArray(new URL[0]), loader);
  }
}
