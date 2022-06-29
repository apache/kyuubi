/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.service.cli;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.Map.Entry;

public class StringUtils {
  public StringUtils() {}

  public static String intern(String str) {
    return str == null ? null : str.intern();
  }

  public static List<String> intern(List<String> list) {
    if (list == null) {
      return null;
    } else {
      List<String> newList = new ArrayList(list.size());
      Iterator var2 = list.iterator();

      while (var2.hasNext()) {
        String str = (String) var2.next();
        newList.add(intern(str));
      }

      return newList;
    }
  }

  public static Map<String, String> intern(Map<String, String> map) {
    if (map == null) {
      return null;
    } else if (map.isEmpty()) {
      return map;
    } else {
      Map<String, String> newMap = new HashMap(map.size());
      Iterator var2 = map.entrySet().iterator();

      while (var2.hasNext()) {
        Entry<String, String> entry = (Entry) var2.next();
        newMap.put(intern((String) entry.getKey()), intern((String) entry.getValue()));
      }

      return newMap;
    }
  }

  public static Set<String> asSet(String... elements) {
    if (elements == null) {
      return new HashSet();
    } else {
      Set<String> set = new HashSet(elements.length);
      Collections.addAll(set, elements);
      return set;
    }
  }

  public static String normalizeIdentifier(String identifier) {
    return identifier.trim().toLowerCase();
  }

  public static String stringifyException(Throwable e) {
    StringWriter stm = new StringWriter();
    PrintWriter wrt = new PrintWriter(stm);
    e.printStackTrace(wrt);
    wrt.close();
    return stm.toString();
  }

  public static String byteToHexString(byte[] bytes, int start, int end) {
    return org.apache.hadoop.util.StringUtils.byteToHexString(bytes, start, end);
  }

  public static boolean isEmpty(CharSequence cs) {
    return cs == null || cs.length() == 0;
  }
}
