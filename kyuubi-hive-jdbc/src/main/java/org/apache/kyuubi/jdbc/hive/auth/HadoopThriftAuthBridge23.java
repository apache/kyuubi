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

package org.apache.kyuubi.jdbc.hive.auth;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer;

public class HadoopThriftAuthBridge23 extends HadoopThriftAuthBridge {
  private static Field SASL_PROPS_FIELD = null;
  private static Class<?> SASL_PROPERTIES_RESOLVER_CLASS = null;
  private static Method RES_GET_INSTANCE_METHOD;
  private static Method GET_DEFAULT_PROP_METHOD;

  protected HadoopThriftAuthBridge23() {}

  public Map<String, String> getHadoopSaslProperties(Configuration conf) {
    if (SASL_PROPS_FIELD != null) {
      SaslRpcServer.init(conf);

      try {
        return (Map) SASL_PROPS_FIELD.get((Object) null);
      } catch (Exception var3) {
        throw new IllegalStateException("Error finding hadoop SASL properties", var3);
      }
    } else {
      try {
        Configurable saslPropertiesResolver =
            (Configurable) RES_GET_INSTANCE_METHOD.invoke((Object) null, conf);
        saslPropertiesResolver.setConf(conf);
        return (Map) GET_DEFAULT_PROP_METHOD.invoke(saslPropertiesResolver);
      } catch (Exception var4) {
        throw new IllegalStateException("Error finding hadoop SASL properties", var4);
      }
    }
  }

  static {
    String var0 = "org.apache.hadoop.security.SaslPropertiesResolver";

    try {
      SASL_PROPERTIES_RESOLVER_CLASS =
          Class.forName("org.apache.hadoop.security.SaslPropertiesResolver");
    } catch (ClassNotFoundException var4) {
    }

    if (SASL_PROPERTIES_RESOLVER_CLASS != null) {
      try {
        RES_GET_INSTANCE_METHOD =
            SASL_PROPERTIES_RESOLVER_CLASS.getMethod("getInstance", Configuration.class);
        GET_DEFAULT_PROP_METHOD = SASL_PROPERTIES_RESOLVER_CLASS.getMethod("getDefaultProperties");
      } catch (Exception var3) {
      }
    }

    if (SASL_PROPERTIES_RESOLVER_CLASS == null || GET_DEFAULT_PROP_METHOD == null) {
      try {
        SASL_PROPS_FIELD = SaslRpcServer.class.getField("SASL_PROPS");
      } catch (NoSuchFieldException var2) {
        throw new IllegalStateException(
            "Error finding hadoop SASL_PROPS field in " + SaslRpcServer.class.getSimpleName(),
            var2);
      }
    }
  }
}
