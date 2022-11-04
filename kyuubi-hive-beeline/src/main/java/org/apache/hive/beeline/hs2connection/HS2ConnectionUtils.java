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

package org.apache.hive.beeline.hs2connection;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.hive.beeline.KyuubiBeelineException;

public class HS2ConnectionUtils {

  /**
   * JDBC URLs have the following format:
   * jdbc:hive2://<host>:<port>/<dbName>;<sessionVars>?<kyuubiConfs>#<[spark|hive]Vars>
   */
  public static String getUrl(Properties props) throws KyuubiBeelineException {
    if (props == null || props.isEmpty()) {
      return null;
    }
    // use remove instead of get so that it is not parsed again
    // in the for loop below
    String urlPrefix = (String) props.remove(DefaultConnectionUrlParser.URL_PREFIX_PROPERTY_KEY);
    if (urlPrefix == null || urlPrefix.isEmpty()) {
      throw new KyuubiBeelineException("url_prefix parameter cannot be empty");
    }

    String hosts = (String) props.remove(DefaultConnectionUrlParser.HOST_PROPERTY_KEY);
    if (hosts == null || hosts.isEmpty()) {
      throw new KyuubiBeelineException("hosts parameter cannot be empty");
    }
    String defaultDB = (String) props.remove(DefaultConnectionUrlParser.DB_NAME_PROPERTY_KEY);
    if (defaultDB == null) {
      defaultDB = "default";
    }

    // process session var list
    String sessionConfProperties = "";
    if (props.containsKey(DefaultConnectionUrlParser.SESSION_CONF_PROPERTY_KEY)) {
      sessionConfProperties =
          extractKyuubiVariables(
              (String) props.remove(DefaultConnectionUrlParser.SESSION_CONF_PROPERTY_KEY), ";");
    }

    // process kyuubi conf list
    String kyuubiConfProperties = "";
    if (props.containsKey(DefaultConnectionUrlParser.KYUUBI_CONF_PROPERTY_KEY)) {
      kyuubiConfProperties =
          extractKyuubiVariables(
              (String) props.remove(DefaultConnectionUrlParser.KYUUBI_CONF_PROPERTY_KEY), "?");
    }

    // process var list
    String kyuubiVarProperties = "";
    if (props.containsKey(DefaultConnectionUrlParser.KYUUBI_VAR_PROPERTY_KEY)) {
      kyuubiVarProperties =
          extractKyuubiVariables(
              (String) props.remove(DefaultConnectionUrlParser.KYUUBI_VAR_PROPERTY_KEY), "#");
    }

    StringBuilder urlSb = new StringBuilder();
    urlSb.append(urlPrefix.trim());
    urlSb.append(hosts.trim());
    urlSb.append(File.separator);
    urlSb.append(defaultDB.trim());
    List<String> keys = new ArrayList<>(props.stringPropertyNames());
    // sorting the keys from the properties helps to create
    // a deterministic url which is tested for various configuration in
    // TestHS2ConnectionConfigFileManager
    Collections.sort(keys);
    for (String propertyName : keys) {
      urlSb.append(";");
      urlSb.append(propertyName);
      urlSb.append("=");
      urlSb.append(props.getProperty(propertyName));
    }
    if (!sessionConfProperties.isEmpty()) {
      if (keys.size() > 0) {
        urlSb.append(";");
      }
      urlSb.append(sessionConfProperties);
    }
    if (!kyuubiConfProperties.isEmpty()) {
      urlSb.append(kyuubiConfProperties);
    }
    if (!kyuubiVarProperties.isEmpty()) {
      urlSb.append(kyuubiVarProperties);
    }
    return urlSb.toString();
  }

  private static String extractKyuubiVariables(String propertyValue, String delimiter)
      throws KyuubiBeelineException {
    StringBuilder propertiesList = new StringBuilder();
    propertiesList.append(delimiter);
    addPropertyValues(propertyValue, propertiesList);
    return propertiesList.toString();
  }

  private static void addPropertyValues(String value, StringBuilder propertiesList)
      throws KyuubiBeelineException {
    // There could be multiple keyValuePairs separated by comma
    String[] values = value.split(",");
    boolean first = true;
    for (String keyValuePair : values) {
      String[] keyValue = keyValuePair.split("=");
      if (keyValue.length != 2) {
        throw new KyuubiBeelineException(
            "Unable to parse " + keyValuePair + " in hs2 connection config file");
      }
      if (!first) {
        propertiesList.append(";");
      }
      first = false;
      propertiesList.append(keyValue[0].trim());
      propertiesList.append("=");
      propertiesList.append(keyValue[1].trim());
    }
  }
}
