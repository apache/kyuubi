/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive;

import static org.apache.kyuubi.jdbc.hive.Utils.extractURLComponents;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class UtilsTest {

  private String expectedHost;
  private String expectedPort;
  private String expectedCatalog;
  private String expectedDb;
  private Map<String, String> expectedHiveConf;
  private String uri;

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws UnsupportedEncodingException {
    return Arrays.asList(
        new Object[][] {
          {
            "localhost",
            "10009",
            null,
            "db",
            new ImmutableMap.Builder<String, String>().put("k2", "v2").build(),
            "jdbc:hive2:///db;k1=v1?k2=v2#k3=v3"
          },
          {
            "localhost",
            "10009",
            null,
            "default",
            new ImmutableMap.Builder<String, String>().build(),
            "jdbc:hive2:///"
          },
          {
            "localhost",
            "10009",
            null,
            "default",
            new ImmutableMap.Builder<String, String>().build(),
            "jdbc:kyuubi://"
          },
          {
            "localhost",
            "10009",
            null,
            "default",
            new ImmutableMap.Builder<String, String>().build(),
            "jdbc:hive2://"
          },
          {
            "hostname",
            "10018",
            null,
            "db",
            new ImmutableMap.Builder<String, String>().put("k2", "v2").build(),
            "jdbc:hive2://hostname:10018/db;k1=v1?k2=v2#k3=v3"
          },
          {
            "hostname",
            "10018",
            "catalog",
            "db",
            new ImmutableMap.Builder<String, String>().put("k2", "v2").build(),
            "jdbc:hive2://hostname:10018/catalog/db;k1=v1?k2=v2#k3=v3"
          },
          {
            "hostname",
            "10018",
            "catalog",
            "db",
            new ImmutableMap.Builder<String, String>()
                .put("k2", "v2")
                .put("k3", "-Xmx2g -XX:+PrintGCDetails -XX:HeapDumpPath=/heap.hprof")
                .build(),
            "jdbc:hive2://hostname:10018/catalog/db;k1=v1?"
                + URLEncoder.encode(
                        "k2=v2;k3=-Xmx2g -XX:+PrintGCDetails -XX:HeapDumpPath=/heap.hprof",
                        StandardCharsets.UTF_8.toString())
                    .replaceAll("\\+", "%20")
                + "#k4=v4"
          },
          {
            "hostname",
            "10018",
            "catalog",
            "db",
            new ImmutableMap.Builder<String, String>()
                .put("k2", "v2")
                .put("k3", "hostname:10018")
                .build(),
            "jdbc:hive2://hostname:10018/catalog/db;k1=v1?k2=v2;k3=hostname:10018"
          }
        });
  }

  public UtilsTest(
      String expectedHost,
      String expectedPort,
      String expectedCatalog,
      String expectedDb,
      Map<String, String> expectedHiveConf,
      String uri) {
    this.expectedHost = expectedHost;
    this.expectedPort = expectedPort;
    this.expectedCatalog = expectedCatalog;
    this.expectedDb = expectedDb;
    this.expectedHiveConf = expectedHiveConf;
    this.uri = uri;
  }

  @Test
  public void testExtractURLComponents() throws JdbcUriParseException {
    JdbcConnectionParams jdbcConnectionParams1 = extractURLComponents(uri, new Properties());
    assertEquals(expectedHost, jdbcConnectionParams1.getHost());
    assertEquals(Integer.parseInt(expectedPort), jdbcConnectionParams1.getPort());
    assertEquals(expectedCatalog, jdbcConnectionParams1.getCatalogName());
    assertEquals(expectedDb, jdbcConnectionParams1.getDbName());
    assertEquals(expectedHiveConf, jdbcConnectionParams1.getHiveConfs());
  }

  @Test
  public void testGetVersion() {
    Pattern pattern = Pattern.compile("^\\d+\\.\\d+\\.\\d+.*");
    assert pattern.matcher(Utils.getVersion()).matches();
  }

  @Test
  public void testSplitSqlStatement() {
    String simpleSql = "select 1 from ? where a = ?";
    List<String> splitSql = Utils.splitSqlStatement(simpleSql);
    assertEquals(3, splitSql.size());
    assertEquals("select 1 from ", splitSql.get(0));
    assertEquals(" where a = ", splitSql.get(1));
    assertEquals("", splitSql.get(2));

    String placeHolderWithinSingleQuote = "select '?' from ? where a = ?";
    splitSql = Utils.splitSqlStatement(placeHolderWithinSingleQuote);
    assertEquals(3, splitSql.size());
    assertEquals("select '?' from ", splitSql.get(0));
    assertEquals(" where a = ", splitSql.get(1));
    assertEquals("", splitSql.get(2));

    String escapePlaceHolder = "select \\? from ? where a = ?";
    splitSql = Utils.splitSqlStatement(escapePlaceHolder);
    assertEquals(3, splitSql.size());
    assertEquals("select \\? from ", splitSql.get(0));
    assertEquals(" where a = ", splitSql.get(1));
    assertEquals("", splitSql.get(2));

    String inQuoteLikeRegexFunction =
        "select "
            + "regexp_extract(field_a, \"[a-zA-Z]+?\", 0) as extracted_a,"
            + "regexp_extract(field_b, '[a-zA-Z]+?', 0) as extracted_b"
            + " from ?";
    splitSql = Utils.splitSqlStatement(inQuoteLikeRegexFunction);
    assertEquals(2, splitSql.size());
    assertEquals(
        "select "
            + "regexp_extract(field_a, \"[a-zA-Z]+?\", 0) as extracted_a,"
            + "regexp_extract(field_b, '[a-zA-Z]+?', 0) as extracted_b from ",
        splitSql.get(0));
    assertEquals("", splitSql.get(1));

    String inCommentBlock = "--comments\n" + "select --? \n" + "? from ?";
    splitSql = Utils.splitSqlStatement(inCommentBlock);
    assertEquals(3, splitSql.size());
    assertEquals("--comments\n" + "select --? \n", splitSql.get(0));
    assertEquals(" from ", splitSql.get(1));
    assertEquals("", splitSql.get(2));

    String inIdentifierQuoted =
        "SELECT "
            + "regexp_replace(col2, '\\n|\\r|\\t', '') as col2, "
            + "`(col2|col2)?+.+` "
            + "FROM ?";
    splitSql = Utils.splitSqlStatement(inIdentifierQuoted);
    assertEquals(2, splitSql.size());
    assertEquals(
        "SELECT regexp_replace(col2, '\\n|\\r|\\t', '') as col2, `(col2|col2)?+.+` FROM ",
        splitSql.get(0));
    assertEquals("", splitSql.get(1));
  }
}
