package org.apache.kyuubi.jdbc.hive;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import org.junit.Test;

public class UtilsTest {

  @Test
  public void testExtractURLComponentsWithAuthoritiesIsEmpty() throws JdbcUriParseException {
    String uri1 = "jdbc:hive2:///db;k1=v1?k2=v2#k3=v3";
    String uri2 = "jdbc:hive2:///";
    Utils.JdbcConnectionParams jdbcConnectionParams1 =
        Utils.extractURLComponents(uri1, new Properties());
    assertEquals("localhost", jdbcConnectionParams1.getHost());
    assertEquals(10009, jdbcConnectionParams1.getPort());

    Utils.JdbcConnectionParams jdbcConnectionParams2 =
        Utils.extractURLComponents(uri1, new Properties());
    assertEquals("localhost", jdbcConnectionParams2.getHost());
    assertEquals(10009, jdbcConnectionParams2.getPort());
  }

  @Test
  public void testExtractURLComponentsWithAuthoritiesIsNotEmpty() throws JdbcUriParseException {
    String uri = "jdbc:hive2://hostname:10018/db;k1=v1?k2=v2#k3=v3";
    Utils.JdbcConnectionParams jdbcConnectionParams =
        Utils.extractURLComponents(uri, new Properties());
    assertEquals("hostname", jdbcConnectionParams.getHost());
    assertEquals(10018, jdbcConnectionParams.getPort());
  }
}
