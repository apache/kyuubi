package org.apache.kyuubi;

import org.apache.kyuubi.jdbc.hive.KyuubiStatement;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class KyuubiUtilTest {
  private static String driverName = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";
  // TODO: 2022/8/25  url
  private static String url = "";

  public static String[] sqlTemplate() throws SQLException {
    String[] template =
        new String[] {
          "val script = \"SELECT 1,2,3,4,4,5,5\"",
          "val df= spark.sql(script)",
          "df.show()",
          "df.show()",
          "df.show()",
          "df.show()"
        };
    return template;
  }

  @Test
  public void test() throws Exception {
    int count = 15;
    Class.forName(driverName);
    CountDownLatch countDownLatch = new CountDownLatch(count);
    try {
      for (int i = 0; i < count; i++) {
        new Thread(
                new Runnable() {
                  @Override
                  public void run() {
                    Connection con = null;
                    KyuubiStatement state = null;
                    try {
                      // TODO: 2022/8/25  user name
                      con = DriverManager.getConnection(url, "", "");
                      state = (KyuubiStatement) con.createStatement();
                      String query = String.join("\n", sqlTemplate());
                      state.executeScala(query);
                      List<String> execLog = state.getExecLog();
                      for (String log : execLog) {
                        System.out.println(Thread.currentThread().getId() + ":" + log);
                      }
                    } catch (SQLException e) {
                      e.printStackTrace();
                    } finally {
                      try {
                        if (state != null) {
                          state.close();
                        }
                        if (con != null) {
                          con.close();
                        }
                      } catch (SQLException e) {
                        e.printStackTrace();
                      }
                      countDownLatch.countDown();
                    }
                  }
                })
            .start();
      }
      countDownLatch.await();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
