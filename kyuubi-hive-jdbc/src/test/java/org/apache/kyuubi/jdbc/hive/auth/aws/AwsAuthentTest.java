package org.apache.kyuubi.jdbc.hive.auth.aws;

import java.sql.SQLException;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.kyuubi.jdbc.hive.JdbcConnectionParams;
import org.apache.kyuubi.jdbc.hive.Utils;
import org.apache.kyuubi.jdbc.hive.ZooKeeperHiveClientException;

public class AwsAuthentTest extends TestCase {

  public void testEnrichSsoAws() throws ZooKeeperHiveClientException, SQLException {
        Utils.parseURL(
            "jdbc:kyuubi://localhost:10009/default;awsProvider=sso;awsProfile=default");
  }
}
