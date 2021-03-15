package org.apache.kyuubi.web.conf;

import org.apache.kyuubi.common.KyuubiConfigs;
import org.apache.kyuubi.web.utils.KyuubiWebConf;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class DataSourceConfig {

    private static DataSource ds = null;

    public static synchronized DataSource load() {
        KyuubiWebConf weConf = KyuubiWebConf.load();
        boolean auditEnabled = weConf.getBoolean(KyuubiConfigs.KYUUBI_AUDIT_ENABLED);
        if (ds == null && auditEnabled) {
            String user = weConf.get(KyuubiConfigs.KYUUBI_AUDIT_DB_USER);
            String driver = weConf.get(KyuubiConfigs.KYUUBI_AUDIT_DB_DRIVER);
            String pwd = weConf.get(KyuubiConfigs.KYUUBI_AUDIT_DB_PASSWORD);
            String url = weConf.get(KyuubiConfigs.KYUUBI_AUDIT_DB_URL);

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setUsername(user);
            config.setPassword(pwd);
            config.setDriverClassName(driver);
            config.setMinimumIdle(1);
            config.setMaximumPoolSize(10);

            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

            HikariDataSource dataSource = new HikariDataSource(config);


            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    dataSource.close();
                }
            }));
            ds = dataSource;
        }
        return ds;

    }

}
