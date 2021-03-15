package org.apache.kyuubi.web.conf;

import org.apache.kyuubi.common.KyuubiConfigs;
import org.apache.kyuubi.web.dao.ContextDao;
import org.apache.kyuubi.web.dao.MetricDao;
import org.apache.kyuubi.web.dao.SessionDao;
import org.apache.kyuubi.web.dao.StatementDao;
import org.apache.kyuubi.web.model.KState;
import org.apache.kyuubi.web.model.view.KContextView;
import org.apache.kyuubi.web.utils.KyuubiWebConf;
import org.apache.kyuubi.web.utils.YarnUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class AuditCleaner implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditCleaner.class);
    private Thread thread;

    private static final long ONE_DAY_MILLS = 24L * 3600 * 1000L;

    public AuditCleaner() {
        thread = new Thread(this);
        thread.setName("Audit-Cleaner");
        thread.setDaemon(true);
    }

    public void start() {
        thread.start();
        LOGGER.info("Audit-Cleaner thread started.");
    }

    @Override
    public void run() {
        KyuubiWebConf conf = KyuubiWebConf.load();
        int days = conf.getInt(KyuubiConfigs.KYUUBI_AUDIT_CLEAN_INTERVAL_DAYS);

        while (true) {
            try {
                LOGGER.info("do cleaning...");
                Date date = DateUtils.addDays(new Date(), -1 * days);
                Date dateYesterday = DateUtils.addDays(date, -1);

                ContextDao.getInstance().softDelete(dateYesterday, date);
                SessionDao.getInstance().softDelete(dateYesterday, date);
                StatementDao.getInstance().softDelete(dateYesterday, date);
                MetricDao.getInstance().softDelete(dateYesterday, date);

                YarnClient client = YarnClient.createYarnClient();
                Configuration entries = new YarnConfiguration();
                entries.set("yarn.resourcemanager.connect.max-wait.ms", "30000");
                client.init(entries);
                client.start();
                Date yesterday = DateUtils.addDays(new Date(), -1);
                List<KContextView> lastDayContexts = ContextDao.getInstance().queryContexts(null, null, null,
                        KState.started.name(),
                        DateFormatUtils.format(yesterday, "yyyy-MM-dd HH:mm:ss"),
                        DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"),
                        1, 1000);
                for (KContextView lastDayContext : lastDayContexts) {
                    ApplicationId applicationId = ApplicationId.fromString(lastDayContext.getId());
                    ApplicationReport applicationReport = client.getApplicationReport(applicationId);
                    if (YarnUtils.isFinishedState(applicationReport.getYarnApplicationState())) {
                        client.killApplication(applicationId);
                        ContextDao.getInstance().closeContextWithSessionAndStatement(lastDayContext.getId(), KState.stopped.name());
                    }
                }
                client.close();
            } catch (Exception e) {
                LOGGER.warn("Error to clean audit history records.", e);
            } finally {
                try {
                    LOGGER.info("sleeping...");
                    Thread.sleep(ONE_DAY_MILLS);
                } catch (InterruptedException e) {
                    LOGGER.warn("Audit-Clean thread interrupt and exit.");
                }
            }
        }

    }

}
