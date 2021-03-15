package org.apache.kyuubi.web.dao;

import org.apache.kyuubi.web.model.DeleteStatus;
import org.apache.kyuubi.web.model.entity.KMetric;
import org.apache.kyuubi.web.utils.DaoProxyGenerator;

import java.util.Date;
import java.util.List;

public class MetricDao extends BaseDao {
    private static final MetricDao INSTANCE = DaoProxyGenerator.getInstance(MetricDao.class);

    public static MetricDao getInstance() {
        return INSTANCE;
    }

    private static final String SELECT_ALL = "select server_id,name,metric_value,update_time,delete_status from kmetric";

    private static final String INSERT = "insert into kmetric(server_id,name,metric_value,update_time,delete_status)" +
            " values (?,?,?,?,?)";

    public void save(KMetric metric) {
        exec(INSERT, metric.getServer_id(), metric.getName(), metric.getMetric_value(), metric.getUpdate_time(), metric.getDelete_status());
    }

    public List<KMetric> getMetrics(String serverId, String name, String startTime, String endTime) {
        String sql = SELECT_ALL + " where server_id=? and name=? and update_time >= ? and update_time < ? and delete_status=?";
        return query(sql, KMetric.class, serverId, name, startTime, endTime, DeleteStatus.UNDELETED);
    }

    public void softDelete(Date startTime, Date endTime) {
        exec("update kmetric set delete_status=? where update_time >= ? and update_time < ?",
                DeleteStatus.DELETED, dateToStr(startTime), dateToStr(endTime));
    }


}
