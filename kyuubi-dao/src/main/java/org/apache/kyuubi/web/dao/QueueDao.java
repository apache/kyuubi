package org.apache.kyuubi.web.dao;

import org.apache.kyuubi.web.model.DeleteStatus;
import org.apache.kyuubi.web.model.entity.KQueue;
import org.apache.kyuubi.web.utils.DaoProxyGenerator;

import java.util.List;

public class QueueDao extends BaseDao {

    private static final QueueDao INSTANCE = DaoProxyGenerator.getInstance(QueueDao.class);

    public static QueueDao getInstance() {
        return INSTANCE;
    }

    private static final String SELECT_ALL = "select username, queue, delete_status from kqueue";

    public void saveOrUpdate(KQueue kQueue) {
        String sql = SELECT_ALL + " where username=? and delete_status=?";
        KQueue queueInDb = queryOne(sql, KQueue.class, kQueue.getUsername(), DeleteStatus.UNDELETED);
        if (queueInDb == null) {
            exec("insert into kqueue(username, queue, delete_status) values (?, ?, ?)",
                    kQueue.getUsername(), kQueue.getQueue(), DeleteStatus.UNDELETED);
        } else {
            exec("update kqueue set queue=? where username=? and delete_status=?",
                    kQueue.getQueue(), kQueue.getUsername(), DeleteStatus.UNDELETED);
        }

    }

    public List<KQueue> getQueues() {
        String sql = SELECT_ALL + " where delete_status=?";
        return query(sql, KQueue.class, DeleteStatus.UNDELETED);
    }

    public String getQueue(String username) {
        KQueue queue = queryOne(SELECT_ALL + " where username=? and delete_status=?", KQueue.class,
                username, DeleteStatus.UNDELETED);
        if (queue != null) {
            return queue.getQueue();
        } else {
            return null;
        }
    }

    public void softDeleteByUser(String username) {
        exec("update kqueue set delete_status=? where username=? and delete_status=?",
                DeleteStatus.DELETED, username, DeleteStatus.UNDELETED);
    }
}
