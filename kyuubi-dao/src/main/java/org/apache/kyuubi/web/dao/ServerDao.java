package org.apache.kyuubi.web.dao;

import org.apache.kyuubi.web.model.DeleteStatus;
import org.apache.kyuubi.web.model.entity.KServer;
import org.apache.kyuubi.web.utils.DaoProxyGenerator;

import java.util.List;

public class ServerDao extends BaseDao {

    private static final ServerDao INSTANCE = DaoProxyGenerator.getInstance(ServerDao.class);

    public static ServerDao getInstance() {
        return INSTANCE;
    }

    public static final String SELECT_ALL = "select server_id,ip,thrift_port,http_port,start_time,delete_status from kserver";

    public static final String INSERT = "insert into kserver(server_id,ip,thrift_port,http_port,start_time,delete_status )" +
            " values (?, ? , ? , ? , ? , ?)";

    public void saveOrUpdate(KServer server) {
        KServer kServer = queryById(server.getServer_id());
        if (kServer == null) {
            exec(INSERT,
                    server.getServer_id(),
                    server.getIp(),
                    server.getThrift_port(),
                    server.getHttp_port(),
                    dateToStr(server.getStart_time()),
                    server.getDelete_status());
        } else {
            exec("update kserver set ip=?, thrift_port=?, http_port=?, start_time=? where server_id=?",
                    server.getIp(),
                    server.getThrift_port(),
                    server.getHttp_port(),
                    dateToStr(server.getStart_time()),
                    server.getServer_id());
        }

    }

    public List<KServer> getServers() {
        String sql = SELECT_ALL + " where delete_status=?";
        return query(sql, KServer.class, DeleteStatus.UNDELETED);
    }

    public KServer queryById(String server_id) {
        return queryOne(SELECT_ALL + " where server_id=? and delete_status=?", KServer.class,
                server_id, DeleteStatus.UNDELETED);
    }

    public void softDeleteAllRelation(String serverId) {
        exec("update kmetric set delete_status=? where server_id=?", DeleteStatus.DELETED, serverId);
        exec("update kcontext set delete_status=? where server_id=?", DeleteStatus.DELETED, serverId);
        exec("update ksession set delete_status=? where server_id=?", DeleteStatus.DELETED, serverId);
        exec("update kstatement set delete_status=? where server_id=?", DeleteStatus.DELETED, serverId);
        exec("update kserver set delete_status=? where server_id=?", DeleteStatus.DELETED, serverId);
    }

}
