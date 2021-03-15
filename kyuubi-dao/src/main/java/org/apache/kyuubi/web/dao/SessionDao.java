package org.apache.kyuubi.web.dao;

import org.apache.kyuubi.web.model.DeleteStatus;
import org.apache.kyuubi.web.model.KState;
import org.apache.kyuubi.web.model.StatementState;
import org.apache.kyuubi.web.model.entity.KSession;
import org.apache.kyuubi.web.model.view.KSessionView;
import org.apache.kyuubi.web.utils.DaoProxyGenerator;
import org.apache.kyuubi.web.utils.PageUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SessionDao extends BaseDao {

    private static final SessionDao INSTANCE = DaoProxyGenerator.getInstance(SessionDao.class);

    public static SessionDao getInstance() {
        return INSTANCE;
    }

    public static final String SELECT_ALL = "select session_id, context_id,owner,status,start_time," +
            "finish_time,delete_status,server_id,server_location from ksession";

    private static final String INSERT = "insert into ksession (session_id, context_id,owner,status,start_time, " +
            "finish_time,delete_status,server_id,server_location) values (" +
            "?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public long getRunningCount() {
        return count("select count(*) from ksession where status = ? and delete_status = ?",
                "started", DeleteStatus.UNDELETED
        );
    }

    public void save(KSession kSession) {
        exec(INSERT,
                kSession.getSession_id(),
                kSession.getContext_id(),
                kSession.getOwner(),
                kSession.getStatus(),
                dateToStr(kSession.getStart_time()),
                dateToStr(kSession.getFinish_time()),
                kSession.getDelete_status(),
                kSession.getServer_id(),
                kSession.getServer_location()
        );
    }

    public KSession queryById(String sessionId) {
        return queryOne(SELECT_ALL + " where session_id=?", KSession.class, sessionId);
    }

    public List<KSession> querySessions(String sessionId, String contextId, String owner, String serverLocation,
                                        String state, String startTime, String finishTime, int pageIndex, int pageSize) {
        StringBuilder builder = new StringBuilder(SELECT_ALL);
        builder.append(" where 1=1");
        if (StringUtils.isNotBlank(sessionId)) {
            builder.append(" and session_id=?");
        }
        if (StringUtils.isNotBlank(contextId)) {
            builder.append(" and context_id like ?");
        }
        if (StringUtils.isNotBlank(owner)) {
            builder.append(" and owner like ?");
        }
        if (StringUtils.isNotBlank(serverLocation)) {
            builder.append(" and server_location like ?");
        }
        if (StringUtils.isNotBlank(state)) {
            builder.append(" and status=?");
        }
        if (StringUtils.isNotBlank(startTime)) {
            builder.append(" and start_time >= ?");
        }
        if (StringUtils.isNotBlank(finishTime)) {
            builder.append(" and start_time < ?");
        }
        builder.append(" and delete_status = ?");
        builder.append(" order by start_time desc");
        builder.append(PageUtils.limitSQL(pageIndex, pageSize));
        List<KSession> sessions = query(builder.toString(), KSession.class, nonBlankParams(sessionId, likeParam(contextId), likeParam(owner),
                likeParam(serverLocation), state, startTime, finishTime, DeleteStatus.UNDELETED));
        return sessions;
    }

    public List<KSessionView> querySessionViews(String sessionId, String contextId, String owner, String serverLocation,
                                                String state, String startTime, String finishTime, int pageIndex, int pageSize) {
        List<KSession> sessions = querySessions(sessionId, contextId, owner, serverLocation, state, startTime, finishTime, pageIndex, pageSize);
        List<KSessionView> views = sessions.stream().map(KSessionView::new).collect(Collectors.toList());
        for (KSessionView view : views) {
            List<Map<String, Object>> maps = queryMapList("select count(*) as count, status from kstatement where session_id=?", view.getId());
            for (Map<String, Object> map : maps) {
                long num = (long) map.get("count");
                if (Objects.equals(map.get("state"), StatementState.running.name())) {
                    view.incRunningSql(num);
                }
                if (Objects.equals(map.get("state"), StatementState.error.name())) {
                    view.incErrorSql(num);
                }
                view.incTotalSql(num);
            }
        }

        return views;
    }

    public long count(String sessionId, String contextId, String owner, String serverLocation,
                      String state, String startTime, String finishTime) {
        StringBuilder builder = new StringBuilder("select count(*) from ksession ");
        builder.append(" where 1=1");
        if (StringUtils.isNotBlank(sessionId)) {
            builder.append(" and session_id=?");
        }
        if (StringUtils.isNotBlank(contextId)) {
            builder.append(" and context_id like ?");
        }
        if (StringUtils.isNotBlank(owner)) {
            builder.append(" and owner like ?");
        }
        if (StringUtils.isNotBlank(serverLocation)) {
            builder.append(" and server_location like ?");
        }
        if (StringUtils.isNotBlank(state)) {
            builder.append(" and status=?");
        }
        if (StringUtils.isNotBlank(startTime)) {
            builder.append(" and start_time >= ?");
        }
        if (StringUtils.isNotBlank(finishTime)) {
            builder.append(" and start_time < ?");
        }
        builder.append(" and delete_status = ?");
        return count(builder.toString(), nonBlankParams(sessionId, likeParam(contextId), likeParam(owner),
                likeParam(serverLocation), state, startTime, finishTime, DeleteStatus.UNDELETED));
    }

    public void updateStatus(String sessionId, String status) {
        StringBuilder sb = new StringBuilder("update ksession set status=? ");
        Long runtime = null;
        Date now = null;
        if (KState.isFinished(status)) {
            now = new Date();
            sb.append(", finish_time=?");
        }
        sb.append(" where session_id=?");
        exec(sb.toString(), nonBlankParams(status, now, sessionId));
    }


    public void softDelete(String sessionId) {
        exec("update ksession set delete_status=? where session_id=?", DeleteStatus.DELETED, sessionId);
    }

    public void softDelete(Date startTime, Date endTime) {
        exec("update ksession set delete_status=? where start_time >= ? and start_time < ? " +
                        " and status in (?,?,?)", DeleteStatus.DELETED, dateToStr(startTime), dateToStr(endTime),
                KState.killed.name(), KState.stopped.name(), KState.error.name());
    }

    public void closeByContextId(String contextId, String status) {
        StringBuilder sb = new StringBuilder("update ksession set status=? ");
        Date now = new Date();
        sb.append(", finish_time=?");
        sb.append(" where context_id=? and status in (?,?,?)");
        exec(sb.toString(), status, dateToStr(now), contextId, KState.started.name(),
                KState.starting.name(), KState.not_started.name());
    }
}
