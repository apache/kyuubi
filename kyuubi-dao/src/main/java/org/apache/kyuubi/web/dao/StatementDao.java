package org.apache.kyuubi.web.dao;

import org.apache.kyuubi.web.model.DeleteStatus;
import org.apache.kyuubi.web.model.KState;
import org.apache.kyuubi.web.model.StatementState;
import org.apache.kyuubi.web.model.entity.KStatement;
import org.apache.kyuubi.web.model.view.KStatementView;
import org.apache.kyuubi.web.model.view.UserSQLStat;
import org.apache.kyuubi.web.utils.DaoProxyGenerator;
import org.apache.kyuubi.web.utils.PageUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatementDao extends BaseDao {
    private static final StatementDao INSTANCE = DaoProxyGenerator.getInstance(StatementDao.class);

    public static StatementDao getInstance() {
        return INSTANCE;
    }

    public static final String SELECT_ALL = "select statement_id, session_id, context_id,owner,queue,code," +
            "status,row_count,start_time,finish_time,error,runtime,delete_status,server_id,server_location from kstatement";

    private static final String INSERT = "insert kstatement(statement_id, session_id, context_id,owner,queue,code, " +
            "status,row_count,start_time,finish_time,error,runtime,delete_status,server_id,server_location) values (" +
            "?, ?, ?, ?, ?, ?, ?, ? , ? , ?, ? , ?, ? , ?, ?)";

    public void save(KStatement statement) {
        exec(INSERT,
                statement.getStatement_id(),
                statement.getSession_id(),
                statement.getContext_id(),
                statement.getOwner(),
                statement.getQueue(),
                statement.getCode(),
                statement.getStatus(),
                statement.getRow_count(),
                dateToStr(statement.getStart_time()),
                dateToStr(statement.getFinish_time()),
                statement.getError(),
                statement.getRuntime(),
                statement.getDelete_status(),
                statement.getServer_id(),
                statement.getServer_location());
    }

    public KStatement queryById(String statemnetId) {
        return queryOne(SELECT_ALL + " where statement_id=?", KStatement.class, statemnetId);
    }

    public long getRunningCount() {
        return count("select count(*) from kstatement where status = ? or status = ? and delete_status = ?",
                "running", "available", DeleteStatus.UNDELETED
        );
    }

    public List<KStatementView> queryTopRuntimeOn7Days() {
        String sql = SELECT_ALL + " where delete_status=? order by runtime desc limit 10";
        List<KStatement> statements = query(sql, KStatement.class, DeleteStatus.UNDELETED);
        return statements.stream().map(KStatementView::new).collect(Collectors.toList());
    }

    public List<UserSQLStat> queryStatementByUserRankOn7Days() {
        Date ago = DateUtils.addDays(new Date(), -7);

        String sql = "select owner, status, count(*) as count from kstatement where delete_status=? and start_time >= ? group by owner,status";
        Map<String, UserSQLStat> statMap = new HashMap<>();
        List<Map<String, Object>> query = queryMapList(sql, DeleteStatus.UNDELETED, dateToStr(ago));
        for (Map map : query) {
            String owner = (String) map.get("owner");
            String status = (String) map.get("status");
            Long count = (Long) map.get("count");

            UserSQLStat userSQLStat = statMap.computeIfAbsent(owner, UserSQLStat::new);
            if (StatementState.isSuccess(status)) {
                userSQLStat.incSuccess(count);
            } else if (StatementState.isFailure(status)) {
                userSQLStat.incFailure(count);
            }
            userSQLStat.incTotal(count);
        }
        return new ArrayList<>(statMap.values());
    }

    public List<KStatementView> queryStatements(String contextId, String sessionId, String statementId, String owner,
                                                String state, String sql, String serverLocation, String startTime,
                                                String finishTime, Integer pageIndex, Integer pageSize) {
        StringBuilder builder = new StringBuilder(SELECT_ALL);
        builder.append(" where 1=1");
        if (StringUtils.isNotBlank(contextId)) {
            builder.append(" and context_id like ?");
        }
        if (StringUtils.isNotBlank(sessionId)) {
            builder.append(" and session_id = ?");
        }
        if (StringUtils.isNotBlank(statementId)) {
            builder.append(" and statement_id = ?");
        }
        if (StringUtils.isNotBlank(owner)) {
            builder.append(" and owner like ?");
        }
        if (StringUtils.isNotBlank(state)) {
            builder.append(" and status=?");
        }
        if (StringUtils.isNotBlank(sql)) {
            builder.append(" and code like ?");
        }
        if (StringUtils.isNotBlank(serverLocation)) {
            builder.append(" and server_location like ?");
        }
        if (StringUtils.isNotBlank(startTime)) {
            builder.append(" and start_time >= ?");
        }
        if (StringUtils.isNotBlank(finishTime)) {
            builder.append(" and start_time < ?");
        }
        builder.append(" and delete_status=?");
        builder.append(" order by start_time desc");
        builder.append(PageUtils.limitSQL(pageIndex, pageSize));
        List<KStatement> statements = query(builder.toString(), KStatement.class, nonBlankParams(likeParam(contextId), sessionId, statementId, likeParam(owner),
                state, likeParam(sql), likeParam(serverLocation), startTime, finishTime, DeleteStatus.UNDELETED));
        return statements.stream().map(KStatementView::new).collect(Collectors.toList());
    }

    public long count(String contextId, String sessionId, String statementId, String owner,
                      String state, String sql, String serverLocation, String startTime,
                      String finishTime) {
        StringBuilder builder = new StringBuilder("select count(*) from kstatement ");
        builder.append(" where 1=1");
        if (StringUtils.isNotBlank(contextId)) {
            builder.append(" and context_id like ?");
        }
        if (StringUtils.isNotBlank(sessionId)) {
            builder.append(" and session_id = ?");
        }
        if (StringUtils.isNotBlank(statementId)) {
            builder.append(" and statement_id = ?");
        }
        if (StringUtils.isNotBlank(owner)) {
            builder.append(" and owner like ?");
        }
        if (StringUtils.isNotBlank(state)) {
            builder.append(" and status=?");
        }
        if (StringUtils.isNotBlank(sql)) {
            builder.append(" and code like ?");
        }
        if (StringUtils.isNotBlank(serverLocation)) {
            builder.append(" and server_location like ?");
        }
        if (StringUtils.isNotBlank(startTime)) {
            builder.append(" and start_time >= ?");
        }
        if (StringUtils.isNotBlank(finishTime)) {
            builder.append(" and start_time < ?");
        }
        builder.append(" and delete_status=?");
        return count(builder.toString(), nonBlankParams(likeParam(contextId), sessionId, statementId, likeParam(owner),
                state, likeParam(sql), likeParam(serverLocation), startTime, finishTime, DeleteStatus.UNDELETED));
    }

    public void updateStatus(String statementId, String status) {
        StringBuilder sb = new StringBuilder("update kstatement set status=? ");
        Long runtime = null;
        Date finishTime = null;
        if (StatementState.isFinished(status)) {
            finishTime = new Date();
            runtime = finishTime.getTime() - queryById(statementId).getStart_time().getTime();
            sb.append(", runtime=?");
            sb.append(", finish_time=?");
        }
        sb.append(" where statement_id=?");
        exec(sb.toString(), nonBlankParams(status, runtime, dateToStr(finishTime), statementId));
    }

    public void updateError(String statementId, String msg) {
        StringBuilder sb = new StringBuilder("update kstatement set status=? ");
        Date finishTime = new Date();
        Long runtime = finishTime.getTime() - queryById(statementId).getStart_time().getTime();
        sb.append(", runtime=?");
        sb.append(", finish_time=?");
        sb.append(", error=?");
        sb.append(" where statement_id=?");
        exec(sb.toString(), nonBlankParams(StatementState.error.name(), runtime, dateToStr(finishTime), msg, statementId));
    }

    public void updateRowCount(String statementId, long rowcount) {
        exec("update kstatement set row_count=? where statement_id=?", rowcount, statementId);
    }

    public void softDelete(String statementId) {
        exec("update kstatement set delete_status=? where statement_id=?", DeleteStatus.DELETED, statementId);
    }

    public void softDelete(Date startTime, Date endTime) {
        exec("update kstatement set delete_status=? where start_time >= ? and start_time < ?" +
                        "and status in (?,?,?)", DeleteStatus.DELETED, dateToStr(startTime), dateToStr(endTime),
                StatementState.closed.name(), StatementState.cancelled.name(), StatementState.error.name());
    }

    public void closeByContextId(String contextId, String status) {
        KState kState = KState.valueOf(status);
        StatementState statementState = StatementState.closed;
        if (kState == KState.error) {
            statementState = StatementState.error;
        } else if (kState == KState.killed) {
            statementState = StatementState.cancelled;
        }
        StringBuilder sb = new StringBuilder("update kstatement set status=? ");
        Date now = new Date();
        sb.append(", runtime=0");
        sb.append(", finish_time=?");
        sb.append(" where context_id=? and status in (?,?,?)");
        exec(sb.toString(), statementState.name(), dateToStr(now), contextId,
                StatementState.available.name(), StatementState.running.name(), StatementState.waiting.name());
    }
}
