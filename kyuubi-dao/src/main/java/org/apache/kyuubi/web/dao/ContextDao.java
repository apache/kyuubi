package org.apache.kyuubi.web.dao;

import org.apache.kyuubi.web.model.DeleteStatus;
import org.apache.kyuubi.web.model.KState;
import org.apache.kyuubi.web.model.entity.KContext;
import org.apache.kyuubi.web.model.view.KContextView;
import org.apache.kyuubi.web.utils.DaoProxyGenerator;
import org.apache.kyuubi.web.utils.PageUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class ContextDao extends BaseDao {

    private static final ContextDao INSTANCE = DaoProxyGenerator.getInstance(ContextDao.class);

    public static ContextDao getInstance() {
        return INSTANCE;
    }

    private static final String SELECT_ALL = "select context_id, name,owner,status,queue,executors," +
            "executor_vcores,executor_memory,start_time,finish_time,delete_status,server_id,server_location from kcontext";

    public long getRunningCount() {
        return count("select count(*) from kcontext where status = ? and delete_status = ?",
                "started", DeleteStatus.UNDELETED
        );
    }

    public void save(KContext context) {
        exec("insert into kcontext(context_id, name,owner,status,queue,executors," +
                        "executor_vcores,executor_memory,start_time,finish_time,delete_status,server_id,server_location) values" +
                        "(?,?,? ,?,?,?, ?,?,?, ?,?,?, ?)",
                context.getContext_id(),
                context.getName(),
                context.getOwner(),
                context.getStatus(),
                context.getQueue(),
                context.getExecutors(),
                context.getExecutor_vcores(),
                context.getExecutor_memory(),
                dateToStr(context.getStart_time()),
                dateToStr(context.getFinish_time()),
                context.getDelete_status(),
                context.getServer_id(),
                context.getServer_location());
    }

    public List<KContextView> queryContexts(String contextId, String owner, String serverLocation, String state,
                                            String startTime, String finishTime, Integer pageIndex, Integer pageSize) {
        StringBuilder condition = new StringBuilder(SELECT_ALL);
        condition.append(" where 1=1 ");
        if (StringUtils.isNoneBlank(contextId)) {
            condition.append(" and context_id like ?");
        }
        if (StringUtils.isNoneBlank(serverLocation)) {
            condition.append(" and server_location like ?");
        }
        if (StringUtils.isNoneBlank(owner)) {
            condition.append(" and owner like ?");
        }
        if (StringUtils.isNoneBlank(state)) {
            condition.append(" and status = ?");
        }
        if (StringUtils.isNoneBlank(startTime)) {
            condition.append(" and start_time >= ?");
        }
        if (StringUtils.isNoneBlank(finishTime)) {
            condition.append(" and start_time < ?");
        }
        condition.append(" and delete_status = ?");
        condition.append(" order by start_time desc");
        condition.append(PageUtils.limitSQL(pageIndex, pageSize));

        List<KContext> contexts = query(condition.toString(), KContext.class,
                nonBlankParams(likeParam(contextId), likeParam(serverLocation), likeParam(owner),
                        state, startTime, finishTime, DeleteStatus.UNDELETED));
        return contexts.stream().map(KContextView::new).collect(Collectors.toList());
    }

    public long count(String contextId, String owner, String serverLocation, String state,
                      String startTime, String finishTime) {
        StringBuilder condition = new StringBuilder("select count(*) from kcontext ");
        condition.append(" where 1=1 ");
        if (StringUtils.isNoneBlank(contextId)) {
            condition.append(" and context_id like ?");
        }
        if (StringUtils.isNoneBlank(serverLocation)) {
            condition.append(" and server_location like ?");
        }
        if (StringUtils.isNoneBlank(owner)) {
            condition.append(" and owner like ?");
        }
        if (StringUtils.isNoneBlank(state)) {
            condition.append(" and status = ?");
        }
        if (StringUtils.isNoneBlank(startTime)) {
            condition.append(" and start_time >= ?");
        }
        if (StringUtils.isNoneBlank(finishTime)) {
            condition.append(" and start_time < ?");
        }
        condition.append(" and delete_status = ?");
        return count(condition.toString(), nonBlankParams(likeParam(contextId), likeParam(serverLocation), likeParam(owner),
                state, startTime, finishTime, DeleteStatus.UNDELETED));
    }

    public void updateStatus(String contextId, String status) {
        StringBuilder sb = new StringBuilder("update kcontext set status=? ");
        Date finishTime = null;
        if (KState.isFinished(status)) {
            finishTime = new Date();
            sb.append(", finish_time=?");
        }
        sb.append(" where context_id=?");
        exec(sb.toString(), nonBlankParams(status, dateToStr(finishTime), contextId));
    }

    public void closeContextWithSessionAndStatement(String contextId, String status) {
        updateStatus(contextId, status);
        SessionDao.getInstance().closeByContextId(contextId, status);
        StatementDao.getInstance().closeByContextId(contextId, status);
    }


    public void softDelete(String contextId) {
        exec("update kcontext set delete_status=? where context_id=?", DeleteStatus.DELETED, contextId);
    }

    public void softDelete(Date startTime, Date endTime) {
        exec("update kcontext set delete_status=? where start_time >= ? and start_time < ?" +
                        " and status in (?,?,?)", DeleteStatus.DELETED, dateToStr(startTime), dateToStr(endTime),
                KState.killed.name(), KState.stopped.name(), KState.error.name());
    }
}
