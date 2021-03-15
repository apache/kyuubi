package org.apache.kyuubi.web.servlet;

import org.apache.kyuubi.web.conf.ViewModel;
import org.apache.kyuubi.web.dao.ServerDao;
import org.apache.kyuubi.web.dao.StatementDao;
import org.apache.kyuubi.web.model.KResult;
import org.apache.kyuubi.web.model.entity.KServer;
import org.apache.kyuubi.web.model.entity.KStatement;
import org.apache.kyuubi.web.model.view.KStatementView;
import org.apache.kyuubi.web.utils.PageUtils;
import org.apache.kyuubi.web.utils.YarnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("statements")
public class StatementServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatementServlet.class);

    @GET
    @Produces(MediaType.TEXT_HTML)
    public ViewModel getStatements(@Context ServletContext context,
                                   @Context HttpServletRequest req,
                                   @Context HttpServletResponse res,
                                   @QueryParam("contextId") String contextId,
                                   @QueryParam("sessionId") String sessionId,
                                   @QueryParam("statementId") String statementId,
                                   @QueryParam("owner") String owner,
                                   @QueryParam("state") String state,
                                   @QueryParam("sql") String sql,
                                   @QueryParam("serverLocation") String serverLocation,
                                   @QueryParam("startTime") String startTime,
                                   @QueryParam("finishTime") String finishTime,
                                   @QueryParam("pageSize") Integer pageSize,
                                   @QueryParam("pageIndex") Integer pageIndex) {
        int realPage = PageUtils.pageIndex(pageIndex);
        int realSize = PageUtils.pageSize(pageSize);
        List<KStatementView> statementViews = StatementDao.getInstance().queryStatements(contextId, sessionId,
                statementId, owner, state, sql,
                serverLocation, startTime, finishTime, realPage, realSize);
        long totalCount = StatementDao.getInstance().count(contextId, sessionId, statementId, owner, state,
                sql, serverLocation, startTime, finishTime);

        ViewModel viewModel = new ViewModel("/statements.html", req, res);
        viewModel.attr("statements", statementViews);
        viewModel.attr("totalPage", PageUtils.calcTotalPage(totalCount, realSize));
        viewModel.attr("pageIndex", realPage);
        viewModel.attr("yarnUrl", YarnUtils.resolveBaseYarnUrl());
        return viewModel;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<KStatementView> getStatementsJson(@Context ServletContext context,
                                                  @Context HttpServletRequest req,
                                                  @Context HttpServletResponse res,
                                                  @QueryParam("contextId") String contextId,
                                                  @QueryParam("sessionId") String sessionId,
                                                  @QueryParam("statementId") String statementId,
                                                  @QueryParam("owner") String owner,
                                                  @QueryParam("state") String state,
                                                  @QueryParam("sql") String sql,
                                                  @QueryParam("serverLocation") String serverLocation,
                                                  @QueryParam("startTime") String startTime,
                                                  @QueryParam("finishTime") String finishTime,
                                                  @QueryParam("pageSize") Integer pageSize,
                                                  @QueryParam("pageIndex") Integer pageIndex) {
        int realPage = PageUtils.pageIndex(pageIndex);
        int realSize = PageUtils.pageSize(pageSize);
        List<KStatementView> statementViews = StatementDao.getInstance().queryStatements(contextId, sessionId,
                statementId, owner, state, sql, serverLocation, startTime, finishTime, realPage, realSize);
        return statementViews;
    }

    /**
     * cancel to execute sql in executor queue
     *
     * @param statementId SQL Statement ID
     * @return
     */
    @POST
    @Path("cancel")
    @Produces(MediaType.APPLICATION_JSON)
    public KResult cancel(@Context HttpServletRequest req,
                          @Context ServletContext context,
                          @FormParam("id") String statementId) {
        KStatement kStatement = StatementDao.getInstance().queryById(statementId);
        KServer server = ServerDao.getInstance().queryById(kStatement.getServer_id());
        try {
//            KyuubiThriftClient client = new KyuubiThriftClient(server.getIp(), server.getThrift_port());
//            client.startClient();
//            client.cancelStatement(statementId);
//            client.closeClient();
            return KResult.success("Cancel statement module not open");
        } catch (Exception e) {
            LOGGER.warn("Error cancel statement: " + statementId, e);
            return KResult.failure("Cancel statement module not open");
        }
    }

}
