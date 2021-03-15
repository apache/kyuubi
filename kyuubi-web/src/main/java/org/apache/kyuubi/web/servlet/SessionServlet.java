package org.apache.kyuubi.web.servlet;

import org.apache.kyuubi.web.conf.ViewModel;
import org.apache.kyuubi.web.dao.SessionDao;
import org.apache.kyuubi.web.model.view.KSessionView;
import org.apache.kyuubi.web.utils.PageUtils;
import org.apache.kyuubi.web.utils.YarnUtils;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("sessions")
public class SessionServlet {

    @GET
    @Produces(MediaType.TEXT_HTML)
    public ViewModel getSessions(@Context ServletContext context,
                                 @Context HttpServletRequest req,
                                 @Context HttpServletResponse res,
                                 @QueryParam("contextId") String contextId,
                                 @QueryParam("sessionId") String sessionId,
                                 @QueryParam("owner") String owner,
                                 @QueryParam("serverLocation") String serverLocation,
                                 @QueryParam("state") String state,
                                 @QueryParam("startTime") String startTime,
                                 @QueryParam("finishTime") String finishTime,
                                 @QueryParam("pageSize") Integer pageSize,
                                 @QueryParam("pageIndex") Integer pageIndex) {
        int realPage = PageUtils.pageIndex(pageIndex);
        int realSize = PageUtils.pageSize(pageSize);

        List<KSessionView> kSessionViews = SessionDao.getInstance().querySessionViews(sessionId, contextId, owner,
                serverLocation, state, startTime, finishTime, realPage, realSize);
        long totalCount = SessionDao.getInstance().count(sessionId, contextId, owner, serverLocation, state, startTime, finishTime);

        ViewModel viewModel = new ViewModel("/sessions.html", req, res);
        viewModel.attr("totalPage", PageUtils.calcTotalPage(totalCount, realSize));
        viewModel.attr("spark_sessions", kSessionViews);
        viewModel.attr("pageIndex", realPage);
        viewModel.attr("yarnUrl", YarnUtils.resolveBaseYarnUrl());
        return viewModel;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<KSessionView> getSessionsJson(@Context ServletContext context,
                                          @Context HttpServletRequest req,
                                          @Context HttpServletResponse res,
                                          @QueryParam("contextId") String contextId,
                                          @QueryParam("sessionId") String sessionId,
                                          @QueryParam("owner") String owner,
                                          @QueryParam("serverLocation") String serverLocation,
                                          @QueryParam("state") String state,
                                          @QueryParam("startTime") String startTime,
                                          @QueryParam("finishTime") String finishTime,
                                          @QueryParam("pageSize") Integer pageSize,
                                          @QueryParam("pageIndex") Integer pageIndex) {
        int realPage = PageUtils.pageIndex(pageIndex);
        int realSize = PageUtils.pageSize(pageSize);

        return SessionDao.getInstance().querySessionViews(sessionId, contextId, owner,
                serverLocation, state, startTime, finishTime, realPage, realSize);
    }
}
