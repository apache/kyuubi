package org.apache.kyuubi.web.servlet;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kyuubi.web.conf.ViewModel;
import org.apache.kyuubi.web.dao.ContextDao;
import org.apache.kyuubi.web.KyuubiWebServer;
import org.apache.kyuubi.web.model.KResult;
import org.apache.kyuubi.web.model.KState;
import org.apache.kyuubi.web.model.view.KContextView;
import org.apache.kyuubi.web.utils.PageUtils;
import org.apache.kyuubi.web.utils.ServletUtils;
import org.apache.kyuubi.web.utils.YarnUtils;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;

@Path("contexts")
public class ContextServlet {
    YarnClient yarn = YarnClient.createYarnClient();

    {
        yarn.init(YarnUtils.getYarnConf());
        yarn.start();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public ViewModel getContexts(@Context ServletContext context,
                                 @Context HttpServletRequest req,
                                 @Context HttpServletResponse res,
                                 @QueryParam("contextId") String contextId,
                                 @QueryParam("owner") String owner,
                                 @QueryParam("serverLocation") String serverLocation,
                                 @QueryParam("state") String state,
                                 @QueryParam("startTime") String startTime,
                                 @QueryParam("finishTime") String finishTime,
                                 @QueryParam("pageSize") Integer pageSize,
                                 @QueryParam("pageIndex") Integer pageIndex) {
        int realPage = PageUtils.pageIndex(pageIndex);
        int realSize = PageUtils.pageSize(pageSize);

        List<KContextView> contexts = ContextDao.getInstance().queryContexts(contextId, owner, serverLocation, state,
                startTime, finishTime, realPage, realSize);

        long totalCount = ContextDao.getInstance().count(contextId, owner, serverLocation, state,
                startTime, finishTime);

        ViewModel viewModel = new ViewModel("/contexts.html", req, res);
        viewModel.attr("contexts", contexts);
        viewModel.attr("totalPage", PageUtils.calcTotalPage(totalCount, realSize));
        viewModel.attr("pageIndex", realPage);
        boolean isAdmin = ServletUtils.isAdmin(req);
        viewModel.attr("isAdmin", isAdmin);
        viewModel.attr("yarnUrl", YarnUtils.resolveBaseYarnUrl());

        return viewModel;
    }


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<KContextView> getContextsJson(@Context ServletContext context,
                                              @Context HttpServletRequest req,
                                              @Context HttpServletResponse res,
                                              @QueryParam("contextId") String contextId,
                                              @QueryParam("owner") String owner,
                                              @QueryParam("serverLocation") String serverLocation,
                                              @QueryParam("state") String state,
                                              @QueryParam("startTime") String startTime,
                                              @QueryParam("finishTime") String finishTime,
                                              @QueryParam("pageSize") Integer pageSize,
                                              @QueryParam("pageIndex") Integer pageIndex) {
        int realPage = PageUtils.pageIndex(pageIndex);
        int realSize = PageUtils.pageSize(pageSize);

        return ContextDao.getInstance().queryContexts(contextId, owner, serverLocation, state,
                startTime, finishTime, realPage, realSize);
    }

    @POST
    @Path("kill")
    @Produces(MediaType.APPLICATION_JSON)
    public KResult cancel(@Context HttpServletRequest req,
                          @FormParam("id") String contextId) throws IOException, YarnException {
        String loginUser = (String) req.getSession().getAttribute(KyuubiWebServer.LOGIN_USER_ATTR);
        boolean admin = ServletUtils.isAdmin(req);
        if (!admin) {
            throw new RuntimeException("User[" + loginUser + "] has no permission to kill the application: " + contextId);
        }
        ApplicationId appId = ApplicationId.fromString(contextId);
        ApplicationReport report = yarn.getApplicationReport(appId);
        if (report != null && YarnUtils.isRunningState(report.getYarnApplicationState())) {
            yarn.killApplication(appId);
        }
        ContextDao.getInstance().closeContextWithSessionAndStatement(contextId, KState.killed.name());
        return KResult.success("cancel statement success");
    }

}
