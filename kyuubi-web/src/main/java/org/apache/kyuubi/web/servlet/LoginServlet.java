package org.apache.kyuubi.web.servlet;

import org.apache.kyuubi.web.conf.ViewModel;
import org.apache.kyuubi.web.dao.ContextDao;
import org.apache.kyuubi.web.dao.MetricDao;
import org.apache.kyuubi.web.dao.ServerDao;
import org.apache.kyuubi.web.dao.SessionDao;
import org.apache.kyuubi.web.dao.StatementDao;
import org.apache.kyuubi.web.model.KResult;
import org.apache.kyuubi.web.model.entity.KMetric;
import org.apache.kyuubi.web.model.entity.KServer;
import org.apache.kyuubi.web.utils.CollectionUtils;
import org.apache.kyuubi.web.utils.KyuubiWebConf;
import org.apache.kyuubi.web.utils.ServletUtils;
import org.apache.kyuubi.web.utils.auth.AuthenticationProviderFactory;
import org.apache.kyuubi.web.utils.auth.PasswdAuthenticationProvider;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kyuubi.web.KyuubiWebServer;
import org.glassfish.jersey.server.mvc.Viewable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kyuubi.common.KyuubiConfigs.AUTHENTICATION_METHOD;

@Path("")
public class LoginServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoginServlet.class);

    @GET
    @Path("index")
    @Produces(MediaType.TEXT_HTML)
    public Viewable index(@Context ServletContext context,
                          @Context HttpServletRequest req,
                          @Context HttpServletResponse res) {
        ViewModel model = new ViewModel("/index.html", req, res);
        List<KServer> servers = ServerDao.getInstance().getServers();

        model.attr("servers", servers);
        model.attr("runningStatementNum", StatementDao.getInstance().getRunningCount());
        model.attr("runningSessions", SessionDao.getInstance().getRunningCount());
        model.attr("runningContexts", ContextDao.getInstance().getRunningCount());

        model.attr("statements", StatementDao.getInstance().queryTopRuntimeOn7Days());
        model.attr("userRanks", StatementDao.getInstance().queryStatementByUserRankOn7Days());
        Date timeAgo = DateUtils.addDays(new Date(), -7);
        model.attr("userRankStartTime", DateFormatUtils.format(timeAgo, "yyyy-MM-dd HH:mm:ss"));
        return model;
    }

    @GET
    @Path("metric")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> metrics(@QueryParam("serverId") String serverId,
                                       @QueryParam("name") String name,
                                       @QueryParam("startTime") String startTime,
                                       @QueryParam("endTime") String endTime) {
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> series = new ArrayList<>();
        data.put("series", series);
        for (String item : name.split(",")) {
            List<KMetric> metrics = MetricDao.getInstance().getMetrics(serverId, item, startTime, endTime);
            List<String> timeList = metrics.stream()
                    .map(KMetric::getUpdate_time)
                    .map(time -> DateFormatUtils.format(time, "HH:mm:ss"))
                    .collect(Collectors.toList());
            data.putIfAbsent("categories", timeList);
            series.add(CollectionUtils.mapOf("name", item, "data", metrics.stream().map(KMetric::getMetric_value).collect(Collectors.toList())));
        }
        return data;
    }

    @POST
    @Path("metric/del")
    @Produces(MediaType.APPLICATION_JSON)
    public KResult deleteServer(@Context HttpServletRequest req,
                                @FormParam("serverId") String serverId) {
        if(ServletUtils.isAdmin(req)) {
            ServerDao.getInstance().softDeleteAllRelation(serverId);
            return new KResult("delete success", 0);
        } else {
            return KResult.failure("Permission not allowed");
        }


    }

    @GET
    @Path("login")
    @Produces(MediaType.TEXT_HTML)
    public ViewModel loginPage(@Context ServletContext context,
                               @Context HttpServletRequest req,
                               @Context HttpServletResponse res) {
        return new ViewModel("/login.html", req, res);
    }

    @POST
    @Path("login")
    @Produces(MediaType.APPLICATION_JSON)
    public KResult login(@Context ServletContext context,
                         @Context HttpServletRequest req,
                         @FormParam("username") String username,
                         @FormParam("password") String password) {
        String pass = new String(Base64.decodeBase64(password));
        String method = KyuubiWebConf.load().get(AUTHENTICATION_METHOD);
        PasswdAuthenticationProvider authenticationProvider = AuthenticationProviderFactory.getAuthenticationProvider(method, KyuubiWebConf.load());
        try {
            authenticationProvider.authenticate(username, pass);
            req.getSession().setAttribute(KyuubiWebServer.USER_LOGIN_SUCCESS_ATTR, true);
            req.getSession().setAttribute(KyuubiWebServer.LOGIN_USER_ATTR, username);
            req.getSession().setAttribute(KyuubiWebServer.LOGIN_USER_PASSWORD_ATTR, pass);
            return new KResult("login success", 0);
        } catch (Exception e) {
            LOGGER.warn("Failed  to auth login", e);
            return new KResult("Error auth to login", 1);
        }
    }

    @GET
    @Path("logout")
    @Produces(MediaType.APPLICATION_JSON)
    public KResult logout(
            @Context HttpServletRequest req) {
        req.getSession().removeAttribute(KyuubiWebServer.USER_LOGIN_SUCCESS_ATTR);
        req.getSession().removeAttribute(KyuubiWebServer.LOGIN_USER_ATTR);
        return new KResult("logout success", 0);
    }

}
