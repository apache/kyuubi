package org.apache.kyuubi.web;

import org.apache.kyuubi.common.KyuubiConfigs;
import org.apache.kyuubi.web.conf.BasicAuthFilter;
import org.apache.kyuubi.web.utils.KyuubiWebConf;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.EnumSet;

public class KyuubiWebServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KyuubiWebServer.class);

    public static final String REST_VERSION_CONTEXT_PATH = "/v1";
    public static final String USER_LOGIN_SUCCESS_ATTR = "use_login_success";
    public static final String LOGIN_USER_ATTR = "login_user";
    public static final String LOGIN_USER_PASSWORD_ATTR = "login_user_pwd";

    public static final String BANNER = "";
    private Server server;
    private KyuubiWebConf conf = KyuubiWebConf.load();

    public void init() throws MalformedURLException {
        String host = conf.get(KyuubiConfigs.KYUUBI_AUDIT_HTTP_HOST);
        int port = Integer.parseInt(conf.get(KyuubiConfigs.KYUUBI_AUDIT_HTTP_PORT));
        server = new Server();
        ServerConnector httpConnector = new ServerConnector(server);
        httpConnector.setHost(host);
        httpConnector.setPort(port);
        httpConnector.setIdleTimeout(50000);
        server.addConnector(httpConnector);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(REST_VERSION_CONTEXT_PATH);


        URL classPath = KyuubiWebServer.class.getResource("/web");
        LOGGER.info("kyuubi web base path: " + classPath.toString());
        context.setBaseResource(Resource.newResource(classPath));
        ServletHolder serHol = context.addServlet(ServletContainer.class, "/rest/*");
        serHol.setInitParameter("javax.ws.rs.Application", "org.apache.kyuubi.web.conf.ViewApplicationConfig");
        serHol.setInitOrder(1);

        ServletHolder holderHome = new ServletHolder("static-home", DefaultServlet.class);
        holderHome.setInitParameter("resourceBase", classPath.toString() + "/static");
        holderHome.setInitParameter("dirAllowed", "true");
        holderHome.setInitParameter("pathInfoOnly", "true");
        context.addServlet(holderHome, "/*");

        context.addFilter(BasicAuthFilter.class, "/rest/*", EnumSet.of(DispatcherType.REQUEST));

        server.setHandler(context);
    }

    public void start() throws Exception {
        server.start();
        server.join();
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Staring kyuubi Web Server.");
        LOGGER.info(BANNER);
        KyuubiWebServer server = new KyuubiWebServer();
        server.init();
        server.start();
    }
}
