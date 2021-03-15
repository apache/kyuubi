package org.apache.kyuubi.web.conf;

import org.apache.kyuubi.common.KyuubiConfigs;
import org.apache.kyuubi.web.KyuubiWebServer;
import org.apache.kyuubi.web.model.KResult;
import org.apache.kyuubi.web.utils.KyuubiWebConf;
import org.apache.kyuubi.web.utils.auth.AuthenticationProviderFactory;
import org.apache.kyuubi.web.utils.auth.PasswdAuthenticationProvider;
import org.apache.commons.codec.binary.Base64;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

public class BasicAuthFilter implements Filter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuthFilter.class);

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpReq = (HttpServletRequest) request;
        KyuubiWebConf conf = KyuubiWebConf.load();

        if (isAuthEnabled(conf)) {
            boolean isLoginPage = httpReq.getRequestURI().startsWith(KyuubiWebServer.REST_VERSION_CONTEXT_PATH + "/rest/login");
            boolean isLogoutPage = httpReq.getRequestURI().startsWith(KyuubiWebServer.REST_VERSION_CONTEXT_PATH + "/rest/logout");
            boolean logged = Optional.ofNullable(httpReq.getSession().getAttribute(KyuubiWebServer.USER_LOGIN_SUCCESS_ATTR))
                    .map(String::valueOf)
                    .map(Boolean::parseBoolean)
                    .orElse(false);
            if (isLoginPage || isLogoutPage || logged) {
                // do nothing
            } else {
                boolean loginByBasicAuth = doBasicLogin(conf, request);
                if (!loginByBasicAuth) {
                    sendAuthenticationFailedResponse(request, response);
                    return;
                }
            }
        }
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {

    }

    public void sendAuthenticationFailedResponse(ServletRequest request, ServletResponse response) throws IOException {
        HttpServletRequest httpReq = (HttpServletRequest) request;
        HttpServletResponse httpRes = (HttpServletResponse) response;
        String accept = httpReq.getHeader("Accept");
        if (Objects.equals("application/json", accept)) {
            httpRes.setStatus(HttpStatus.UNAUTHORIZED_401);
            httpRes.setContentType("application/json");
            Writer writer = httpRes.getWriter();
            KResult data = KResult.failure("Not authenticated to access kyuubi http service");
            writer.write(ObjectMapperProvider.toJson(data));
            writer.flush();
            writer.close();
        } else {
            httpRes.setStatus(HttpStatus.FOUND_302);
            httpRes.setHeader("Location", httpReq.getContextPath() + "/rest/login");
        }
    }

    public boolean doBasicLogin(KyuubiWebConf conf, ServletRequest request) {
        HttpServletRequest req = (HttpServletRequest) request;
        String header = req.getHeader("Authorization");
        if (header != null && header.startsWith("Basic ")) {
            String base64Token = header.substring(6);
            String token = new String(Base64.decodeBase64(base64Token), StandardCharsets.UTF_8);
            int delim = token.indexOf(":");
            if (delim > 0) {
                String username = token.substring(0, delim);
                String password = token.substring(delim + 1);
                String method = conf.get(KyuubiConfigs.AUTHENTICATION_METHOD);

                PasswdAuthenticationProvider authenticationProvider = AuthenticationProviderFactory.getAuthenticationProvider(method, conf);
                try {
                    authenticationProvider.authenticate(username, password);
                    request.setAttribute(KyuubiWebServer.LOGIN_USER_ATTR, username);
                    request.setAttribute(KyuubiWebServer.LOGIN_USER_PASSWORD_ATTR, password);
                    return true;
                } catch (Throwable e) {
                    LOGGER.warn("authenticate failed to login  kyuubi http service", e);
                }
            }
        }
        return false;
    }

    private boolean isAuthEnabled(KyuubiWebConf conf) {
        boolean authEnabled = conf.getBoolean(KyuubiConfigs.KYUUBI_AUDIT_AUTH_ENABLED);
        String method = conf.get(KyuubiConfigs.AUTHENTICATION_METHOD);
        return authEnabled && ("LDAP".equals(method) || "KERBEROS".equals(method));
    }

}
