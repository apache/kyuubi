package org.apache.kyuubi.web.utils;

import org.apache.kyuubi.common.KyuubiConfigs;
import org.apache.kyuubi.web.KyuubiWebServer;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;

public class ServletUtils {

    public static boolean isAdmin(HttpServletRequest req) {
        String loginUser = (String) req.getSession().getAttribute(KyuubiWebServer.LOGIN_USER_ATTR);
        String[] adminUsers = KyuubiWebConf.load().get(KyuubiConfigs.KYUUBI_AUDIT_ADMIN).split(",");
        boolean isAdmin = Arrays.asList(adminUsers).contains(loginUser);
        return isAdmin;
    }
}
