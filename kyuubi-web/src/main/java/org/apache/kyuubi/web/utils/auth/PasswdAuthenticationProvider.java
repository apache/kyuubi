package org.apache.kyuubi.web.utils.auth;

public interface PasswdAuthenticationProvider {

    void authenticate(String user, String password);
}
