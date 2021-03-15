package org.apache.kyuubi.web.utils.auth;

import org.apache.kyuubi.web.utils.KyuubiWebConf;

public class AuthenticationProviderFactory {
    public static PasswdAuthenticationProvider getAuthenticationProvider(String method,
                                                                         KyuubiWebConf conf) {
        switch (method.toLowerCase()) {
            case "ldap":
                return new LdapAuthenticationProviderImpl(conf);
            default:
                return new AnonymousAuthenticationProviderImpl();

        }
    }
}
