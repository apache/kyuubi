package org.apache.kyuubi.web.utils.auth;

import org.apache.kyuubi.common.KyuubiConfigs;
import org.apache.kyuubi.common.utils.KyuubiUtils;
import org.apache.kyuubi.web.utils.KyuubiWebConf;
import org.apache.kyuubi.web.utils.KyuubiWebException;
import org.apache.commons.lang3.StringUtils;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;


public class LdapAuthenticationProviderImpl implements PasswdAuthenticationProvider {
    private KyuubiWebConf conf;

    public LdapAuthenticationProviderImpl(KyuubiWebConf conf) {
        this.conf = conf;
    }

    /**
     * The authenticate method is called by the kyuubi Server authentication layer
     * to authenticate users for their requests.
     * If a user is to be granted, return nothing/throw nothing.
     * When a user is to be disallowed, throw an appropriate [[AuthenticationException]].
     *
     * @param user     The username received over the connection request
     * @param password The password received over the connection request
     */
    @Override
    public void authenticate(String user, String password) {
        if (StringUtils.isBlank(user)) {
            throw new KyuubiWebException(
                    "Error validating LDAP user, user is null or contains blank space");
        }

        if (StringUtils.isBlank(password)) {
            throw new KyuubiWebException(
                    "Error validating LDAP user, password is null or contains blank space");
        }

        Map<String, String> env = new HashMap<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");

        env.put(Context.PROVIDER_URL, conf.get(KyuubiConfigs.AUTHENTICATION_LDAP_URL));

        String domain = conf.get(KyuubiConfigs.AUTHENTICATION_LDAP_DOMAIN);
        String u;
        if (!hasDomain(user) && StringUtils.isNotBlank(domain)) {
            u = user + "@" + domain;
        } else {
            u = user;
        }
        String baseDn = conf.get(KyuubiConfigs.AUTHENTICATION_LDAP_BASEDN);
        String bindDn = StringUtils.isBlank(baseDn) ? u : "uid=" + u + "," + baseDn;

        env.put(Context.SECURITY_PRINCIPAL, bindDn);
        env.put(Context.SECURITY_CREDENTIALS, password);

        try {
            InitialDirContext ctx = new InitialDirContext(new Hashtable<>(env));
            ctx.close();
        } catch (NamingException e) {
            throw new KyuubiWebException("Can not open LDAP service", e);
        }
    }

    private boolean hasDomain(String userName) {
        return KyuubiUtils.indexOfDomainMatch(userName) > 0;
    }
}
