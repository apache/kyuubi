package com.dropbox;

import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;

public class DummyPasswdAuthenticationProvider implements PasswdAuthenticationProvider {
    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
        if (!user.equals("the-user") || !password.equals("p4ssw0rd")) {
            throw new AuthenticationException();
        }
    }
}
