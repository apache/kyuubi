package org.apache.kyuubi.web.utils;

import org.apache.kyuubi.common.KyuubiConfigs;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

public class DaoProxyGenerator {


    public static <T> T getInstance(Class<T> clz) {
        KyuubiWebConf load = KyuubiWebConf.load();
        if (load.getBoolean(KyuubiConfigs.KYUUBI_AUDIT_ENABLED)) {
            try {
                return clz.newInstance();
            } catch (Exception e) {
                throw new KyuubiWebException("Can not create DAO instance: " + clz.getName(), e);
            }
        } else {
            Enhancer enhancer = new Enhancer();
            enhancer.setSuperclass(clz);
            enhancer.setCallback(new DAOMethodInterceptor());
            return (T) enhancer.create();
        }
    }

    static class DAOMethodInterceptor implements MethodInterceptor {

        @Override
        public Object intercept(Object obj, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
            return null;
        }
    }
}
