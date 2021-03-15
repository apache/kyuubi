package org.apache.kyuubi.web.conf;

import org.glassfish.jersey.server.mvc.MvcFeature;

import javax.ws.rs.ConstrainedTo;
import javax.ws.rs.RuntimeType;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

@ConstrainedTo(RuntimeType.SERVER)
public class ThymeleafMvcFeature implements Feature {
    @Override
    public boolean configure(FeatureContext context) {
        Configuration configuration = context.getConfiguration();
        if (!configuration.isRegistered(ThymeleafTemplateProcessor.class)) {
            context.register(ThymeleafTemplateProcessor.class);
            if (!configuration.isRegistered(MvcFeature.class)) {
                context.register(MvcFeature.class);
            }
            return true;
        }
        return false;
    }
}
