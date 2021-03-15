package org.apache.kyuubi.web.conf;

import org.glassfish.jersey.server.ResourceConfig;

public class ViewApplicationConfig extends ResourceConfig {

    public ViewApplicationConfig() {
        packages("org.apache.kyuubi.web");
        property("jersey.config.server.mvc.templateBasePath.thymeleaf", "/views");
        register(ThymeleafMvcFeature.class);

        AuditCleaner cleaner = new AuditCleaner();
        cleaner.start();
    }
}
