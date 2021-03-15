package org.apache.kyuubi.web.conf;

import org.apache.kyuubi.web.utils.KyuubiWebException;
import org.glassfish.jersey.server.mvc.Viewable;
import org.glassfish.jersey.server.mvc.spi.AbstractTemplateProcessor;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ServletContextTemplateResolver;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class ThymeleafTemplateProcessor extends AbstractTemplateProcessor<String> {
    private ServletContextTemplateResolver templateResolver;
    private TemplateEngine templateEngine;
    private ServletContext servletContext;


    @Inject
    public ThymeleafTemplateProcessor(Configuration config, ServletContext servletContext) {
        super(config, servletContext, "thymeleaf", ".html");
        this.servletContext = servletContext;

        templateResolver = new ServletContextTemplateResolver(servletContext);
        templateResolver.setTemplateMode(TemplateMode.HTML);
        templateResolver.setPrefix("");
        templateResolver.setCacheTTLMs(3600000L);
        templateResolver.setSuffix(".html");

        templateEngine = new TemplateEngine();
        templateEngine.setTemplateResolver(templateResolver);

    }

    @Override
    protected String resolve(String templateReference, Reader reader) throws Exception {
        return templateReference;
    }


    @Override
    public void writeTo(String template, Viewable viewable, MediaType mediaType,
                        MultivaluedMap<String, Object> httpHeaders, OutputStream out) throws IOException {
        try {
            Object model = viewable.getModel();
            if (!(model instanceof Map)) {
                model = new HashMap<String, Object>() {{
                    put("model", viewable.getModel());
                }};
            }
            Charset encoding = setContentType(mediaType, httpHeaders);

            Map<String, Object> modelMap = (Map<String, Object>) model;
            HttpServletRequest request = (HttpServletRequest) modelMap.get("_request");
            HttpServletResponse response = (HttpServletResponse) modelMap.get("_response");

            WebContext webContext = new WebContext(request, response, servletContext, Locale.getDefault(), modelMap);
            templateEngine.process(template, webContext, new OutputStreamWriter(out, encoding));
        } catch (Exception te) {
            throw new KyuubiWebException(te);
        }
    }
}
