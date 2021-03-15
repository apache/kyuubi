package org.apache.kyuubi.web.conf;

import org.glassfish.jersey.server.mvc.Viewable;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.Enumeration;
import java.util.HashMap;

public class ViewModel extends Viewable {
    public ViewModel(String templateName) throws IllegalArgumentException {
        super(templateName, new HashMap<String, Object>());
    }

    public ViewModel(String templateName, HttpServletRequest request, HttpServletResponse response) throws IllegalArgumentException {
        super(templateName, new HashMap<String, Object>());
        attr("_request", request);
        attr("_response", response);
    }

    public ViewModel attr(String name, Object value) {
        HashMap<String, Object> model = (HashMap<String, Object>) getModel();
        model.put(name, value);
        return this;
    }

    public ViewModel attr(HttpServletRequest request) {
        Enumeration<String> parameterNames = request.getParameterNames();
        while (parameterNames.hasMoreElements()) {
            String name = parameterNames.nextElement();
            attr("request." + name, request.getParameter(name));
        }

        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String name = headerNames.nextElement();
            attr("header." + name, request.getHeader(name));
        }

        HttpSession session = request.getSession();
        Enumeration<String> attributeNames = session.getAttributeNames();
        while (attributeNames.hasMoreElements()) {
            String name = attributeNames.nextElement();
            attr("session." + name, session.getAttribute(name));
        }

        return this;
    }

    public ViewModel attr(HttpSession session) {
        Enumeration<String> attributeNames = session.getAttributeNames();
        while (attributeNames.hasMoreElements()) {
            String name = attributeNames.nextElement();
            attr("session." + name, session.getAttribute(name));
        }

        return this;
    }
}
