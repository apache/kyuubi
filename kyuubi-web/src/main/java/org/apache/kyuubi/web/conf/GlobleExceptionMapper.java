package org.apache.kyuubi.web.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class GlobleExceptionMapper implements ExceptionMapper<Exception> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobleExceptionMapper.class);

    @Override
    public Response toResponse(Exception e) {
        LOGGER.warn("kyuubi Web Server Error: " + e.getMessage(), e);
        return Response
                .status(Response.Status.OK)
                .entity("kyuubi Server Error")
                .type(MediaType.APPLICATION_JSON)
                .build();
    }
}
