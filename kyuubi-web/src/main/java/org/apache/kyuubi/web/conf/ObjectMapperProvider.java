package org.apache.kyuubi.web.conf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ObjectMapper getContext(Class<?> aClass) {
        return mapper;
    }

    public static String toJson(Object anyRef) {
        if (anyRef == null) {
            return null;
        }
        try {
            return mapper.writeValueAsString(anyRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error parse object[" + anyRef.toString() + "] to json.", e);
        }
    }
}