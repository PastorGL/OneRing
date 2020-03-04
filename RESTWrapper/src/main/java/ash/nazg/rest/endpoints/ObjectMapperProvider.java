package ash.nazg.rest.endpoints;

import ash.nazg.config.tdl.TDLObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {
    private final ObjectMapper mapper;

    public ObjectMapperProvider() {
        this.mapper = new TDLObjectMapper();
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }
}