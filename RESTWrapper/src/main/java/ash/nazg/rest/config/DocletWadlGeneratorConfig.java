package ash.nazg.rest.config;

import org.glassfish.jersey.server.wadl.config.WadlGeneratorConfig;
import org.glassfish.jersey.server.wadl.config.WadlGeneratorDescription;
import org.glassfish.jersey.server.wadl.internal.generators.resourcedoc.WadlGeneratorResourceDocSupport;

import java.util.List;

public class DocletWadlGeneratorConfig extends WadlGeneratorConfig {
    public List<WadlGeneratorDescription> configure() {
        return generator(WadlGeneratorResourceDocSupport.class)
                .prop("resourceDocStream", "resourcedoc.xml")
                .descriptions();
    }
}