/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
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