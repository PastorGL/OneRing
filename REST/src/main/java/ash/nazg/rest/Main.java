/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest;

import ash.nazg.rest.config.OneRingRestConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Module;
import io.logz.guice.jersey.JerseyModule;
import io.logz.guice.jersey.JerseyServer;
import io.logz.guice.jersey.configuration.JerseyConfiguration;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.wadl.WadlFeature;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.github.pastorgl.rest.init.GlobalConfig.PROPERTY_SERVER_INTERFACE;
import static io.github.pastorgl.rest.init.GlobalConfig.PROPERTY_SERVER_PORT;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String... args) throws Exception {
        String appPackage = Main.class.getPackage().getName();

        ResourceConfig resourceConfig = new ResourceConfig()
                .register(new WadlFeature())
                .property(ServerProperties.PROVIDER_PACKAGES, appPackage)
                .property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);

        OneRingRestConfig config = new OneRingRestConfig();

        try {
            config.parse(args);
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                new HelpFormatter().printHelp("One Ring REST", config.getOptions());
            } else {
                LOG.error(ex.getMessage(), ex);
            }

            System.exit(1);
        }

        Properties properties = config.getProperties();
        JerseyConfiguration configuration = JerseyConfiguration.builder()
                .withResourceConfig(resourceConfig)
                .addPackage(appPackage)
                .addHost(properties.getProperty(PROPERTY_SERVER_INTERFACE), Integer.parseInt(properties.getProperty(PROPERTY_SERVER_PORT)))
                .build();

        List<Module> modules = new ArrayList<>();
        modules.add(new JerseyModule(configuration));
        modules.add(new AbstractModule() {
            @Override
            protected void configure() {
                bind(Properties.class).toInstance(config.getProperties());
            }
        });

        Guice.createInjector(modules)
                .getInstance(JerseyServer.class).start();
    }
}