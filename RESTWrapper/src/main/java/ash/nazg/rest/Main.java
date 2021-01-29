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
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.wadl.WadlFeature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static ash.nazg.cli.TaskWrapper.APP_NAME;
import static io.github.pastorgl.rest.init.GlobalConfig.PROPERTY_SERVER_INTERFACE;
import static io.github.pastorgl.rest.init.GlobalConfig.PROPERTY_SERVER_PORT;

public class Main {
    public static void main(String... args) throws Exception {
        String appPackage = Main.class.getPackage().getName();

        ResourceConfig resourceConfig = new ResourceConfig()
                .register(new WadlFeature())
                .property(ServerProperties.PROVIDER_PACKAGES, appPackage)
                .property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);

        OneRingRestConfig config = new OneRingRestConfig();

        try {
            config.parse(args);
        } catch (ParseException | IOException e) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp(APP_NAME, config.getOptions());
            System.exit(1);
            return;
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