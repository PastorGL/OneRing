/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

public abstract class TestBase {
    static Injector injector;

    @BeforeClass
    static public void setup() throws Exception {
        List<Module> modules = new ArrayList<>();
        modules.add(new AbstractModule() {
            @Override
            protected void configure() {

            }
        });

        injector = Guice.createInjector(modules);
    }

    @AfterClass
    static public void teardown() {
        injector = null;
    }
}