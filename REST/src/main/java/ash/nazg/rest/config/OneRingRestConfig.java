/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.config;

import io.github.pastorgl.rest.init.GlobalConfig;

public class OneRingRestConfig extends GlobalConfig {
    public OneRingRestConfig() {
        super(false, false);
    }

    @Override
    protected String defaultConfigFile() {
        return "/etc/one-ring/one-ring-rest.conf";
    }

    @Override
    protected int defaultPort() {
        return 9996;
    }
}
