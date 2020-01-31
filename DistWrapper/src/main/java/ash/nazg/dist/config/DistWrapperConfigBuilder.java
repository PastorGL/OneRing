/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist.config;

import ash.nazg.cli.config.TaskWrapperConfigBuilder;

public class DistWrapperConfigBuilder extends TaskWrapperConfigBuilder {
    public DistWrapperConfig build() {
        super.build(null);

        DistWrapperConfig thisConfig = new DistWrapperConfig();
        thisConfig.setProperties(wrapperConfig.getProperties());

        wrapperConfig = thisConfig;

        return (DistWrapperConfig) wrapperConfig;
    }
}
