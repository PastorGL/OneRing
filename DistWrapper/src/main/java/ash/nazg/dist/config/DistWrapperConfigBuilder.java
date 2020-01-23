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
