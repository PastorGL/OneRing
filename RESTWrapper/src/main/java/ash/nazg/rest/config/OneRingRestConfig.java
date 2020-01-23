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
