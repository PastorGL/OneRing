package ash.nazg.composer.config;

import ash.nazg.cli.config.WrapperConfigBuilder;
import ash.nazg.config.WrapperConfig;
import org.apache.commons.cli.Options;

import java.util.Properties;

public class ComposeWrapperConfigBuilder extends WrapperConfigBuilder {
    public ComposeWrapperConfigBuilder() {
        options = new Options();
    }

    public WrapperConfig build(String prefix, String config) {
        wrapperConfig = new WrapperConfig();

        wrapperConfig.setPrefix(prefix);

        Properties ini = loadConfig(config, null, prefix);
        wrapperConfig.setProperties(ini);

        return wrapperConfig;
    }
}
