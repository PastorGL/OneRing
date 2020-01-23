package ash.nazg.dist;

import ash.nazg.cli.config.TaskWrapperConfig;
import ash.nazg.dist.config.DistWrapperConfig;
import ash.nazg.dist.config.DistWrapperConfigBuilder;

public class Main {
    public static void main(String[] args) {
        DistWrapperConfigBuilder configBuilder = new DistWrapperConfigBuilder();

        try {
            configBuilder.addRequiredOption("c", "config", true, "Config file");
            configBuilder.addOption("d", "direction", true, "Copy direction. Can be 'from', 'to', or 'nop' to just validate the config file and exit");
            configBuilder.addOption("o", "output", true, "Path to output distcp.ini");

            configBuilder.setCommandLine(args);

            DistWrapperConfig config = configBuilder.build();
            configBuilder.overrideFromCommandLine(DistWrapperConfig.DISTCP_INI_PATH, "o");
            configBuilder.overrideFromCommandLine(DistWrapperConfig.DISTCP_DIRECTION, "d");
            configBuilder.overrideFromCommandLine(TaskWrapperConfig.META_DISTCP_STORE_PATH, "S");

            new DistWrapper(config)
                    .go();
        } catch (Exception e) {
            e.printStackTrace();

            System.exit(1);
        }
    }
}
