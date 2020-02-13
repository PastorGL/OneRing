/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import ash.nazg.cli.config.TaskWrapperConfigBuilder;
import ash.nazg.config.WrapperConfig;

public class Dist {
    public static void main(String[] args) {
        TaskWrapperConfigBuilder configBuilder = new TaskWrapperConfigBuilder();

        try {
            configBuilder.addRequiredOption("c", "config", true, "Config file");
            configBuilder.addOption("d", "direction", true, "Copy direction. Can be 'from', 'to', or 'nop' to just validate the config file and exit");
            configBuilder.addOption("o", "output", true, "Path to output distcp.ini");
            configBuilder.addOption("S", "wrapperStorePath", true, "Path to DistWrapper interface file");

            configBuilder.setCommandLine(args);

            WrapperConfig config = configBuilder.build(null);
            configBuilder.overrideFromCommandLine("distcp.ini", "o");
            configBuilder.overrideFromCommandLine("distcp.wrap", "d");
            configBuilder.overrideFromCommandLine("distcp.store", "S");

            new DistWrapper(config)
                    .go();
        } catch (Exception e) {
            e.printStackTrace();

            System.exit(1);
        }
    }
}
