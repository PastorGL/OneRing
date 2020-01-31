/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist.config;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.cli.config.CpDirection;
import ash.nazg.cli.config.TaskWrapperConfig;

public class DistWrapperConfig extends TaskWrapperConfig {
    public static final String DISTCP_EXE = "distcp.exe";

    public static final String DISTCP_DIRECTION = "distcp.direction";
    public static final String DISTCP_MOVE = "distcp.move";

    public static final String DISTCP_INI_PATH = "distcp.ini";

    public String getDistCpExe() {
        return getProperty(DISTCP_EXE, "s3-dist-cp");
    }

    public String getDistCpIni() {
        return getProperty(DISTCP_INI_PATH, null);
    }

    /**
     * Can be set only if CpDirection.from is true, and is true by default for these modes.
     */
    public boolean getDistCpMove() throws InvalidConfigValueException {
        if (getDistDirection() == CpDirection.ONLY_FROM_HDFS) {
            return Boolean.parseBoolean(getProperty(DISTCP_MOVE, "true"));
        }

        return false;
    }

    public CpDirection getDistDirection() throws InvalidConfigValueException {
        CpDirection direction = CpDirection.parse(getProperty(DISTCP_DIRECTION, "nop"));
        if (direction == CpDirection.BOTH_DIRECTIONS) {
            throw new InvalidConfigValueException("DistWrapper's copy direction can't be 'both' because it's ambiguous");
        }
        return direction;
    }
}
