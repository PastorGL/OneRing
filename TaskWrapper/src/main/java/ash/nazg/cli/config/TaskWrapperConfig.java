/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.cli.config;

import ash.nazg.config.WrapperConfig;

public class TaskWrapperConfig extends WrapperConfig {
    public static final String META_DISTCP_WRAP = "distcp.wrap";

    public static final String META_DISTCP_TO_DIR = "distcp.dir.to";
    public static final String META_DISTCP_FROM_DIR = "distcp.dir.from";

    public static final String META_DISTCP_STORE_PATH = "distcp.store";

    private CpDirection cpDirection;

    public CpDirection getCpDirection() {
        if (cpDirection == null) {
            cpDirection = CpDirection.parse(getProperty(META_DISTCP_WRAP, "none"));
        }
        return cpDirection;
    }

    public String getCpToDir() {
        return getProperty(META_DISTCP_TO_DIR, "/input");
    }

    public String getCpFromDir() {
        return getProperty(META_DISTCP_FROM_DIR, "/output");
    }

    public String getWrapperStorePath() {
        return getProperty(META_DISTCP_STORE_PATH);
    }
}
