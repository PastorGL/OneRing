/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.cli.config;

public enum CpDirection {
    NOP(false, false),
    ONLY_TO_HDFS(true, false),
    ONLY_FROM_HDFS(false, true),
    BOTH_DIRECTIONS(true, true);

    public final boolean to;
    public final boolean from;
    public final boolean any;

    CpDirection(boolean to, boolean from) {
        this.to = to;
        this.from = from;
        this.any = to || from;
    }

    public static CpDirection parse(Object v) {
        switch (String.valueOf(v).toLowerCase()) {
            case "from" : {
                return ONLY_FROM_HDFS;
            }
            case "to" : {
                return ONLY_TO_HDFS;
            }
            case "true" :
            case "both" : {
                return BOTH_DIRECTIONS;
            }
            default : {
                return NOP;
            }
        }
    }
}
