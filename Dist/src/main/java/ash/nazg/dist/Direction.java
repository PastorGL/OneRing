/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

public enum Direction {
    NOP(false, false),
    TO_CLUSTER(true, false),
    FROM_CLUSTER(false, true),
    BOTH_DIRECTIONS(true, true);

    public final boolean toCluster;
    public final boolean fromCluster;
    public final boolean anyDirection;

    Direction(boolean toCluster, boolean fromCluster) {
        this.toCluster = toCluster;
        this.fromCluster = fromCluster;
        this.anyDirection = toCluster || fromCluster;
    }

    public static Direction parse(Object v) {
        switch (String.valueOf(v).toLowerCase()) {
            case "result" :
            case "from": {
                return FROM_CLUSTER;
            }
            case "source" :
            case "to": {
                return TO_CLUSTER;
            }
            case "true":
            case "both": {
                return BOTH_DIRECTIONS;
            }
            default: {
                return NOP;
            }
        }
    }
}
