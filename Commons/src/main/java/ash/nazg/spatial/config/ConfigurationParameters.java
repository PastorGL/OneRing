/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.config;

import ash.nazg.config.tdl.Description;

public final class ConfigurationParameters {
    @Description("If set, generated Points will have this value in the _radius parameter")
    public static final String OP_DEFAULT_RADIUS = "radius.default";

    @Description("If set, generated Points will take their _radius parameter from the specified column")
    public static final String DS_CSV_RADIUS_COLUMN = "radius.column";
    @Description("Point latitude column")
    public static final String DS_CSV_LAT_COLUMN = "lat.column";
    @Description("Point longitude column")
    public static final String DS_CSV_LON_COLUMN = "lon.column";
    @Description("H3 hash column")
    public static final String DS_CSV_HASH_COLUMN = "hash.column";

    @Description("Point latitude")
    public static final String GEN_CENTER_LAT = "_center_lat";
    @Description("Point longitude")
    public static final String GEN_CENTER_LON = "_center_lon";
    @Description("Point radius attribute")
    public static final String GEN_RADIUS = "_radius";
    @Description("Column with a generated hash value")
    public static final String GEN_HASH = "_hash";
}
