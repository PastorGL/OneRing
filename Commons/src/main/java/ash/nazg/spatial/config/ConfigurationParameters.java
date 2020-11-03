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
    @Description("Point time stamp column")
    public static final String DS_CSV_TIMESTAMP_COLUMN = "ts.column";
    @Description("H3 hash column")
    public static final String DS_CSV_HASH_COLUMN = "hash.column";
    @Description("Point User ID column")
    public static final String DS_CSV_USERID_COLUMN = "userid.column";
    @Description("Point track segment ID column")
    public static final String DS_CSV_TRACKID_COLUMN = "trackid.column";

    @Description("Point latitude")
    public static final String GEN_CENTER_LAT = "_center_lat";
    @Description("Point longitude")
    public static final String GEN_CENTER_LON = "_center_lon";
    @Description("Point radius attribute")
    public static final String GEN_RADIUS = "_radius";
    @Description("Column with a generated hash value")
    public static final String GEN_HASH = "_hash";

    @Description("Polygon area")
    public static final String GEN_AREA = "_area";
    @Description("Polygon perimeter")
    public static final String GEN_PERIMETER = "_perimeter";

    @Description("Track and/or track segments output")
    public static final String RDD_OUTPUT_TRACKS = "tracks";
    @Description("Point data output")
    public static final String RDD_OUTPUT_POINTS = "points";

    @Description("Track or segment duration, seconds")
    public static final String GEN_DURATION = "_duration";
    @Description("Track or segment length, meters")
    public static final String GEN_DISTANCE = "_distance";
    @Description("Track or segment range from the base point, meters")
    public static final String GEN_RANGE = "_range";
    @Description("Number of track or segment points")
    public static final String GEN_POINTS = "_points";
    @Description("Track User ID")
    public static final String GEN_USERID = "_userid";
    @Description("Track segment ID (for segmented tracks)")
    public static final String GEN_TRACKID = "_trackid";
}
