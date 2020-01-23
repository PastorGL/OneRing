package ash.nazg.proximity.config;

import ash.nazg.config.tdl.Description;

public final class ConfigurationParameters {
    @Description("Source Point RDD")
    public static final String RDD_INPUT_SIGNALS = "signals";
    @Description("Source Polygon RDD")
    public static final String RDD_INPUT_GEOMETRIES = "geometries";
    @Description("Source POI Point RDD with _radius attribute set")
    public static final String RDD_INPUT_POIS = "pois";

    @Description("Output Point RDD")
    public static final String RDD_OUTPUT_SIGNALS = "signals";
}
