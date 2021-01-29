/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.proximity.config;

import ash.nazg.config.tdl.Description;

public final class ConfigurationParameters {
    @Description("Source Point RDD")
    public static final String RDD_INPUT_SIGNALS = "signals";
    @Description("Source Polygon RDD")
    public static final String RDD_INPUT_GEOMETRIES = "geometries";
    @Description("Source POI Point RDD with _radius attribute set")
    public static final String RDD_INPUT_POIS = "pois";

    @Description("This flag suppresses creation of copies of a signal for each proximal geometry. If set to 'true'," +
            " properties of the source signal will be unchanged. Otherwise a copy of source signal will be created" +
            " for each proximal geometry, and their properties will be augmented with properties of that geometry")
    public static final String OP_ENCOUNTER_ONCE = "encounter.once";

    @Description("Output Point RDD with target signals")
    public static final String RDD_OUTPUT_SIGNALS = "signals";
    @Description("Optional output Point RDD with evicted signals")
    public static final String RDD_OUTPUT_EVICTED = "signals_evicted";
}
