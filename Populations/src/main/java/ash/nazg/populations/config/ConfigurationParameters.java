/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.config;

import ash.nazg.config.tdl.Description;

public final class ConfigurationParameters {
    @Description("Target audience signals, f sub-population of base audience signals")
    public static final String RDD_INPUT_TARGET = "target_signals";
    @Description("Target audience signals user ID column")
    public static final String DS_TARGET_USERID_COLUMN = "target_signals.userid.column";
    @Description("Target audience signals grid cell ID column")
    public static final String DS_TARGET_GID_COLUMN = "target_signals.gid.column";

    @Description("Source user signals")
    public static final String RDD_INPUT_SIGNALS = "signals";
    @Description("Column with the user ID")
    public static final String DS_SIGNALS_USERID_COLUMN = "signals.userid.column";

    @Description("Parametric scores output")
    public static final String RDD_OUTPUT_SCORES = "scores";

    @Description("Values to group and count")
    public static final String RDD_INPUT_VALUES = "values";

    @Description("Column to count unique values of other column")
    public static final String DS_COUNT_COLUMN = "count.column";
    @Description("Column for counting unique values per other column")
    public static final String DS_VALUE_COLUMN = "value.column";

    @Description("Column to count unique values of other column")
    public static final String DS_VALUES_COUNT_COLUMN = "values.count.column";
    @Description("Column for counting unique values per other column")
    public static final String DS_VALUES_VALUE_COLUMN = "values.value.column";
    @Description("Column for grouping count columns per value column values")
    public static final String DS_VALUES_GROUP_COLUMN = "values.group.column";
}
