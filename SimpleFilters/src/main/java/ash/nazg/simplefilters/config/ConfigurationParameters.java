package ash.nazg.simplefilters.config;

import ash.nazg.config.tdl.Description;

public final class ConfigurationParameters {
    @Description("CSV RDD with to be filtered")
    public static final String RDD_INPUT_SOURCE = "source";
    @Description("CSV RDD with values to match any of them")
    public static final String RDD_INPUT_VALUES = "values";
    @Description("Column to match a value")
    public static final String DS_SOURCE_MATCH_COLUMN = "source.match.column";
    @Description("Column with a value to match")
    public static final String DS_VALUES_MATCH_COLUMN = "values.match.column";

    @Description("CSV RDD with matching values")
    public static final String RDD_OUTPUT_MATCHED = "matched";
    @Description("CSV RDD with non-matching values")
    public static final String RDD_OUTPUT_EVICTED = "evicted";
}
