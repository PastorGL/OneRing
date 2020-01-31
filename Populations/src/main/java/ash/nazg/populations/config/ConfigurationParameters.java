/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.config;

import ash.nazg.config.tdl.Description;

public final class ConfigurationParameters {
    @Description("Column with the user ID")
    public static final String DS_SIGNALS_USERID_COLUMN = "signals.userid.column";
    @Description("Column with the silo ID")
    public static final String DS_SIGNALS_SILOS_COLUMN = "signals.silos.column";
    @Description("Column with the year")
    public static final String DS_SIGNALS_YEAR_COLUMN = "signals.year.column";
    @Description("Column with the month")
    public static final String DS_SIGNALS_MONTH_COLUMN = "signals.month.column";
    @Description("Column with the day of the month")
    public static final String DS_SIGNALS_DAY_COLUMN = "signals.day.column";

    @Description("Dates to filter users by, YYYY-MM-DD format")
    public static final String OP_POP_FILTER_DATES = "dates";
    @Description("Minimal number of occurred dates for a user")
    public static final String OP_POP_FILTER_MIN_DAYS = "min.days";
    @Description("Minimal count of signals for a user")
    public static final String OP_POP_FILTER_MIN_SIGNALS = "min.signals";
    @Description("Maximal count of signals for a user")
    public static final String OP_POP_FILTER_MAX_SIGNALS = "max.signals";

    @Description("Source user signals")
    public static final String RDD_INPUT_SIGNALS = "signals";
    @Description("Filtered user signals")
    public static final String RDD_OUTPUT_SIGNALS = "signals";

    @Description("Parametric scores output")
    public static final String RDD_OUTPUT_SCORES = "scores";

    @Description("Values to group and count")
    public static final String RDD_INPUT_VALUES = "values";
    @Description("General population")
    public static final String RDD_INPUT_POPULATION = "population";

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
