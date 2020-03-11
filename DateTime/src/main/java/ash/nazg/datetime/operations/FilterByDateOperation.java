/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.datetime.functions.FilterByDateDefinition;
import ash.nazg.datetime.functions.FilterByDateFunction;
import ash.nazg.spark.Operation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class FilterByDateOperation extends Operation {
    @Description("By default do not filter by date of month")
    public static final String DEF_DATE_COLUMN = null;
    @Description("By default do not filter by year")
    public static final String DEF_YEAR_COLUMN = null;
    @Description("By default do not filter by day of week")
    public static final String DEF_DOW_COLUMN = null;
    @Description("By default do not filter by month")
    public static final String DEF_MONTH_COLUMN = null;
    @Description("By default do not filter by hour")
    public static final String DEF_HOUR_COLUMN = null;
    @Description("By default do not filter by minute")
    public static final String DEF_MINUTE_COLUMN = null;
    @Description("By default do not filter by date range start")
    public static final String DEF_START = null;
    @Description("By default do not filter by date range end")
    public static final String DEF_END = null;
    @Description("By default do not filter by date of month")
    public static final String[] DEF_DATE_VALUE = null;
    @Description("By default do not filter by year")
    public static final String[] DEF_YEAR_VALUE = null;
    @Description("By default do not filter by day of week")
    public static final String[] DEF_DOW_VALUE = null;
    @Description("By default do not filter by month")
    public static final String[] DEF_MONTH_VALUE = null;
    @Description("By default do not filter by starting time of day")
    public static final Integer DEF_HHMM_START = null;
    @Description("By default do not filter by ending time of day")
    public static final Integer DEF_HHMM_END = null;
    @Description("Column with date of month")
    public static final String DS_DATE_COLUMN = "date.column";
    @Description("Column with year")
    public static final String DS_YEAR_COLUMN = "year.column";
    @Description("Column with day of week")
    public static final String DS_DOW_COLUMN = "dow.column";
    @Description("Column with month")
    public static final String DS_MONTH_COLUMN = "month.column";
    @Description("Column with hour")
    public static final String DS_HOUR_COLUMN = "hour.column";
    @Description("Column with minute")
    public static final String DS_MINUTE_COLUMN = "minute.column";
    @Description("Start of the range filter")
    public static final String OP_START = "start";
    @Description("End of the range filter")
    public static final String OP_END = "end";
    @Description("List of date filter values")
    public static final String OP_DATE_VALUE = "date.value";
    @Description("List of year filter values")
    public static final String OP_YEAR_VALUE = "year.value";
    @Description("List of day of week filter values")
    public static final String OP_DOW_VALUE = "dow.value";
    @Description("List of month filter values")
    public static final String OP_MONTH_VALUE = "month.value";
    @Description("Starting time of day, exclusive, in HHMM format, in the range of 0000 to 2359")
    public static final String OP_HHMM_START = "hhmm.start";
    @Description("Ending time of day, inclusive, in HHMM format, in the range of 0000 to 2359")
    public static final String OP_HHMM_END = "hhmm.end";

    public static final String VERB = "filterByDate";

    private String inputName;
    private String outputName;

    private FilterByDateDefinition def;

    @Override
    @Description("Filter a CSV RDD by exploded timestamp field values (year, month, day of month, day of week)" +
            " and optionally full date and/or time of day (hours and minutes) range. Multiple filter values are supported, and all fields are optional")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_DATE_COLUMN, DEF_DATE_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_YEAR_COLUMN, DEF_YEAR_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_DOW_COLUMN, DEF_DOW_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_MONTH_COLUMN, DEF_MONTH_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_HOUR_COLUMN, DEF_HOUR_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_MINUTE_COLUMN, DEF_MINUTE_COLUMN),
                        new TaskDescriptionLanguage.Definition(OP_START, DEF_START),
                        new TaskDescriptionLanguage.Definition(OP_END, DEF_END),
                        new TaskDescriptionLanguage.Definition(OP_DATE_VALUE, DEF_DATE_VALUE),
                        new TaskDescriptionLanguage.Definition(OP_YEAR_VALUE, DEF_YEAR_VALUE),
                        new TaskDescriptionLanguage.Definition(OP_DOW_VALUE, DEF_DOW_VALUE),
                        new TaskDescriptionLanguage.Definition(OP_MONTH_VALUE, DEF_MONTH_VALUE),
                        new TaskDescriptionLanguage.Definition(OP_HHMM_START, Integer.class, DEF_HHMM_START),
                        new TaskDescriptionLanguage.Definition(OP_HHMM_END, Integer.class, DEF_HHMM_END),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Passthru},
                                false
                        )
                )
        );
    }

    @Override
    public void setConfig(OperationConfig propertiesConfig) throws InvalidConfigValueException {
        super.setConfig(propertiesConfig);

        def = new FilterByDateDefinition();

        inputName = describedProps.inputs.get(0);
        def.inputDelimiter = dataStreamsProps.inputDelimiter(inputName);
        outputName = describedProps.outputs.get(0);
        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);

        boolean filteringNeeded = false;

        String prop = describedProps.defs.getTyped(DS_DATE_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.dateCol = inputColumns.get(prop);
        }
        prop = describedProps.defs.getTyped(DS_YEAR_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.yearCol = inputColumns.get(prop);
        }
        prop = describedProps.defs.getTyped(DS_DOW_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.dowCol = inputColumns.get(prop);
        }
        prop = describedProps.defs.getTyped(DS_MONTH_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.monthCol = inputColumns.get(prop);
        }
        prop = describedProps.defs.getTyped(DS_HOUR_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.hourCol = inputColumns.get(prop);
        }
        prop = describedProps.defs.getTyped(DS_MINUTE_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.minuteCol = inputColumns.get(prop);
        }

        prop = describedProps.defs.getTyped(OP_START);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.start = FilterByDateDefinition.parseDate(prop);
        }
        prop = describedProps.defs.getTyped(OP_END);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.end = FilterByDateDefinition.parseDate(prop);
        }

        String[] arr = describedProps.defs.getTyped(OP_DATE_VALUE);
        if (arr != null) {
            filteringNeeded = true;
            def.dates = integers(arr, 1, 32);
        }
        arr = describedProps.defs.getTyped(OP_YEAR_VALUE);
        if (arr != null) {
            filteringNeeded = true;
            Calendar c = Calendar.getInstance();
            Integer lower = null, upper = null;
            if (def.start != null) {
                c.setTime(def.start);
                lower = c.get(Calendar.YEAR);
            }
            if (def.end != null) {
                c.setTime(def.end);
                upper = c.get(Calendar.YEAR) + 1;
            }

            def.years = integers(arr, lower, upper);
        }
        arr = describedProps.defs.getTyped(OP_DOW_VALUE);
        if (arr != null) {
            filteringNeeded = true;
            def.dows = integers(arr, 1, 8);
        }
        arr = describedProps.defs.getTyped(OP_MONTH_VALUE);
        if (arr != null) {
            filteringNeeded = true;
            def.months = integers(arr, 1, 13);
        }

        Integer hhmm = describedProps.defs.getTyped(OP_HHMM_START);
        if (hhmm != null) {
            if (hhmm < 0 || hhmm > 2359) {
                throw new InvalidConfigValueException("Filter by starting time of day for the operation '" + name + "' exceeds the allowed range of 0000 to 2359");
            }
            filteringNeeded = true;
            def.startHHMM = hhmm;
        }
        hhmm = describedProps.defs.getTyped(OP_HHMM_END);
        if (hhmm != null) {
            if (hhmm < 0 || hhmm > 2359) {
                throw new InvalidConfigValueException("Filter by ending time of day for the operation '" + name + "' exceeds the allowed range of 0000 to 2359");
            }
            filteringNeeded = true;
            def.endHHMM = hhmm;
        }

        if (!filteringNeeded) {
            throw new InvalidConfigValueException("Filter by date was not configured for the operation '" + name + "'");
        }
    }

    private Integer[] integers(String[] intColl, Integer lower, Integer upper) {
        if (intColl == null) {
            return null;
        }

        if (intColl.length > 0) {
            List<Integer> l = Arrays.stream(intColl).map((String y) -> {
                try {
                    return new Integer(String.valueOf(y).trim());
                } catch (NumberFormatException e) {
                    return -1;
                }
            }).distinct().filter(y -> {
                boolean fit = y > 0;

                if (upper != null) {
                    fit = fit && (upper > y);
                }
                if (lower != null) {
                    fit = fit && (lower <= y);
                }

                return fit;
            }).collect(Collectors.toList());
            if (l.size() == 0) {
                return null;
            }

            return l.toArray(new Integer[0]);
        }

        return null;
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        @SuppressWarnings("unchecked")
        JavaRDD output = input.get(inputName)
                .mapPartitions(new FilterByDateFunction(def));

        return Collections.singletonMap(outputName, output);
    }
}
