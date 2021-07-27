/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.datetime.functions.FilterByDateDefinition;
import ash.nazg.datetime.functions.FilterByDateFunction;
import ash.nazg.spark.Operation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class FilterByDateOperation extends Operation {
    public static final String DS_YEAR_COLUMN = "year.column";
    public static final String DS_MONTH_COLUMN = "month.column";
    public static final String DS_DATE_COLUMN = "date.column";
    public static final String DS_DOW_COLUMN = "dow.column";
    public static final String DS_HOUR_COLUMN = "hour.column";
    public static final String DS_MINUTE_COLUMN = "minute.column";
    public static final String OP_START = "start";
    public static final String OP_END = "end";
    public static final String OP_DATE_VALUE = "date.value";
    public static final String OP_YEAR_VALUE = "year.value";
    public static final String OP_DOW_VALUE = "dow.value";
    public static final String OP_MONTH_VALUE = "month.value";
    public static final String OP_HHMM_START = "hhmm.start";
    public static final String OP_HHMM_END = "hhmm.end";

    private String inputName;
    private String outputName;

    private FilterByDateDefinition def;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("filterByDate", "Filter a CSV RDD by exploded timestamp field values (year, month, day of month, day of week)" +
                " and optionally full date and/or time of day (hours and minutes) range. Multiple filter values are supported, and all fields are optional",

                new PositionalStreamsMetaBuilder()
                        .ds("",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_YEAR_COLUMN, "Column with year", null, "By default do not filter by year")
                        .def(DS_MONTH_COLUMN, "Column with month", null, "By default do not filter by month")
                        .def(DS_DATE_COLUMN, "Column with date of month", null, "By default do not filter by date of month")
                        .def(DS_DOW_COLUMN, "Column with day of week", null, "By default do not filter by day of week")
                        .def(DS_HOUR_COLUMN, "Column with hour", null, "By default do not filter by hour")
                        .def(DS_MINUTE_COLUMN, "Column with minute", null, "By default do not filter by minute")
                        .def(OP_START, "Start of the range filter", null, "By default do not filter by date range start")
                        .def(OP_END, "End of the range filter", null, "By default do not filter by date range end")
                        .def(OP_YEAR_VALUE, "List of year filter values", String[].class, null, "By default do not filter by year")
                        .def(OP_MONTH_VALUE, "List of month filter values", String[].class, null, "By default do not filter by month")
                        .def(OP_DATE_VALUE, "List of date filter values", String[].class, null, "By default do not filter by date of month")
                        .def(OP_DOW_VALUE, "List of day of week filter values", String[].class, null, "By default do not filter by day of week")
                        .def(OP_HHMM_START, "Starting time of day, exclusive, in HHMM format, in the range of 0000 to 2359", Integer.class, null, "By default do not filter by starting time of day")
                        .def(OP_HHMM_END, "Ending time of day, inclusive, in HHMM format, in the range of 0000 to 2359", Integer.class, null, "By default do not filter by ending time of day")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("",
                                new StreamType[]{StreamType.Passthru}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        def = new FilterByDateDefinition();

        inputName = opResolver.positionalInput(0);
        def.inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputName = opResolver.positionalOutput(0);
        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);

        boolean filteringNeeded = false;

        String prop = opResolver.definition(DS_YEAR_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.yearCol = inputColumns.get(prop);
        }
        prop = opResolver.definition(DS_MONTH_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.monthCol = inputColumns.get(prop);
        }
        prop = opResolver.definition(DS_DATE_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.dateCol = inputColumns.get(prop);
        }
        prop = opResolver.definition(DS_DOW_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.dowCol = inputColumns.get(prop);
        }
        prop = opResolver.definition(DS_HOUR_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.hourCol = inputColumns.get(prop);
        }
        prop = opResolver.definition(DS_MINUTE_COLUMN);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.minuteCol = inputColumns.get(prop);
        }

        prop = opResolver.definition(OP_START);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.start = FilterByDateDefinition.parseDate(prop);
        }
        prop = opResolver.definition(OP_END);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            def.end = FilterByDateDefinition.parseDate(prop);
        }

        String[] arr = opResolver.definition(OP_YEAR_VALUE);
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
        arr = opResolver.definition(OP_MONTH_VALUE);
        if (arr != null) {
            filteringNeeded = true;
            def.months = integers(arr, 1, 13);
        }
        arr = opResolver.definition(OP_DATE_VALUE);
        if (arr != null) {
            filteringNeeded = true;
            def.dates = integers(arr, 1, 32);
        }
        arr = opResolver.definition(OP_DOW_VALUE);
        if (arr != null) {
            filteringNeeded = true;
            def.dows = integers(arr, 1, 8);
        }

        Integer hhmm = opResolver.definition(OP_HHMM_START);
        if (hhmm != null) {
            if (hhmm < 0 || hhmm > 2359) {
                throw new InvalidConfigValueException("Filter by starting time of day for the operation '" + name + "' exceeds the allowed range of 0000 to 2359");
            }
            filteringNeeded = true;
            def.startHHMM = hhmm;
        }
        hhmm = opResolver.definition(OP_HHMM_END);
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

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        @SuppressWarnings("unchecked")
        JavaRDD output = input.get(inputName)
                .mapPartitions(new FilterByDateFunction(def));

        return Collections.singletonMap(outputName, output);
    }
}
