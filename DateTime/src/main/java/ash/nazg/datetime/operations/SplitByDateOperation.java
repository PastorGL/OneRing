/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.NamedStreamsMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.datetime.functions.FilterByDateDefinition;
import ash.nazg.datetime.functions.FilterByDateFunction;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.storage.StorageLevel;
import org.sparkproject.guava.primitives.Ints;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class SplitByDateOperation extends Operation {
    public static final String OP_SPLIT_TEMPLATE = "split.template";
    public static final String RDD_OUTPUT_SPLITS_TEMPLATE = "template";
    public static final String RDD_OUTPUT_DISTINCT_SPLITS = "distinct_splits";
    public static final String DS_YEAR_COLUMN = "year.column";
    public static final String DS_MONTH_COLUMN = "month.column";
    public static final String DS_DATE_COLUMN = "date.column";
    public static final String DS_DOW_COLUMN = "dow.column";
    public static final String DS_HOUR_COLUMN = "hour.column";
    public static final String DS_MINUTE_COLUMN = "minute.column";

    private String inputName;

    private String outputNameTemplate;
    private String outputDistinctSplits;

    private int[] splitColumns;
    private final Map<Integer, String> splitColumnNames = new HashMap<>();

    private final FilterByDateDefinition def = new FilterByDateDefinition();

    @Override
    public OperationMeta meta() {
        return new OperationMeta("splitByDate", "Take a CSV RDD that contains exploded timestamp columns and split it into several RDDs" +
                " by selected columns' values. Output 'template' name is treated as a template for a set of" +
                " generated outputs that can reference to encountered unique values of selected columns",

                new PositionalStreamsMetaBuilder()
                        .ds("A CSV RDD with one or more exploded timestamp columns",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_YEAR_COLUMN, "If set, split by year column value", null, "By default do not explode date of month")
                        .def(DS_MONTH_COLUMN, "If set, split by month column value", null, "By default do not explode year")
                        .def(DS_DATE_COLUMN, "If set, split by date of month column value", null, "By default do not explode day of week")
                        .def(DS_DOW_COLUMN, "If set, split by day of week column value", null, "By default do not explode month")
                        .def(DS_HOUR_COLUMN, "If set, split by hour column value", null, "By default do not explode hour")
                        .def(DS_MINUTE_COLUMN, "If set, split by minute column value", null, "By default do not explode minute")
                        .def(OP_SPLIT_TEMPLATE, "Template for output names wildcard part in form of {input.column1}/{input.column2}")
                        .build(),

                new NamedStreamsMetaBuilder()
                        .ds(RDD_OUTPUT_SPLITS_TEMPLATE, "Template output with a wildcard part, i.e. output_*",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .ds(RDD_OUTPUT_DISTINCT_SPLITS, "Optional output that contains all the distinct splits occurred on the input data," +
                                        " in the form of names of the generated inputs. Its column order is always year,month,date,dow,hour,minute." +
                                        " Unreferenced split columns are omitted from this output",
                                new StreamType[]{StreamType.Fixed}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        def.inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputDistinctSplits = opResolver.namedOutput(RDD_OUTPUT_DISTINCT_SPLITS);
        outputNameTemplate = opResolver.namedOutput(RDD_OUTPUT_SPLITS_TEMPLATE)
                .replace("*", opResolver.definition(OP_SPLIT_TEMPLATE));

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);
        String prop;

        List<String> splitColumns = new ArrayList<>();
        prop = opResolver.definition(DS_YEAR_COLUMN);
        if (prop != null) {
            def.yearCol = inputColumns.get(prop);
            splitColumns.add(prop);
        }
        prop = opResolver.definition(DS_MONTH_COLUMN);
        if (prop != null) {
            def.monthCol = inputColumns.get(prop);
            splitColumns.add(prop);
        }
        prop = opResolver.definition(DS_DATE_COLUMN);
        if (prop != null) {
            def.dateCol = inputColumns.get(prop);
            splitColumns.add(prop);
        }
        prop = opResolver.definition(DS_DOW_COLUMN);
        if (prop != null) {
            def.dowCol = inputColumns.get(prop);
            splitColumns.add(prop);
        }
        prop = opResolver.definition(DS_HOUR_COLUMN);
        if (prop != null) {
            def.hourCol = inputColumns.get(prop);
            splitColumns.add(prop);
        }
        prop = opResolver.definition(DS_MINUTE_COLUMN);
        if (prop != null) {
            def.minuteCol = inputColumns.get(prop);
            splitColumns.add(prop);
        }

        if (splitColumns.size() == 0) {
            throw new InvalidConfigValueException("Operation '" + name + "' must have defined at least one exploded timestamp column to perform a split");
        }

        List<Integer> splitCols = new ArrayList<>();
        for (String col : splitColumns) {
            Integer n = inputColumns.get(col);
            splitColumnNames.put(n, col);
            splitCols.add(n);
        }

        this.splitColumns = Ints.toArray(splitCols);

        for (String scn : splitColumnNames.values()) {
            if (!outputNameTemplate.contains("{" + scn + "}")) {
                throw new InvalidConfigValueException("Split output name template '" + outputNameTemplate + "' must include split column reference {" + scn + "} for the operation '" + name + "'");
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) throws Exception {
        JavaRDD<Object> cachedInput = ((JavaRDD<Object>) input.get(inputName))
                .persist(StorageLevel.MEMORY_AND_DISK_SER());

        Map<String, JavaRDDLike> outs = new HashMap<>();

        int[] _splitColumns = splitColumns;

        FilterByDateDefinition _def = (FilterByDateDefinition) def.clone();

        JavaRDD<Text> distinctSplits = cachedInput
                .mapPartitions(it -> {
                    List<Text> ret = new ArrayList<>();

                    CSVParser parser = new CSVParserBuilder()
                            .withSeparator(_def.inputDelimiter).build();

                    while (it.hasNext()) {
                        Object v = it.next();
                        String l = v instanceof String ? (String) v : String.valueOf(v);

                        boolean matches = true;

                        String[] ll = parser.parseLine(l);
                        String[] acc = new String[_splitColumns.length];
                        for (int i = 0; i < _splitColumns.length; i++) {
                            acc[i] = ll[_splitColumns[i]];
                        }

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _def.inputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(acc, false);
                        writer.close();

                        ret.add(new Text(buffer.toString()));
                    }

                    return ret.iterator();
                })
                .distinct();

        if (outputDistinctSplits != null) {
            outs.put(outputDistinctSplits, distinctSplits);
        }

        List uniques = distinctSplits
                .collect();

        CSVParser parser = new CSVParserBuilder().withSeparator(def.inputDelimiter)
                .build();

        for (Object u : uniques) {
            String l = String.valueOf(u);
            String[] ll = parser.parseLine(l);

            String outputName = outputNameTemplate;
            FilterByDateDefinition uDef = (FilterByDateDefinition) def.clone();

            for (int i = 0; i < splitColumns.length; i++) {
                int sc = splitColumns[i];

                String splitColumnName = splitColumnNames.get(sc);
                outputName = outputName.replace("{" + splitColumnName + "}", ll[i]);

                Integer[] arr = {new Integer(ll[i])};
                if ((uDef.yearCol != null) && (sc == uDef.yearCol)) {
                    uDef.years = arr;
                }
                if ((uDef.monthCol != null) && (sc == uDef.monthCol)) {
                    uDef.months = arr;
                }
                if ((uDef.dateCol != null) && (sc == uDef.dateCol)) {
                    uDef.dates = arr;
                }
                if ((uDef.dowCol != null) && (sc == uDef.dowCol)) {
                    uDef.dows = arr;
                }
                if ((uDef.hourCol != null) && (sc == uDef.hourCol)) {
                    uDef.hours = arr;
                }
                if ((uDef.minuteCol != null) && (sc == uDef.minuteCol)) {
                    uDef.minutes = arr;
                }
            }

            JavaRDD output = cachedInput.mapPartitions(new FilterByDateFunction(uDef));
            outs.put(outputName, output);
        }

        return outs;
    }
}
