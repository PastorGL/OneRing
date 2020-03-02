/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.populations.config.ConfigurationParameters;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;
import scala.Tuple3;

import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class ParametricScoreOperation extends Operation {
    @Description("Value score multipliers")
    public static final String RDD_INPUT_SCORE_MULTIPLIERS = "multipliers";
    @Description("Column with value multiplier")
    public static final String DS_MULTIPLIER_VALUE_COLUMN = "multipliers.value.column";
    @Description("Column to match multiplier value with count value")
    public static final String DS_MULTIPLIER_COUNT_COLUMN = "multipliers.count.column";
    @Description("Generated column with user ID")
    public final static String GEN_GROUP = "_group";
    @Description("Generated column with postcode score")
    public final static String GEN_SCORE_PREFIX = "_score_*";
    @Description("Generated column with User ID")
    public final static String GEN_VALUE_PREFIX = "_value_*";
    @Description("How long is the top scores list")
    public static final String OP_TOP_SCORES = "top.scores";
    @Description("By default, generate only the topmost score")
    public static final Integer DEF_TOP_SCORES = 1;

    public static final String VERB = "parametricScore";

    private String inputValuesName;
    private char inputValuesDelimiter;
    private String inputMultipliersName;
    private char inputMultipliersDelimiter;

    private String outputName;
    private char outputDelimiter;
    private List<Integer> outputCols;

    private Integer top;
    private Integer valueColumn;
    private Integer groupColumn;
    private Integer countColumn;
    private Integer multiplierValueColumn;
    private Integer multiplierCountColumn;

    @Override
    @Description("Calculate a top of parametric scores for a value by its count and multiplier")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_VALUES_GROUP_COLUMN),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_VALUES_VALUE_COLUMN),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_VALUES_COUNT_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_MULTIPLIER_VALUE_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_MULTIPLIER_COUNT_COLUMN),
                        new TaskDescriptionLanguage.Definition(OP_TOP_SCORES, Integer.class, DEF_TOP_SCORES),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(
                                        ConfigurationParameters.RDD_INPUT_VALUES,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_INPUT_SCORE_MULTIPLIERS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                        }
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(
                                        ConfigurationParameters.RDD_OUTPUT_SCORES,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        new String[]{GEN_GROUP, GEN_SCORE_PREFIX, GEN_VALUE_PREFIX}
                                ),
                        }
                )
        );
    }

    @Override
    public void setConfig(OperationConfig propertiesConfig) throws InvalidConfigValueException {
        super.setConfig(propertiesConfig);

        inputValuesName = describedProps.namedInputs.get(ConfigurationParameters.RDD_INPUT_VALUES);
        inputValuesDelimiter = dataStreamsProps.inputDelimiter(inputValuesName);
        inputMultipliersName = describedProps.namedInputs.get(RDD_INPUT_SCORE_MULTIPLIERS);
        inputMultipliersDelimiter = dataStreamsProps.inputDelimiter(inputMultipliersName);
        outputName = describedProps.namedOutputs.get(ConfigurationParameters.RDD_OUTPUT_SCORES);
        outputDelimiter = dataStreamsProps.outputDelimiter(outputName);

        top = describedProps.defs.getTyped(OP_TOP_SCORES);

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputValuesName);
        String prop;

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_VALUES_GROUP_COLUMN);
        groupColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_VALUES_VALUE_COLUMN);
        valueColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_VALUES_COUNT_COLUMN);
        countColumn = inputColumns.get(prop);

        inputColumns = dataStreamsProps.inputColumns.get(inputMultipliersName);

        prop = describedProps.defs.getTyped(DS_MULTIPLIER_VALUE_COLUMN);
        multiplierValueColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(DS_MULTIPLIER_COUNT_COLUMN);
        multiplierCountColumn = inputColumns.get(prop);

        Map<String, Integer> outputColumns = new LinkedHashMap<>();
        String[] output = dataStreamsProps.outputColumns.get(outputName);
        for (String c : output) {
            outputColumns.put(c, null);
        }

        for (Integer i = 1; i <= top; i++) {
            String prefRepl = GEN_SCORE_PREFIX.replace("*", i.toString());
            if (outputColumns.containsKey(prefRepl)) {
                outputColumns.replace(prefRepl, -i);
            }
            prefRepl = GEN_VALUE_PREFIX.replace("*", i.toString());
            if (outputColumns.containsKey(prefRepl)) {
                outputColumns.replace(prefRepl, +i);
            }
        }

        if (outputColumns.containsKey(GEN_GROUP)) {
            outputColumns.replace(GEN_GROUP, 0);
        }

        if (outputColumns.containsValue(null)) {
            throw new InvalidConfigValueException("Output column specification for operation '" + name + "' contains an invalid column");
        }

        outputCols = new ArrayList<>(outputColumns.values());
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputValuesDelimiter = inputValuesDelimiter;
        final char _inputMultipliersDelimiter = inputMultipliersDelimiter;
        final char _outputDelimiter = outputDelimiter;
        final Integer[] _outputColumns = outputCols.toArray(new Integer[0]);

        final int _groupColumn = groupColumn;
        final int _valueColumn = valueColumn;
        final int _countColumn = countColumn;
        final int _multiplierCountColumn = multiplierCountColumn;
        final int _multiplierValueColumn = multiplierValueColumn;
        final int _top = top;

        JavaPairRDD<Text, Double> multipliers = ((JavaRDD<Object>) input.get(inputMultipliersName))
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Text, Double>> ret = new ArrayList<>();
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputMultipliersDelimiter)
                            .build();

                    while (it.hasNext()) {
                        Object o = it.next();
                        String l = o instanceof String ? (String) o : String.valueOf(o);

                        String[] row = parser.parseLine(l);

                        Text value = new Text(row[_multiplierCountColumn]);
                        Double multiplier = new Double(row[_multiplierValueColumn]);

                        ret.add(new Tuple2<>(value, multiplier));
                    }

                    return ret.iterator();
                });

        JavaPairRDD<Text, Tuple3<Text, Text, Long>> countGroupValues = ((JavaRDD<Object>) input.get(inputValuesName))
                .mapPartitionsToPair(it1 -> {
                    List<Tuple2<Tuple3<Text, Text, Text>, Long>> ret1 = new ArrayList<>();
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputValuesDelimiter)
                            .build();

                    while (it1.hasNext()) {
                        Object o = it1.next();
                        String l = o instanceof String ? (String) o : String.valueOf(o);

                        String[] row = parser.parseLine(l);

                        Text count = new Text(row[_countColumn]);
                        Text group = new Text(row[_groupColumn]);
                        Text value = new Text(row[_valueColumn]);

                        ret1.add(new Tuple2<>(new Tuple3<>(count, group, value), 1L));
                    }

                    return ret1.iterator();
                })
                .reduceByKey(Long::sum)
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Text, Tuple3<Text, Text, Long>>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Tuple3<Text, Text, Text>, Long> t = it.next();
                        ret.add(new Tuple2<>(t._1._1(), new Tuple3<>(t._1._2(), t._1._3(), t._2)));
                    }

                    return ret.iterator();
                });

        JavaRDD<Text> output = countGroupValues.join(multipliers)
                .values()
                .mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1._1(), t._1._2()), t._2 * t._1._3()))
                .reduceByKey(Double::sum)
                .mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2)))
                .combineByKey(
                        v -> {
                            Map<Double, Text> r = new HashMap<>();
                            r.put(v._2, v._1);
                            return r;
                        },
                        (t, v) -> {
                            t.put(v._2, v._1);
                            return t;
                        },
                        (t1, t2) -> {
                            t1.putAll(t2);

                            return t1;
                        }
                )
                .mapPartitions(it -> {
                    List<Text> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Text, Map<Double, Text>> t = it.next();

                        StringWriter buffer = new StringWriter();
                        String[] acc = new String[_outputColumns.length];

                        Map<Double, Text> resortMap = new TreeMap<>(Comparator.reverseOrder());
                        resortMap.putAll(t._2);
                        List<Map.Entry<Double, Text>> r = new ArrayList<>(resortMap.entrySet());
                        for (int i = 0; i < _outputColumns.length; i++) {
                            Integer col = _outputColumns[i];

                            if (col == 0) {
                                acc[i] = t._1.toString();
                            }
                            if (col < 0) {
                                int index = -col - 1;
                                acc[i] = (index > r.size() - 1) ? "" : r.get(index).getKey().toString();
                            }
                            if (col > 0) {
                                int index = +col - 1;
                                acc[i] = (index > r.size() - 1) ? "" : r.get(index).getValue().toString();
                            }
                        }

                        CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(acc, false);
                        writer.close();

                        ret.add(new Text(buffer.toString()));
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
