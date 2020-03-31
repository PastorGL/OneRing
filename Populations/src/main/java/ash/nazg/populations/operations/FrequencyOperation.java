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
import ash.nazg.populations.functions.MedianCalcFunction;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.*;

@SuppressWarnings("unused")
public class FrequencyOperation extends PopulationIndicatorOperation {
    private static final String VERB = "frequency";

    @Override
    @Description("Statistical indicator for the frequency of a value occurring per other value selected as a count value")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_COUNT_COLUMN),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_VALUE_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Fixed},
                                false
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputValuesName = describedProps.inputs.get(0);
        inputValuesDelimiter = dataStreamsProps.inputDelimiter(inputValuesName);

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputValuesName);
        String prop;

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_COUNT_COLUMN);
        countColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_VALUE_COLUMN);
        valueColumn = inputColumns.get(prop);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        char _inputDelimiter = inputValuesDelimiter;
        int _valueColumn = valueColumn;
        int _countColumn = countColumn;
        final char _outputDelimiter = outputDelimiter;

        JavaRDD<Object> signals = (JavaRDD) input.get(inputValuesName);

        JavaPairRDD<Text, Double> gidToScores = signals.mapPartitionsToPair(it1 -> {
            CSVParser parser = new CSVParserBuilder()
                    .withSeparator(_inputDelimiter).build();

            List<Tuple2<Text, Text>> ret = new ArrayList<>();
            while (it1.hasNext()) {
                Object o = it1.next();
                String l = (o instanceof String) ? (String) o : String.valueOf(o);

                String[] row = parser.parseLine(l);

                Text value = new Text(row[_valueColumn]);
                Text count = new Text(row[_countColumn]);

                ret.add(new Tuple2<>(value, count));
            }

            return ret.iterator();
        }).combineByKey(
                t -> {
                    Map<Text, Long> ret = Collections.singletonMap(t, 1L);
                    return new Tuple2<>(ret, 1L);
                },
                (c, t) -> {
                    HashMap<Text, Long> ret = new HashMap<>(c._1);
                    ret.compute(t, (k, v) -> (v == null) ? 1L : v + 1L);
                    return new Tuple2<>(ret, c._2 + 1L);
                },
                (c1, c2) -> {
                    HashMap<Text, Long> ret = new HashMap<>(c1._1);
                    c2._1.forEach((key, v2) -> ret.compute(key, (k, v1) -> (v1 == null) ? v2 : v1 + v2));

                    return new Tuple2<>(ret, c1._2 + c2._2);
                }
        ).mapPartitionsToPair(it -> {
            List<Tuple2<Text, Double>> ret = new ArrayList<>();

            while (it.hasNext()) {
                //userid, gid -> count, total
                Tuple2<Text, Tuple2<Map<Text, Long>, Long>> t = it.next();

                t._2._1.forEach((value, count) -> ret.add(new Tuple2<>(value, count.doubleValue() / t._2._2.doubleValue())));
            }

            return ret.iterator();
        });

        JavaRDD<Tuple2<Text, Double>> polygonMedianScore = new MedianCalcFunction(ctx).call(gidToScores);

        JavaRDD<Text> output = polygonMedianScore.mapPartitions(it -> {
            List<Text> ret = new ArrayList<>();

            while (it.hasNext()) {
                Tuple2<Text, Double> t = it.next();

                String[] acc = new String[]{t._1.toString(), Double.toString(t._2)};

                StringWriter buffer = new StringWriter();
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
