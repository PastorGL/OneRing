/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
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

import static ash.nazg.populations.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class DwellTimeOperation extends Operation {
    private static final String VERB = "dwellTime";

    private int signalsUseridColumn;
    private int targetUseridColumn;
    private int targetGidColumn;
    private String inputSignalsName;
    private char inputSignalsDelimiter;
    private String inputTargetName;
    private char inputTargetDelimiter;
    private String outputName;
    private char outputDelimiter;

    @Override
    @Description("Statistical indicator for the Dwell Time of a sub-population that they spend in target cells")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_SIGNALS_USERID_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_TARGET_USERID_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_TARGET_GID_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(new TaskDescriptionLanguage.NamedStream[]{
                        new TaskDescriptionLanguage.NamedStream(RDD_INPUT_SIGNALS,
                                new StreamType[]{StreamType.CSV},
                                true
                        ),
                        new TaskDescriptionLanguage.NamedStream(RDD_INPUT_TARGET,
                                new StreamType[]{StreamType.CSV},
                                true
                        )
                }),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.Fixed},
                                false
                        )
                )
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputSignalsName = opResolver.namedInput(RDD_INPUT_SIGNALS);
        inputSignalsDelimiter = dsResolver.inputDelimiter(inputSignalsName);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputSignalsName);
        String prop;

        prop = opResolver.definition(DS_SIGNALS_USERID_COLUMN);
        signalsUseridColumn = inputColumns.get(prop);

        inputTargetName = opResolver.namedInput(RDD_INPUT_TARGET);
        inputTargetDelimiter = dsResolver.inputDelimiter(inputTargetName);

        inputColumns = dsResolver.inputColumns(inputTargetName);

        prop = opResolver.definition(DS_TARGET_USERID_COLUMN);
        targetUseridColumn = inputColumns.get(prop);

        prop = opResolver.definition(DS_TARGET_GID_COLUMN);
        targetGidColumn = inputColumns.get(prop);

        outputName = opResolver.positionalOutput(0);
        outputDelimiter = dsResolver.outputDelimiter(outputName);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        char _inputSignalsDelimiter = inputSignalsDelimiter;
        int _signalsUseridColumn = signalsUseridColumn;

        // userid -> S
        JavaPairRDD<Text, Long> S = ((JavaRDD<Object>) input.get(inputSignalsName))
                .mapPartitionsToPair(it -> {
                    CSVParser parser = new CSVParserBuilder()
                            .withSeparator(_inputSignalsDelimiter).build();

                    List<Tuple2<Text, Void>> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Object o = it.next();
                        String l = (o instanceof String) ? (String) o : String.valueOf(o);

                        String[] row = parser.parseLine(l);

                        Text userid = new Text(row[_signalsUseridColumn]);

                        ret.add(new Tuple2<>(userid, null));
                    }

                    return ret.iterator();
                })
                .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum);

        int _targetUseridColumn = targetUseridColumn;
        int _targetGidColumn = targetGidColumn;
        char _inputTargetDelimiter = inputTargetDelimiter;

        // userid -> gid, s
        JavaPairRDD<Text, Tuple2<Text, Long>> s = ((JavaRDD<Object>) input.get(inputTargetName))
                .mapPartitionsToPair(it -> {
                    CSVParser parser = new CSVParserBuilder()
                            .withSeparator(_inputTargetDelimiter).build();

                    List<Tuple2<Tuple2<Text, Text>, Void>> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Object o = it.next();
                        String l = (o instanceof String) ? (String) o : String.valueOf(o);

                        String[] row = parser.parseLine(l);

                        Text userid = new Text(row[_targetUseridColumn]);
                        Text gid = new Text(row[_targetGidColumn]);

                        ret.add(new Tuple2<>(new Tuple2<>(userid, gid), null));
                    }

                    return ret.iterator();
                })
                .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum)
                .mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2)));

        final char _outputDelimiter = outputDelimiter;

        JavaRDD<Text> output = s.join(S)
                .mapToPair(t -> new Tuple2<>(t._2._1._1, t._2._1._2.doubleValue() / t._2._2.doubleValue()))
                .aggregateByKey(new Tuple2<>(0L, 0.D),
                        (c, t) -> new Tuple2<>(c._1 + 1L, c._2 + t),
                        (c1, c2) -> new Tuple2<>(c1._1 + c2._1, c1._2 + c2._2)
                )
                .mapToPair(c -> new Tuple2<>(c._1, c._2._2 / c._2._1))
                .mapPartitions(it -> {
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
