/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.NamedStreamsMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ash.nazg.populations.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class DwellTimeOperation extends Operation {
    private String inputSignalsName;
    private char inputSignalsDelimiter;
    private int signalsUseridColumn;

    private String inputTargetName;
    private char inputTargetDelimiter;
    private int targetUseridColumn;
    private int targetGroupingColumn;

    private String outputName;
    private char outputDelimiter;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("dwellTime", "Statistical indicator for the Dwell Time of a sub-population that they spend in target cells",

                new NamedStreamsMetaBuilder()
                        .ds(RDD_INPUT_SIGNALS, "Source user signals",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .ds(RDD_INPUT_TARGET, "Target audience signals, a sub-population of base audience signals",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_SIGNALS_USERID_COLUMN, "Column with the user ID")
                        .def(DS_TARGET_USERID_COLUMN, "Target audience signals user ID column")
                        .def(DS_TARGET_GROUPING_COLUMN, "Target audience signals grouping (i.e. grid cell ID) column")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("",
                                new StreamType[]{StreamType.Fixed}
                        )
                        .build()
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

        prop = opResolver.definition(DS_TARGET_GROUPING_COLUMN);
        targetGroupingColumn = inputColumns.get(prop);

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
        int _targetGroupingColumn = targetGroupingColumn;
        char _inputTargetDelimiter = inputTargetDelimiter;

        // userid -> groupid, s
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
                        Text groupid = new Text(row[_targetGroupingColumn]);

                        ret.add(new Tuple2<>(new Tuple2<>(userid, groupid), null));
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
