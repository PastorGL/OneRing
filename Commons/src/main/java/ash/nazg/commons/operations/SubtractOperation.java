/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.*;

@SuppressWarnings("unused")
public class SubtractOperation extends Operation {
    @Description("Column to match a value in the minuend if it is a CSV RDD")
    public static final String DS_MINUEND_COLUMN = "minuend.column";
    @Description("By default, treat minuend RDD as a plain one")
    public static final String DEF_MINUEND_COLUMN = null;
    @Description("Column to match a value in the subtrahend if it is a CSV RDD")
    public static final String DS_SUBTRAHEND_COLUMN = "subtrahend.column";
    @Description("By default, treat subtrahend RDD as a plain one")
    public static final String DEF_SUBTRAHEND_COLUMN = null;

    public static final String VERB = "subtract";

    private Integer minuendCol;
    private String minuendName;
    private char minuendDelimiter;

    private String subtrahendName;
    private Integer subtrahendCol;
    private char subtrahendDelimiter;

    private String outputName;

    @Override
    @Description("Take two RDDs and emit an RDD that consists" +
            " of rows of first (the minuend) that do not present in the second (the subtrahend)." +
            " If either RDD is a Plain one, entire rows will be matched." +
            " If either of RDDs is a PairRDD, its keys will be used to match instead." +
            " If either RDD is a CSV, you should specify the column to match")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_SUBTRAHEND_COLUMN, DEF_SUBTRAHEND_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_MINUEND_COLUMN, DEF_MINUEND_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.KeyValue, TaskDescriptionLanguage.StreamType.Plain},
                                true
                        ),
                        2
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
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        minuendName = describedProps.inputs.get(0);
        subtrahendName = describedProps.inputs.get(1);

        String subtrahendColumn = describedProps.defs.getTyped(DS_SUBTRAHEND_COLUMN);
        Map<String, Integer> columns = dataStreamsProps.inputColumns.get(subtrahendName);
        subtrahendCol = columns.get(subtrahendColumn);
        if (subtrahendCol != null) {
            subtrahendDelimiter = dataStreamsProps.inputDelimiter(subtrahendName);
        }

        String minuendColumn = describedProps.defs.getTyped(DS_MINUEND_COLUMN);
        columns = dataStreamsProps.inputColumns.get(minuendName);
        minuendCol = columns.get(minuendColumn);
        if (minuendCol != null) {
            minuendDelimiter = dataStreamsProps.inputDelimiter(minuendName);
        }

        outputName = describedProps.outputs.get(0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _minuendDelimiter = minuendDelimiter;
        final char _subtrahendDelimiter = subtrahendDelimiter;
        Integer _subtrahendCol = subtrahendCol;
        Integer _minuendCol = minuendCol;

        JavaRDDLike minuend = input.get(minuendName);
        JavaRDDLike subtrahend = input.get(subtrahendName);

        JavaPairRDD<Object, Object> right;
        if (subtrahend instanceof JavaPairRDD) {
            right = (JavaPairRDD) subtrahend;
        } else {
            right = ((JavaRDD<Object>) subtrahend)
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Object>> ret = new ArrayList<>();
                        if (_subtrahendCol != null) {
                            CSVParser parser1 = new CSVParserBuilder().withSeparator(_subtrahendDelimiter).build();

                            while (it.hasNext()) {
                                String[] line = parser1.parseLine(String.valueOf(it.next()));

                                ret.add(new Tuple2<>(line[_subtrahendCol], null));
                            }
                        } else {
                            while (it.hasNext()) {
                                ret.add(new Tuple2<>(it.next(), null));
                            }
                        }

                        return ret.iterator();
                    });
        }

        JavaRDDLike output;
        if (minuend instanceof JavaPairRDD) {
            output = ((JavaPairRDD) minuend).subtractByKey(right);
        } else {
            output = ((JavaRDD<Object>) minuend)
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Object>> ret = new ArrayList<>();
                        if (_minuendCol != null) {
                            CSVParser parser1 = new CSVParserBuilder().withSeparator(_minuendDelimiter).build();

                            while (it.hasNext()) {
                                String o = String.valueOf(it.next());
                                String[] line = parser1.parseLine(o);

                                ret.add(new Tuple2<>(line[_minuendCol], o));
                            }
                        } else {
                            while (it.hasNext()) {
                                Object o = it.next();
                                ret.add(new Tuple2<>(o, o));
                            }
                        }

                        return ret.iterator();
                    })
                    .subtractByKey(right)
                    .map(t -> t._2);
        }

        return Collections.singletonMap(outputName, output);
    }
}
