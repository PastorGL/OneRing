/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;

@SuppressWarnings("unused")
public class JoinByKeyOperation extends Operation {
    @Description("Type of the join")
    public static final String OP_JOIN = "join";
    @Description("Default value for the missing right row values")
    public static final String OP_DEFAULT_RIGHT = "default.right";
    @Description("Default value for the missing left row values")
    public static final String OP_DEFAULT_LEFT = "default.left";
    @Description("By default, any missing row from left is an empty string")
    public static final String DEF_DEFAULT_LEFT = "";
    @Description("By default, any missing row from right is an empty string")
    public static final String DEF_DEFAULT_RIGHT = "";
    @Description("By default, perform an inner join")
    public static final Join DEF_JOIN = Join.INNER;

    public static final String VERB = "joinByKey";

    private String[] inputNames;
    private char[] inputDelimiters;
    private String outputName;
    private Tuple2<Integer, Integer>[] outputColumns;
    private char outputDelimiter;

    private Join joinType;
    private String inputDefaultL;
    private String inputDefaultR;

    @Override
    @Description("Takes two or more Pair RDDs and joins them by key. Missing rows from either Pair RDDs on left and" +
            " right side of the join are settable and must have a proper column count")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_JOIN, Join.class, DEF_JOIN),
                        new TaskDescriptionLanguage.Definition(OP_DEFAULT_LEFT, DEF_DEFAULT_LEFT),
                        new TaskDescriptionLanguage.Definition(OP_DEFAULT_RIGHT, DEF_DEFAULT_RIGHT),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.KeyValue},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputNames = describedProps.inputs.toArray(new String[0]);
        if (inputNames.length < 2) {
            throw new InvalidConfigValueException("There must be at least two Pair RDDs to join by '" + name + "'");
        }

        outputName = describedProps.outputs.get(0);

        Map<String, Tuple2<Integer, Integer>> inputColumns = new HashMap<>();
        inputDelimiters = new char[inputNames.length];
        for (int i = 0; i < inputNames.length; i++) {
            inputDelimiters[i] = dataStreamsProps.inputDelimiter(inputNames[i]);

            for (Map.Entry<String, Integer> ic : dataStreamsProps.inputColumns.get(inputNames[i]).entrySet()) {
                inputColumns.put(ic.getKey(), new Tuple2<>(i, ic.getValue()));
            }
        }

        String[] outColumns = dataStreamsProps.outputColumns.get(outputName);
        List<Tuple2<Integer, Integer>> out = new ArrayList<>();
        for (String outCol : outColumns) {
            out.add(inputColumns.get(outCol));
        }

        joinType = describedProps.defs.getTyped(OP_JOIN);
        if (joinType == Join.RIGHT || joinType == Join.OUTER) {
            inputDefaultL = describedProps.defs.getTyped(OP_DEFAULT_LEFT);
        }
        if (joinType == Join.LEFT || joinType == Join.OUTER) {
            inputDefaultR = describedProps.defs.getTyped(OP_DEFAULT_RIGHT);
        }

        outputColumns = out.toArray(new Tuple2[0]);
        outputDelimiter = dataStreamsProps.outputDelimiter(outputName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char[] _inputDelimiters = inputDelimiters;
        final char _outputDelimiter = outputDelimiter;
        final String _inputDefaultL = inputDefaultL;
        final String _inputDefaultR = inputDefaultR;
        final Tuple2<Integer, Integer>[] _outputColumns = outputColumns;

        JavaPairRDD<Object, Object> leftInputRDD = (JavaPairRDD<Object, Object>) input.get(inputNames[0]);

        JavaPairRDD<Object, Object> j = leftInputRDD.mapPartitionsToPair(it -> {
            List<Tuple2<Object, Object>> res = new ArrayList<>();

            while (it.hasNext()) {
                Tuple2<Object, Object> o = it.next();

                String l1 = String.valueOf(o._2);
                CSVParser parser1 = new CSVParserBuilder().withSeparator(_inputDelimiters[0]).build();
                String[] line1 = parser1.parseLine(l1);

                StringWriter buffer = new StringWriter();

                String[] acc = new String[_outputColumns.length];
                int i = 0;
                for (Tuple2<Integer, Integer> col : _outputColumns) {
                    if (col._1 == 0) {
                        acc[i] = line1[col._2];
                    }

                    i++;
                }

                CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                writer.writeNext(acc, false);
                writer.close();

                res.add(new Tuple2<>(o._1, new Text(buffer.toString())));
            }

            return res.iterator();
        });

        for (int r = 1; r < inputNames.length; r++) {
            JavaPairRDD<Object, Object> rightInputRDD = (JavaPairRDD<Object, Object>) input.get(inputNames[r]);

            JavaPairRDD jj;
            switch (joinType) {
                case LEFT:
                    jj = j.leftOuterJoin(rightInputRDD);
                    break;
                case RIGHT:
                    jj = j.rightOuterJoin(rightInputRDD);
                    break;
                case OUTER:
                    jj = j.fullOuterJoin(rightInputRDD);
                    break;
                default:
                    jj = j.join(rightInputRDD);
            }

            final int _r = r;
            j = jj.mapPartitionsToPair(ito -> {
                List<Tuple2> res = new ArrayList<>();

                Iterator<Tuple2> it = (Iterator) ito;

                while (it.hasNext()) {
                    Tuple2<Object, Object> o = it.next();

                    Tuple2<Object, Object> v = (Tuple2<Object, Object>) o._2;

                    String l1 = (v._1 instanceof String) ?
                            (String) v._1 : String.valueOf((v._1 instanceof Optional) ?
                            ((Optional) v._1).orElse(_inputDefaultL) : v._1);
                    CSVParser parser1 = new CSVParserBuilder().withSeparator(_outputDelimiter).build();
                    String[] line1 = parser1.parseLine(l1);

                    String l2 = (v._2 instanceof String) ?
                            (String) v._2 : String.valueOf((v._2 instanceof Optional) ?
                            ((Optional) v._2).orElse(_inputDefaultR) : v._2);
                    CSVParser parser2 = new CSVParserBuilder().withSeparator(_inputDelimiters[_r]).build();
                    String[] line2 = parser2.parseLine(l2);

                    StringWriter buffer = new StringWriter();

                    String[] acc = new String[_outputColumns.length];
                    int i = 0;
                    for (Tuple2<Integer, Integer> col : _outputColumns) {
                        if (col._1 == _r) {
                            acc[i] = line2[col._2];
                        } else {
                            acc[i] = line1[i];
                        }

                        i++;
                    }

                    CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                            CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                    writer.writeNext(acc, false);
                    writer.close();

                    res.add(new Tuple2<>(o._1, new Text(buffer.toString())));
                }

                return res.iterator();
            });
        }

        return Collections.singletonMap(outputName, j.values());
    }

    public enum Join implements Serializable {
        @Description("Inner join")
        INNER,
        @Description("Left outer join")
        LEFT,
        @Description("Right outer join")
        RIGHT,
        @Description("Full outer join")
        OUTER;
    }
}
