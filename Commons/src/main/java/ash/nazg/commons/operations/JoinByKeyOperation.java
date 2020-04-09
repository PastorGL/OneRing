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
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.Optional;
import org.spark_project.guava.primitives.Ints;
import scala.Tuple2;

import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;

@SuppressWarnings("unused")
public class JoinByKeyOperation extends Operation {
    @Description("Type of the join")
    public static final String OP_JOIN = "join";
    @Description("Default value for the missing right row")
    public static final String OP_DEFAULT_RIGHT = "default.right";
    @Description("Default value for the missing left row")
    public static final String OP_DEFAULT_LEFT = "default.left";
    @Description("By default, any missing row from left is an empty string")
    public static final String DEF_DEFAULT_LEFT = "";
    @Description("By default, any missing row from right is an empty string")
    public static final String DEF_DEFAULT_RIGHT = "";
    @Description("By default, perform an inner join")
    public static final Join DEF_JOIN = Join.INNER;

    public static final String VERB = "joinByKey";

    private String inputName0;
    private String inputName1;
    private char inputDelimiter0;
    private char inputDelimiter1;
    private String outputName;
    private int[] outputColumns;
    private char outputDelimiter;

    private Join joinType;
    private String inputDefault0;
    private String inputDefault1;

    @Override
    @Description("Takes two PairRDDs and joins them by key. In the terms of joins, first PairRDD is the 'left'," +
            " second is the 'right'. Missing rows from either PairRDDs are settable and must have a proper column count.")
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

        inputName0 = describedProps.inputs.get(0);
        inputName1 = describedProps.inputs.get(1);
        inputDelimiter0 = dataStreamsProps.inputDelimiter(inputName0);
        inputDelimiter1 = dataStreamsProps.inputDelimiter(inputName1);

        outputName = describedProps.outputs.get(0);

        Map<String, Integer> input1Columns = dataStreamsProps.inputColumns.get(inputName0);
        Map<String, Integer> input2Columns = dataStreamsProps.inputColumns.get(inputName1);

        List<Integer> out = new ArrayList<>();
        String[] outColumns = dataStreamsProps.outputColumns.get(outputName);
        for (String outCol : outColumns) {
            if (input1Columns.containsKey(outCol)) {
                out.add(input1Columns.get(outCol));
            }
            if (input2Columns.containsKey(outCol)) {
                out.add(-input2Columns.get(outCol) - 1);
            }
        }

        joinType = describedProps.defs.getTyped(OP_JOIN);
        if (joinType == Join.LEFT || joinType == Join.OUTER) {
            inputDefault1 = describedProps.defs.getTyped(OP_DEFAULT_RIGHT);
        }
        if (joinType == Join.RIGHT || joinType == Join.OUTER) {
            inputDefault0 = describedProps.defs.getTyped(OP_DEFAULT_LEFT);
        }

        outputColumns = Ints.toArray(out);
        outputDelimiter = dataStreamsProps.outputDelimiter(outputName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter0 = inputDelimiter0;
        final char _inputDelimiter1 = inputDelimiter1;
        final char _outputDelimiter = outputDelimiter;
        final String _inputDefault0 = inputDefault0;
        final String _inputDefault1 = inputDefault1;
        final int[] _outputColumns = outputColumns;

        JavaPairRDD j;
        JavaPairRDD<Object, Object> leftInputRDD = (JavaPairRDD<Object, Object>) input.get(inputName0);
        JavaPairRDD<Object, Object> rightInputRDD = (JavaPairRDD<Object, Object>) input.get(inputName1);

        switch (joinType) {
            case LEFT:
                j = leftInputRDD.leftOuterJoin(rightInputRDD);
                break;
            case RIGHT:
                j = leftInputRDD.rightOuterJoin(rightInputRDD);
                break;
            case OUTER:
                j = leftInputRDD.fullOuterJoin(rightInputRDD);
                break;
            default:
                j = leftInputRDD.join(rightInputRDD);
        }

        JavaRDD out = j.mapPartitions(ito -> {
            List<Text> res = new ArrayList<>();

            Iterator it = (Iterator) ito;

            while (it.hasNext()) {
                Object o = it.next();

                Tuple2 v = (Tuple2) ((Tuple2) o)._2;

                String l1 = (v._1 instanceof String) ?
                        (String) v._1 : String.valueOf((v._1 instanceof Optional) ?
                        ((Optional) v._1).orElse(_inputDefault0) : v._1);
                CSVParser parser1 = new CSVParserBuilder().withSeparator(_inputDelimiter0).build();
                String[] line1 = parser1.parseLine(l1);

                String l2 = (v._2 instanceof String) ?
                        (String) v._2 : String.valueOf((v._2 instanceof Optional) ?
                        ((Optional) v._2).orElse(_inputDefault1) : v._2);
                CSVParser parser2 = new CSVParserBuilder().withSeparator(_inputDelimiter1).build();
                String[] line2 = parser2.parseLine(l2);

                StringWriter buffer = new StringWriter();

                String[] acc = new String[_outputColumns.length];
                int i = 0;
                for (Integer col : _outputColumns) {
                    acc[i++] = (col >= 0) ? line1[col] : line2[-col - 1];
                }

                CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                writer.writeNext(acc, false);
                writer.close();

                res.add(new Text(buffer.toString()));
            }

            return res.iterator();
        });

        return Collections.singletonMap(outputName, out);
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
