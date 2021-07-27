/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionEnum;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
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
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.*;

@SuppressWarnings("unused")
public class JoinByKeyOperation extends Operation {
    public static final String OP_JOIN = "join";
    public static final String OP_DEFAULT = "default";
    public static final String OP_DEFAULT_RIGHT = "default.right";
    public static final String OP_DEFAULT_LEFT = "default.left";

    private String[] inputNames;
    private char[] inputDelimiters;

    private String outputName;
    private char outputDelimiter;
    private Tuple2<Integer, Integer>[] outputColumns;

    private Join joinType;
    private String inputDefaultL;
    private String inputDefaultR;
    private String[] outputTemplate;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("joinByKey", "Takes two or more Pair RDDs and joins them by key." +
                " Missing rows' values from either Pair RDDs on left and right side of the join are settable" +
                " via result template, or left and right default values",

                new PositionalStreamsMetaBuilder(2)
                        .ds("A list of Pair RDDs to join by key values",
                                new StreamType[]{StreamType.KeyValue}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_JOIN, "Type of the join", Join.class, Join.INNER.name(), "Type of the join")
                        .def(OP_DEFAULT, "Template for resulting rows", String[].class, null,
                                "By default, there is no join result template, defaults for left and right are used")
                        .def(OP_DEFAULT_LEFT, "Default value for the missing left rows' values", null,
                                "By default, there is no left default, join result template is used")
                        .def(OP_DEFAULT_RIGHT, "Default value for the missing right rows' values", null,
                                "By default, there is no right default, join result template is used")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Output CSV RDD made from value columns of source RDDs",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputNames = opResolver.positionalInputs();
        outputName = opResolver.positionalOutput(0);

        Map<String, Tuple2<Integer, Integer>> inputColumns = new HashMap<>();
        inputDelimiters = new char[inputNames.length];
        for (int i = 0; i < inputNames.length; i++) {
            inputDelimiters[i] = dsResolver.inputDelimiter(inputNames[i]);

            for (Map.Entry<String, Integer> ic : dsResolver.inputColumns(inputNames[i]).entrySet()) {
                inputColumns.put(ic.getKey(), new Tuple2<>(i, ic.getValue()));
            }
        }

        String[] outColumns = dsResolver.outputColumns(outputName);
        List<Tuple2<Integer, Integer>> out = new ArrayList<>();
        for (String outCol : outColumns) {
            out.add(inputColumns.get(outCol));
        }

        joinType = opResolver.definition(OP_JOIN);

        outputColumns = out.toArray(new Tuple2[0]);
        outputDelimiter = dsResolver.outputDelimiter(outputName);

        outputTemplate = opResolver.definition(OP_DEFAULT);
        if (outputTemplate != null) {
            if (outputColumns.length != outputTemplate.length) {
                throw new InvalidConfigValueException("Operation '" + name + "' output template and output columns specifications doesn't match");
            }
        } else {
            if (joinType == Join.RIGHT || joinType == Join.OUTER) {
                inputDefaultL = opResolver.definition(OP_DEFAULT_LEFT);
                if (inputDefaultL == null) {
                    throw new InvalidConfigValueException("Operation '" + name + "' has no output template neither default for left side of the join");
                }
            }
            if (joinType == Join.LEFT || joinType == Join.OUTER) {
                inputDefaultR = opResolver.definition(OP_DEFAULT_RIGHT);
                if (inputDefaultR == null) {
                    throw new InvalidConfigValueException("Operation '" + name + "' has no output template neither default for right side of the join");
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char[] _inputDelimiters = inputDelimiters;

        final String _inputDefaultL = inputDefaultL;
        final String _inputDefaultR = inputDefaultR;
        final Tuple2<Integer, Integer>[] _outputColumns = outputColumns;
        final String[] _outputTemplate = outputTemplate;
        final char _outputDelimiter = outputDelimiter;

        JavaPairRDD<Object, Object[]> leftInputRDD = ((JavaPairRDD<Object, Object>) input.get(inputNames[0])).mapPartitionsToPair(it -> {
            List<Tuple2<Object, Object[]>> res = new ArrayList<>();

            CSVParser parse0 = new CSVParserBuilder().withSeparator(_inputDelimiters[0]).build();
            final String[] accTemplate = (_outputTemplate != null) ? _outputTemplate : new String[_outputColumns.length];

            while (it.hasNext()) {
                Tuple2<Object, Object> o = it.next();

                String l = String.valueOf(o._2);
                String[] line = parse0.parseLine(l);

                String[] acc = accTemplate.clone();
                int i = 0;
                for (Tuple2<Integer, Integer> col : _outputColumns) {
                    if (col._1 == 0) {
                        acc[i] = line[col._2];
                    }

                    i++;
                }

                res.add(new Tuple2<>(o._1, acc));
            }

            return res.iterator();
        });

        for (int r = 1; r < inputNames.length; r++) {
            JavaPairRDD<Object, Object> rightInputRDD = (JavaPairRDD<Object, Object>) input.get(inputNames[r]);

            JavaPairRDD partialJoin;
            switch (joinType) {
                case LEFT:
                    partialJoin = leftInputRDD.leftOuterJoin(rightInputRDD);
                    break;
                case RIGHT:
                    partialJoin = leftInputRDD.rightOuterJoin(rightInputRDD);
                    break;
                case OUTER:
                    partialJoin = leftInputRDD.fullOuterJoin(rightInputRDD);
                    break;
                default:
                    partialJoin = leftInputRDD.join(rightInputRDD);
            }

            final int _r = r;
            final int _l = r - 1;
            leftInputRDD = partialJoin.mapPartitionsToPair(ito -> {
                List<Tuple2> res = new ArrayList<>();

                Iterator<Tuple2> it = (Iterator) ito;
                CSVParser parseRight = new CSVParserBuilder().withSeparator(_inputDelimiters[_r]).build();
                final String[] accTemplate = (_outputTemplate != null) ? _outputTemplate : new String[_outputColumns.length];

                while (it.hasNext()) {
                    Tuple2<Object, Object> o = it.next();

                    Tuple2<Object, Object> v = (Tuple2<Object, Object>) o._2;

                    String[] left = null;
                    if (v._1 instanceof Optional) {
                        Optional o1 = (Optional) v._1;
                        if (o1.isPresent()) {
                            left = (String[]) o1.get();
                        }
                    } else {
                        left = (String[]) v._1;
                    }

                    String line = null;
                    if (v._2 instanceof Optional) {
                        Optional o2 = (Optional) v._2;
                        if (o2.isPresent()) {
                            line = String.valueOf(o2.get());
                        }
                    } else {
                        line = String.valueOf(v._2);
                    }
                    String[] right = (line != null) ? parseRight.parseLine(line) : null;

                    String[] acc = (left != null) ? left : accTemplate.clone();
                    int i = 0;
                    for (Tuple2<Integer, Integer> col : _outputColumns) {
                        if (col._1 == _r) {
                            if (right != null) {
                                acc[i] = right[col._2];
                            } else if (_inputDefaultR != null) {
                                acc[i] = _inputDefaultR;
                            }
                        } else if (col._1 <= _l) {
                            if ((left == null) && (_inputDefaultL != null)) {
                                acc[i] = _inputDefaultL;
                            }
                        }

                        i++;
                    }

                    res.add(new Tuple2<>(o._1, acc));
                }

                return res.iterator();
            });
        }

        JavaRDD<Text> output = leftInputRDD.values().mapPartitions(it -> {
            List<Text> res = new ArrayList<>();

            while (it.hasNext()) {
                String[] acc = (String[]) it.next();

                StringWriter buffer = new StringWriter();

                CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                writer.writeNext(acc, false);
                writer.close();

                res.add(new Text(buffer.toString()));
            }

            return res.iterator();
        });

        return Collections.singletonMap(outputName, output);
    }

    public enum Join implements DefinitionEnum {
        INNER("Inner join"),
        LEFT("Left outer join"),
        RIGHT("Right outer join"),
        OUTER("Full outer join");

        private final String descr;

        Join(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
