/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.math.config.KeyedMath;
import ash.nazg.math.functions.keyed.*;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ash.nazg.math.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class KeyedMathOperation extends Operation {
    private static final String OP_MINIMAX_FULL = "minimax.full";

    private String inputName;
    private char inputDelimiter;
    private Integer calcColumn;
    private KeyedMath keyedFunction;
    private Boolean minimaxFull = false;

    private String outputName;

    private ash.nazg.math.functions.keyed.KeyedFunction keyedFunc;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("keyedMath", "Take an PairRDD and calculate a 'series' mathematical" +
                " function over all values (or a selected value column), treated as a Double, under each unique key",

                new PositionalStreamsMetaBuilder()
                        .ds("Pair RDD with a set of columns of type Double that comprise a series under each unique key",
                                new StreamType[]{StreamType.KeyValue}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_CALC_COLUMN, "Column with a Double to use as series source",
                                null, "By default, use entire value as calculation source")
                        .def(OP_MINIMAX_FULL, "If set to true, output full source value for MIN and" +
                                " MAX. Constant will be ignored", Boolean.class, Boolean.FALSE.toString(),
                                "By default, output only subject column")
                        .def(OP_CALC_FUNCTION, "The mathematical function to perform", KeyedMath.class)
                        .def(OP_CALC_CONST, "An optional constant value for the selected function", Double.class,
                                null, "By default the constant isn't set")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Pair RDD with calculation result under each input series' key",
                                new StreamType[]{StreamType.KeyValue}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);

        String prop;
        prop = opResolver.definition(DS_CALC_COLUMN);
        calcColumn = inputColumns.get(prop);

        keyedFunction = opResolver.definition(OP_CALC_FUNCTION);
        switch (keyedFunction) {
            case SUM: {
                Double _const = opResolver.definition(OP_CALC_CONST);
                keyedFunc = new SumFunction(_const);
                break;
            }
            case SUBTRACT: {
                Double _const = opResolver.definition(OP_CALC_CONST);
                keyedFunc = new SubtractFunction(_const);
                break;
            }
            case POWERMEAN: {
                Double pow = opResolver.definition(OP_CALC_CONST);
                if (pow == null) {
                    throw new InvalidConfigValueException("POWERMEAN function of the operation '" + name + "' requires " + OP_CALC_CONST + " set");
                }
                keyedFunc = new PowerMeanFunction(pow);
                break;
            }
            case AVERAGE: {
                Double shift = opResolver.definition(OP_CALC_CONST);
                keyedFunc = new AverageFunction(shift);
                break;
            }
            case RMS: {
                keyedFunc = new PowerMeanFunction(2.D);
                break;
            }
            case MIN: {
                Double floor = opResolver.definition(OP_CALC_CONST);
                keyedFunc = new MinFunction(floor);
                break;
            }
            case MAX: {
                Double ceil = opResolver.definition(OP_CALC_CONST);
                keyedFunc = new MaxFunction(ceil);
                break;
            }
            case MUL: {
                Double _const = opResolver.definition(OP_CALC_CONST);
                keyedFunc = new MulFunction(_const);
                break;
            }
            case DIV: {
                Double _const = opResolver.definition(OP_CALC_CONST);
                keyedFunc = new DivFunction(_const);
                break;
            }
            case EQUALITY: {
                Double _const = opResolver.definition(OP_CALC_CONST);
                keyedFunc = new EqualityFunction(_const);
                break;
            }
            case MEDIAN: {
                keyedFunc = new MedianFunction();
                break;
            }
        }

        if ((calcColumn != null) && ((keyedFunction == KeyedMath.MIN) || (keyedFunction == KeyedMath.MAX))) {
            minimaxFull = opResolver.definition(OP_MINIMAX_FULL);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaPairRDD<Object, Object> inputRDD = (JavaPairRDD<Object, Object>) input.get(inputName);

        if (!minimaxFull) {
            JavaPairRDD<Object, Double> doubleRDD;

            if (calcColumn != null) {
                final char _inputDelimiter = inputDelimiter;
                final int _calcColumn = calcColumn;
                doubleRDD = inputRDD.mapPartitionsToPair(it -> {
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                    List<Tuple2<Object, Double>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Object> t = it.next();

                        Object o = t._2;
                        String l = o instanceof String ? (String) o : String.valueOf(o);
                        String[] row = parser.parseLine(l);

                        ret.add(new Tuple2<>(t._1, Double.parseDouble(row[_calcColumn])));
                    }

                    return ret.iterator();
                });
            } else {
                doubleRDD = inputRDD.mapToPair(t -> new Tuple2<>(t._1, Double.parseDouble(String.valueOf(t._2))));
            }

            JavaPairRDD<Object, Double> output = doubleRDD
                    .combineByKey(
                            t -> {
                                List<Double> ret = new ArrayList<>();
                                ret.add(t);
                                return ret;
                            },
                            (l, t) -> {
                                l.add(t);
                                return l;
                            },
                            (l1, l2) -> {
                                l1.addAll(l2);
                                return l1;
                            }
                    )
                    .mapPartitionsToPair(keyedFunc);

            return Collections.singletonMap(outputName, output);
        }

        final char _inputDelimiter = inputDelimiter;
        final int _calcColumn = calcColumn;
        final KeyedMath _keyedFunction = keyedFunction;
        JavaPairRDD<Object, Object> output = inputRDD.mapPartitionsToPair(it -> {
            CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
            List<Tuple2<Object, Tuple2<Object, Double>>> ret = new ArrayList<>();

            while (it.hasNext()) {
                Tuple2<Object, Object> t = it.next();

                Object o = t._2;
                String l = o instanceof String ? (String) o : String.valueOf(o);
                String[] row = parser.parseLine(l);

                ret.add(new Tuple2<>(t._1, new Tuple2<>(o, Double.parseDouble(row[_calcColumn]))));
            }

            return ret.iterator();
        })
        .reduceByKey(
                (t1, t2) -> {
                    if (_keyedFunction == KeyedMath.MIN) {
                        if (t1._2 < t2._2) {
                            return t1;
                        } else {
                            return t2;
                        }
                    }
                    if (t1._2 > t2._2) {
                        return t1;
                    } else {
                        return t2;
                    }
                }
        )
        .mapValues(Tuple2::_1);

        return Collections.singletonMap(outputName, output);
    }
}
