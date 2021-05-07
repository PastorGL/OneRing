/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.math.config.CalcFunction;
import ash.nazg.math.config.ConfigurationParameters;
import ash.nazg.math.functions.keyed.*;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.*;

@SuppressWarnings("unused")
public class KeyedMathOperation extends Operation {
    @Description("By default the constant isn't set")
    public static final Double DEF_CALC_CONST = null;
    @Description("By default, use entire value as calculation source")
    public static final String DEF_CALC_COLUMN = null;

    private static final String VERB = "keyedMath";

    private String inputName;
    private char inputDelimiter;
    private String outputName;

    private Integer calcColumn;

    private KeyedFunction keyedFunc;

    @Override
    @Description("Take an PairRDD and calculate a 'series' mathematical function over all values (or a selected" +
            " value column), treated as a Double, under each unique key")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_CALC_COLUMN, DEF_CALC_COLUMN),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_CALC_FUNCTION, CalcFunction.class),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_CALC_CONST, Double.class, DEF_CALC_CONST),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.KeyValue},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.KeyValue},
                                false
                        )
                )
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);

        String prop;
        prop = opResolver.definition(ConfigurationParameters.DS_CALC_COLUMN);
        calcColumn = inputColumns.get(prop);

        CalcFunction cf = opResolver.definition(ConfigurationParameters.OP_CALC_FUNCTION);
        switch (cf) {
            case SUM: {
                Double _const = opResolver.definition(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new SumFunction(_const);
                break;
            }
            case POWERMEAN: {
                Double pow = opResolver.definition(ConfigurationParameters.OP_CALC_CONST);
                if (pow == null) {
                    throw new InvalidConfigValueException("POWERMEAN function of the operation '" + name + "' requires " + ConfigurationParameters.OP_CALC_CONST + " set");
                }
                keyedFunc = new PowerMeanFunction(pow);
                break;
            }
            case AVERAGE: {
                Double shift = opResolver.definition(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new AverageFunction(shift);
                break;
            }
            case RMS: {
                keyedFunc = new PowerMeanFunction(2.D);
                break;
            }
            case MIN: {
                Double floor = opResolver.definition(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new MinFunction(floor);
                break;
            }
            case MAX: {
                Double ceil = opResolver.definition(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new MaxFunction(ceil);
                break;
            }
            case MUL: {
                Double _const = opResolver.definition(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new MulFunction(_const);
                break;
            }
            case DIV: {
                Double _const = opResolver.definition(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new DivFunction(_const);
                break;
            }
        }

    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaPairRDD<Object, Object> inputRDD = (JavaPairRDD<Object, Object>) input.get(inputName);
        JavaPairRDD<Object, Double> doubleRDD;

        if (calcColumn != null) {
            char _inputDelimiter = inputDelimiter;
            int _calcColumn = calcColumn;
            doubleRDD = inputRDD.mapPartitionsToPair(it -> {
                CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                List<Tuple2<Object, Double>> ret = new ArrayList<>();

                while (it.hasNext()) {
                    Tuple2<Object, Object> t = it.next();

                    Object o = t._2;
                    String l = o instanceof String ? (String) o : String.valueOf(o);
                    String[] row = parser.parseLine(l);

                    ret.add(new Tuple2<>(t._1, new Double(row[_calcColumn])));
                }

                return ret.iterator();
            });
        } else {
            doubleRDD = inputRDD.mapToPair(t -> new Tuple2<>(t._1, new Double(String.valueOf(t._2))));
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
}
