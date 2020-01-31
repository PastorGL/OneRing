/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.math.functions.keyed.*;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import ash.nazg.math.config.CalcFunction;
import ash.nazg.math.config.ConfigurationParameters;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.KeyValue},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.KeyValue},
                                false
                        )
                )
        );
    }

    @Override
    public void setConfig(OperationConfig config) throws InvalidConfigValueException {
        super.setConfig(config);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);
        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);

        String prop;
        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CALC_COLUMN);
        calcColumn = inputColumns.get(prop);

        CalcFunction cf = describedProps.defs.getTyped(ConfigurationParameters.OP_CALC_FUNCTION);
        switch (cf) {
            case SUM: {
                Double _const = describedProps.defs.getTyped(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new SumFunction(_const);
                break;
            }
            case POWERMEAN:
            case POWER_MEAN: {
                Double pow = describedProps.defs.getTyped(ConfigurationParameters.OP_CALC_CONST);
                if (pow == null) {
                    throw new InvalidConfigValueException("POWERMEAN function of the operation '" + name + "' requires " + ConfigurationParameters.OP_CALC_CONST + " set");
                }
                keyedFunc = new PowerMeanFunction(pow);
                break;
            }
            case MEAN:
            case AVERAGE: {
                Double shift = describedProps.defs.getTyped(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new AverageFunction(shift);
                break;
            }
            case RMS:
            case ROOTMEAN:
            case ROOT_MEAN:
            case ROOTMEANSQUARE:
            case ROOT_MEAN_SQUARE: {
                keyedFunc = new PowerMeanFunction(2.D);
                break;
            }
            case MIN: {
                Double floor = describedProps.defs.getTyped(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new MinFunction(floor);
                break;
            }
            case MAX: {
                Double ceil = describedProps.defs.getTyped(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new MaxFunction(ceil);
                break;
            }
            case MUL: {
                Double _const = describedProps.defs.getTyped(ConfigurationParameters.OP_CALC_CONST);
                keyedFunc = new MulFunction(_const);
                break;
            }
        }

    }

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
